from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from io import BytesIO

    import pandas as pd
    from sklearn.model_selection import train_test_split

    from src.config import settings
    from src.s3_client import S3Client


@dataclass
class PreprocessingResult:
    train_s3_key: str
    test_s3_key: str


def _read_csv_from_s3(s3_key: str, s3: S3Client) -> pd.DataFrame:
    return pd.read_csv(BytesIO(s3.download_bytes(s3_key)))


def _write_csv_to_s3(df: pd.DataFrame, s3_key: str, s3: S3Client) -> str:
    s3.upload_bytes(s3_key, df.to_csv(index=False).encode())
    return s3_key


@activity.defn
def clean_data(staging_s3_key: str, s3: S3Client | None = None) -> str:
    if s3 is None:
        s3 = S3Client()
    df = _read_csv_from_s3(staging_s3_key, s3)

    df = df.drop_duplicates(subset="customerID")
    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")
    df["TotalCharges"] = df["TotalCharges"].fillna(df["TotalCharges"].median())

    output_key = staging_s3_key.replace("staging/", "staging/cleaned_")
    return _write_csv_to_s3(df, output_key, s3)


@activity.defn
def feature_engineer(cleaned_s3_key: str, s3: S3Client | None = None) -> str:
    if s3 is None:
        s3 = S3Client()
    df = _read_csv_from_s3(cleaned_s3_key, s3)

    df = df.drop(columns=["customerID"])
    df["Churn"] = df["Churn"].map({"No": 0, "Yes": 1})

    categorical_cols = df.select_dtypes(include=["object"]).columns.tolist()
    df = pd.get_dummies(df, columns=categorical_cols, drop_first=True)

    # Convert bool columns to int
    bool_cols = df.select_dtypes(include=["bool"]).columns
    df[bool_cols] = df[bool_cols].astype(int)

    output_key = cleaned_s3_key.replace("cleaned_", "engineered_")
    return _write_csv_to_s3(df, output_key, s3)


@activity.defn
def split_data(engineered_s3_key: str, s3: S3Client | None = None) -> PreprocessingResult:
    if s3 is None:
        s3 = S3Client()
    df = _read_csv_from_s3(engineered_s3_key, s3)

    train_df, test_df = train_test_split(df, test_size=0.2, random_state=42)

    train_key = f"{settings.processed_prefix}train.csv"
    test_key = f"{settings.processed_prefix}test.csv"
    _write_csv_to_s3(train_df, train_key, s3)
    _write_csv_to_s3(test_df, test_key, s3)

    return PreprocessingResult(train_s3_key=train_key, test_s3_key=test_key)


@workflow.defn
class PreprocessingWorkflow:
    @workflow.run
    async def run(self, staging_s3_key: str) -> PreprocessingResult:
        retry = RetryPolicy(
            maximum_attempts=3,
            initial_interval=timedelta(seconds=2),
            backoff_coefficient=2.0,
        )
        timeout = timedelta(minutes=5)

        cleaned_key = await workflow.execute_activity(
            clean_data, staging_s3_key,
            start_to_close_timeout=timeout, retry_policy=retry,
        )
        engineered_key = await workflow.execute_activity(
            feature_engineer, cleaned_key,
            start_to_close_timeout=timeout, retry_policy=retry,
        )
        result = await workflow.execute_activity(
            split_data, engineered_key,
            start_to_close_timeout=timeout, retry_policy=retry,
        )
        return result
