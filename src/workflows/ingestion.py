from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from io import BytesIO

    import pandas as pd

    from src.config import settings
    from src.s3_client import S3Client

REQUIRED_COLUMNS = {
    "customerID", "gender", "SeniorCitizen", "Partner", "Dependents",
    "tenure", "PhoneService", "InternetService", "Contract",
    "MonthlyCharges", "TotalCharges", "Churn",
}


@dataclass
class IngestionResult:
    s3_key: str
    row_count: int


@activity.defn
def download_from_s3(raw_s3_key: str, s3: S3Client | None = None) -> str:
    if s3 is None:
        s3 = S3Client()
    data = s3.download_bytes(raw_s3_key)
    staging_key = raw_s3_key.replace(settings.raw_prefix, settings.staging_prefix)
    s3.upload_bytes(staging_key, data)
    return staging_key


@activity.defn
def validate_schema(staging_s3_key: str, s3: S3Client | None = None) -> IngestionResult:
    if s3 is None:
        s3 = S3Client()
    data = s3.download_bytes(staging_s3_key)
    df = pd.read_csv(BytesIO(data))

    if df.empty:
        raise ValueError("CSV file is empty")

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return IngestionResult(s3_key=staging_s3_key, row_count=len(df))


@workflow.defn
class IngestionWorkflow:
    @workflow.run
    async def run(self, raw_s3_key: str) -> IngestionResult:
        staging_key = await workflow.execute_activity(
            download_from_s3,
            raw_s3_key,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=4,
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
            ),
        )
        result = await workflow.execute_activity(
            validate_schema,
            staging_key,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return result
