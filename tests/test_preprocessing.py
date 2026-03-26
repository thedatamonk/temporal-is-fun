from io import BytesIO

import pandas as pd
import pytest
from moto import mock_aws

from src.config import Settings
from src.s3_client import S3Client
from src.workflows.preprocessing import (
    PreprocessingResult,
    clean_data,
    feature_engineer,
    split_data,
)


def _make_csv(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode()


@pytest.fixture
def raw_df():
    return pd.DataFrame({
        "customerID": ["1", "2", "3", "4", "5", "5"],
        "gender": ["Male", "Female", "Male", "Female", "Male", "Male"],
        "SeniorCitizen": [0, 1, 0, 1, 0, 0],
        "Partner": ["Yes", "No", "Yes", "No", "Yes", "Yes"],
        "Dependents": ["No", "No", "Yes", "No", "Yes", "Yes"],
        "tenure": [12, 34, 5, 72, 0, 0],
        "PhoneService": ["Yes", "Yes", "No", "Yes", "Yes", "Yes"],
        "InternetService": ["DSL", "Fiber optic", "No", "DSL", "DSL", "DSL"],
        "Contract": ["Month-to-month", "One year", "Two year", "Month-to-month", "One year", "One year"],
        "MonthlyCharges": [29.85, 56.95, 20.00, 89.10, 45.00, 45.00],
        "TotalCharges": [358.2, 1936.3, 100.0, 6415.2, None, None],
        "Churn": ["No", "Yes", "No", "Yes", "No", "No"],
    })


@mock_aws
def test_clean_data(aws_settings, s3_mock, raw_df):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("staging/test.csv", _make_csv(raw_df))

    result_key = clean_data("staging/test.csv", s3)
    cleaned = pd.read_csv(BytesIO(s3.download_bytes(result_key)))

    # Should have dropped 1 duplicate (row 5 and 6 are identical except index)
    assert len(cleaned) == 5
    # TotalCharges should have no nulls (filled with median)
    assert cleaned["TotalCharges"].isna().sum() == 0


@mock_aws
def test_feature_engineer(aws_settings, s3_mock, raw_df):
    s3 = S3Client(aws_settings)
    # Use cleaned data (no nulls, no dupes)
    clean_df = raw_df.drop_duplicates(subset="customerID").copy()
    clean_df["TotalCharges"] = clean_df["TotalCharges"].fillna(0)
    s3.upload_bytes("staging/cleaned.csv", _make_csv(clean_df))

    result_key = feature_engineer("staging/cleaned.csv", s3)
    engineered = pd.read_csv(BytesIO(s3.download_bytes(result_key)))

    # Should not contain original categorical columns
    assert "gender" not in engineered.columns
    assert "customerID" not in engineered.columns
    # Churn should be numeric (0/1)
    assert engineered["Churn"].dtype in ("int64", "float64")


@mock_aws
def test_split_data(aws_settings, s3_mock, raw_df):
    s3 = S3Client(aws_settings)
    clean_df = raw_df.drop_duplicates(subset="customerID").copy()
    clean_df["TotalCharges"] = clean_df["TotalCharges"].fillna(0)
    # Create a minimal numeric dataset for splitting
    clean_df["Churn"] = clean_df["Churn"].map({"No": 0, "Yes": 1})
    clean_df = clean_df[["tenure", "MonthlyCharges", "TotalCharges", "Churn"]]
    s3.upload_bytes("staging/engineered.csv", _make_csv(clean_df))

    result = split_data("staging/engineered.csv", s3)
    assert result.train_s3_key.startswith("processed/")
    assert result.test_s3_key.startswith("processed/")

    train = pd.read_csv(BytesIO(s3.download_bytes(result.train_s3_key)))
    test = pd.read_csv(BytesIO(s3.download_bytes(result.test_s3_key)))
    assert len(train) + len(test) == len(clean_df)
