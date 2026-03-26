from io import BytesIO

import pandas as pd
import pytest
from moto import mock_aws
from sklearn.datasets import make_classification

from src.config import Settings
from src.models.churn_model import train_churn_model, load_model
from src.s3_client import S3Client
from src.workflows.training import (
    TrainingResult,
    train_model,
    serialize_model,
)


@pytest.fixture
def train_csv_bytes():
    X, y = make_classification(n_samples=100, n_features=5, random_state=42)
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(5)])
    df["Churn"] = y
    return df.to_csv(index=False).encode()


def test_train_churn_model():
    X, y = make_classification(n_samples=50, n_features=5, n_informative=2, n_redundant=2, random_state=42)
    df = pd.DataFrame(X, columns=["a", "b", "c", "d", "e"])
    df["Churn"] = y
    model, metadata = train_churn_model(df)

    assert model is not None
    assert "n_estimators" in metadata
    assert "training_time_seconds" in metadata


@mock_aws
def test_train_model_activity(aws_settings, s3_mock, train_csv_bytes):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("processed/train.csv", train_csv_bytes)

    model_bytes, metadata = train_model("processed/train.csv", s3)
    assert len(model_bytes) > 0
    assert "n_estimators" in metadata


@mock_aws
def test_serialize_model_activity(aws_settings, s3_mock, train_csv_bytes):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("processed/train.csv", train_csv_bytes)

    model_bytes, metadata = train_model("processed/train.csv", s3)
    result = serialize_model(model_bytes, metadata, "test-run-id", s3)

    assert result.model_s3_key.startswith("models/")
    assert result.training_metadata["n_estimators"] == 100

    # Verify model was uploaded
    downloaded = s3.download_bytes(result.model_s3_key)
    assert len(downloaded) > 0
