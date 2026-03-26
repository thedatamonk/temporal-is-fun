from io import BytesIO

import pandas as pd
import pytest
from moto import mock_aws
from sklearn.datasets import make_classification
from sklearn.ensemble import RandomForestClassifier

from src.config import Settings
from src.models.churn_model import serialize, train_churn_model
from src.s3_client import S3Client
from src.workflows.evaluation import (
    EvaluationResult,
    evaluate_model,
    store_artifacts,
)


@pytest.fixture
def trained_model_and_test_data():
    X, y = make_classification(n_samples=100, n_features=5, random_state=42)
    df = pd.DataFrame(X, columns=[f"f{i}" for i in range(5)])
    df["Churn"] = y

    train_df = df.iloc[:80]
    test_df = df.iloc[80:]

    model, _ = train_churn_model(train_df)
    model_bytes = serialize(model)
    test_csv = test_df.to_csv(index=False).encode()

    return model_bytes, test_csv


@mock_aws
def test_evaluate_model(aws_settings, s3_mock, trained_model_and_test_data):
    model_bytes, test_csv = trained_model_and_test_data
    s3 = S3Client(aws_settings)
    s3.upload_bytes("models/test-run/model.pkl", model_bytes)
    s3.upload_bytes("processed/test.csv", test_csv)

    metrics = evaluate_model("models/test-run/model.pkl", "processed/test.csv", s3)

    assert "accuracy" in metrics
    assert "precision" in metrics
    assert "recall" in metrics
    assert "f1" in metrics
    assert 0 <= metrics["accuracy"] <= 1


@mock_aws
def test_store_artifacts(aws_settings, s3_mock):
    s3 = S3Client(aws_settings)

    metrics = {"accuracy": 0.92, "f1": 0.89}
    training_metadata = {"n_estimators": 100}
    result = store_artifacts(
        metrics=metrics,
        training_metadata=training_metadata,
        model_s3_key="models/test-run/model.pkl",
        run_id="test-run",
        row_count=100,
        s3=s3,
    )

    assert result.artifact_s3_key.startswith("artifacts/")
    assert result.metrics["accuracy"] == 0.92

    # Verify JSON was uploaded
    artifact = s3.download_json(result.artifact_s3_key)
    assert artifact["metrics"]["accuracy"] == 0.92
    assert artifact["model_s3_key"] == "models/test-run/model.pkl"
