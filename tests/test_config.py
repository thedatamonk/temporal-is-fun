# tests/test_config.py
from src.config import Settings


def test_default_settings():
    s = Settings()
    assert s.temporal_host == "localhost:7233"
    assert s.default_task_queue == "default-queue"
    assert s.training_task_queue == "training-queue"
    assert s.s3_bucket == "churn-pipeline"


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("PIPELINE_S3_BUCKET", "custom-bucket")
    s = Settings()
    assert s.s3_bucket == "custom-bucket"
