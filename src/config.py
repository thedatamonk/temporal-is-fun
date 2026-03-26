from pydantic import field_validator
from pydantic_settings import BaseSettings


def _empty_to_none(v: str | None) -> str | None:
    """Convert empty strings to None (for env vars like PIPELINE_S3_ENDPOINT_URL=)."""
    if v is not None and v.strip() == "":
        return None
    return v


class Settings(BaseSettings):
    # Temporal
    temporal_host: str = "localhost:7233"
    temporal_namespace: str = "default"
    default_task_queue: str = "default-queue"
    training_task_queue: str = "training-queue"

    # S3
    s3_endpoint_url: str | None = "http://localhost:4566"
    s3_bucket: str = "churn-pipeline"
    s3_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None

    # SQS
    sqs_endpoint_url: str | None = "http://localhost:4566"

    @field_validator("s3_endpoint_url", "sqs_endpoint_url", "aws_access_key_id", "aws_secret_access_key", mode="before")
    @classmethod
    def empty_str_to_none(cls, v: str | None) -> str | None:
        return _empty_to_none(v)
    sqs_queue_name: str = "s3-notifications"

    # S3 prefixes
    raw_prefix: str = "raw/"
    staging_prefix: str = "staging/"
    processed_prefix: str = "processed/"
    models_prefix: str = "models/"
    artifacts_prefix: str = "artifacts/"

    model_config = {"env_prefix": "PIPELINE_"}


settings = Settings()
