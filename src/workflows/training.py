from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from io import BytesIO

    import pandas as pd

    from src.config import settings
    from src.models.churn_model import train_churn_model, serialize as serialize_model_obj
    from src.s3_client import S3Client


@dataclass
class TrainingResult:
    model_s3_key: str
    training_metadata: dict


@activity.defn
def train_model(train_s3_key: str, s3: S3Client | None = None) -> tuple[bytes, dict]:
    if s3 is None:
        s3 = S3Client()
    data = s3.download_bytes(train_s3_key)
    df = pd.read_csv(BytesIO(data))

    model, metadata = train_churn_model(df)
    model_bytes = serialize_model_obj(model)

    try:
        activity.heartbeat("training complete")
    except Exception:
        pass

    return model_bytes, metadata


@activity.defn
def serialize_model(
    model_bytes: bytes,
    metadata: dict,
    run_id: str,
    s3: S3Client | None = None,
) -> TrainingResult:
    if s3 is None:
        s3 = S3Client()
    model_key = f"{settings.models_prefix}{run_id}/model.pkl"
    s3.upload_bytes(model_key, model_bytes)
    return TrainingResult(model_s3_key=model_key, training_metadata=metadata)


@workflow.defn
class TrainingWorkflow:
    @workflow.run
    async def run(self, train_s3_key: str, run_id: str) -> TrainingResult:
        model_bytes, metadata = await workflow.execute_activity(
            train_model,
            train_s3_key,
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
            ),
        )
        result = await workflow.execute_activity(
            serialize_model,
            args=[model_bytes, metadata, run_id],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=RetryPolicy(
                maximum_attempts=4,
                initial_interval=timedelta(seconds=5),
                backoff_coefficient=2.0,
            ),
        )
        return result
