from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from io import BytesIO

    import pandas as pd
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

    from src.config import settings
    from src.models.churn_model import load_model
    from src.s3_client import S3Client


@dataclass
class EvaluationResult:
    metrics: dict
    artifact_s3_key: str


@activity.defn
def evaluate_model(
    model_s3_key: str,
    test_s3_key: str,
    s3: S3Client | None = None,
) -> dict:
    if s3 is None:
        s3 = S3Client()

    model = load_model(s3.download_bytes(model_s3_key))
    test_df = pd.read_csv(BytesIO(s3.download_bytes(test_s3_key)))

    X_test = test_df.drop(columns=["Churn"])
    y_test = test_df["Churn"]
    y_pred = model.predict(X_test)

    try:
        activity.heartbeat("evaluation complete")
    except Exception:
        pass

    return {
        "accuracy": round(accuracy_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred, zero_division=0), 4),
        "recall": round(recall_score(y_test, y_pred, zero_division=0), 4),
        "f1": round(f1_score(y_test, y_pred, zero_division=0), 4),
    }


@activity.defn
def store_artifacts(
    metrics: dict,
    training_metadata: dict,
    model_s3_key: str,
    run_id: str,
    row_count: int,
    s3: S3Client | None = None,
) -> EvaluationResult:
    if s3 is None:
        s3 = S3Client()

    artifact = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "model_s3_key": model_s3_key,
        "metrics": metrics,
        "training_metadata": training_metadata,
        "dataset_info": {"row_count": row_count},
    }

    artifact_key = f"{settings.artifacts_prefix}{run_id}/metadata.json"
    s3.upload_json(artifact_key, artifact)

    return EvaluationResult(metrics=metrics, artifact_s3_key=artifact_key)


@workflow.defn
class EvaluationWorkflow:
    @workflow.run
    async def run(
        self,
        model_s3_key: str,
        test_s3_key: str,
        training_metadata: dict,
        run_id: str,
        row_count: int,
    ) -> EvaluationResult:
        retry = RetryPolicy(
            maximum_attempts=4,
            initial_interval=timedelta(seconds=5),
            backoff_coefficient=2.0,
        )

        metrics = await workflow.execute_activity(
            evaluate_model,
            args=[model_s3_key, test_s3_key],
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(seconds=30),
            retry_policy=retry,
        )
        result = await workflow.execute_activity(
            store_artifacts,
            args=[metrics, training_metadata, model_s3_key, run_id, row_count],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=retry,
        )
        return result
