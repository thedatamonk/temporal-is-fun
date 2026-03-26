import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from src.config import settings
from src.workflows.parent import ChurnPipelineWorkflow
from src.workflows.ingestion import IngestionWorkflow, download_from_s3, validate_schema
from src.workflows.preprocessing import (
    PreprocessingWorkflow, clean_data, feature_engineer, split_data,
)
from src.workflows.evaluation import EvaluationWorkflow, evaluate_model, store_artifacts


async def main():
    client = await Client.connect(settings.temporal_host)

    worker = Worker(
        client,
        task_queue=settings.default_task_queue,
        workflows=[
            ChurnPipelineWorkflow,
            IngestionWorkflow,
            PreprocessingWorkflow,
            EvaluationWorkflow,
        ],
        activities=[
            download_from_s3,
            validate_schema,
            clean_data,
            feature_engineer,
            split_data,
            evaluate_model,
            store_artifacts,
        ],
        activity_executor=ThreadPoolExecutor(max_workers=10),
    )

    print(f"Default worker started on queue: {settings.default_task_queue}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
