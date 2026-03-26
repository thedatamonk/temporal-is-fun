import asyncio
from concurrent.futures import ThreadPoolExecutor

from temporalio.client import Client
from temporalio.worker import Worker

from src.config import settings
from src.workflows.training import TrainingWorkflow, train_model, serialize_model


async def main():
    client = await Client.connect(settings.temporal_host)

    worker = Worker(
        client,
        task_queue=settings.training_task_queue,
        workflows=[TrainingWorkflow],
        activities=[train_model, serialize_model],
        activity_executor=ThreadPoolExecutor(max_workers=5),
    )

    print(f"Training worker started on queue: {settings.training_task_queue}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
