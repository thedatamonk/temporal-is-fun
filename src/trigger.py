# src/trigger.py
import asyncio
import json

import boto3
from temporalio.client import Client

from src.config import settings
from src.workflows.parent import ChurnPipelineWorkflow, PipelineInput


def parse_s3_event(event_body: str) -> tuple[str, str | None]:
    event = json.loads(event_body)

    # LocalStack may wrap the S3 event in a "Message" key
    if "Message" in event:
        event = json.loads(event["Message"])

    if "Records" not in event:
        return "", None

    record = event["Records"][0]
    bucket = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    if not key.startswith(settings.raw_prefix):
        return bucket, None

    return bucket, key


def build_workflow_id(s3_key: str) -> str:
    return f"churn-pipeline-{s3_key}"


async def poll_and_trigger():
    sqs = boto3.client(
        "sqs",
        endpoint_url=settings.sqs_endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=settings.aws_access_key_id,
        aws_secret_access_key=settings.aws_secret_access_key,
    )

    client = await Client.connect(settings.temporal_host)

    queue_url = sqs.get_queue_url(QueueName=settings.sqs_queue_name)["QueueUrl"]
    print(f"Trigger polling SQS queue: {settings.sqs_queue_name}")

    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )

        messages = response.get("Messages", [])
        for msg in messages:
            bucket, key = parse_s3_event(msg["Body"])
            if key is not None:
                workflow_id = build_workflow_id(key)
                print(f"Starting pipeline for: {key} (workflow: {workflow_id})")

                await client.start_workflow(
                    ChurnPipelineWorkflow.run,
                    PipelineInput(raw_s3_key=key),
                    id=workflow_id,
                    task_queue=settings.default_task_queue,
                )

            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )


if __name__ == "__main__":
    asyncio.run(poll_and_trigger())
