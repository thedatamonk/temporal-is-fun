import json

import boto3

from src.config import Settings


class S3Client:
    def __init__(self, settings: Settings | None = None):
        if settings is None:
            settings = Settings()
        self._settings = settings
        self._client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )

    @property
    def bucket(self) -> str:
        return self._settings.s3_bucket

    def upload_bytes(self, key: str, data: bytes) -> str:
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return key

    def download_bytes(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read()

    def upload_json(self, key: str, data: dict) -> str:
        return self.upload_bytes(key, json.dumps(data).encode())

    def download_json(self, key: str) -> dict:
        return json.loads(self.download_bytes(key))
