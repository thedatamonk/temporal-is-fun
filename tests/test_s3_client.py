from moto import mock_aws

from src.s3_client import S3Client


@mock_aws
def test_upload_and_download(aws_settings, s3_mock, sample_csv_bytes):
    client = S3Client(aws_settings)
    key = "raw/test.csv"

    client.upload_bytes(key, sample_csv_bytes)
    result = client.download_bytes(key)

    assert result == sample_csv_bytes


@mock_aws
def test_upload_and_download_json(aws_settings, s3_mock):
    client = S3Client(aws_settings)
    key = "artifacts/meta.json"
    data = {"accuracy": 0.95}

    client.upload_json(key, data)
    result = client.download_json(key)

    assert result == data
