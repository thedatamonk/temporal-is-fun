import boto3
import pytest
from moto import mock_aws

from src.config import Settings


@pytest.fixture
def aws_settings():
    return Settings(
        s3_endpoint_url=None,
        sqs_endpoint_url=None,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        s3_region="us-east-1",
    )


@pytest.fixture
def s3_mock(aws_settings):
    with mock_aws():
        client = boto3.client("s3", region_name=aws_settings.s3_region)
        client.create_bucket(Bucket=aws_settings.s3_bucket)
        yield client


@pytest.fixture
def sample_csv_bytes():
    return b"customerID,gender,SeniorCitizen,tenure,MonthlyCharges,Churn\n1,Male,0,12,29.85,No\n2,Female,1,34,56.95,Yes\n"
