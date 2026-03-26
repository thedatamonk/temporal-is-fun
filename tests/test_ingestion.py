import pytest
from moto import mock_aws

from src.config import Settings
from src.s3_client import S3Client
from src.workflows.ingestion import (
    IngestionResult,
    download_from_s3,
    validate_schema,
)

VALID_CSV = (
    b"customerID,gender,SeniorCitizen,Partner,Dependents,tenure,"
    b"PhoneService,InternetService,Contract,MonthlyCharges,TotalCharges,Churn\n"
    b"1,Male,0,Yes,No,12,Yes,DSL,Month-to-month,29.85,358.2,No\n"
)

INVALID_CSV = b"id,name,value\n1,test,100\n"


@mock_aws
def test_download_from_s3(aws_settings, s3_mock):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("raw/test.csv", VALID_CSV)

    result = download_from_s3("raw/test.csv", s3)
    assert result.startswith(aws_settings.staging_prefix)

    downloaded = s3.download_bytes(result)
    assert downloaded == VALID_CSV


@mock_aws
def test_validate_schema_valid(aws_settings, s3_mock):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("staging/test.csv", VALID_CSV)

    result = validate_schema("staging/test.csv", s3)
    assert result.row_count == 1
    assert result.s3_key == "staging/test.csv"


@mock_aws
def test_validate_schema_invalid(aws_settings, s3_mock):
    s3 = S3Client(aws_settings)
    s3.upload_bytes("staging/bad.csv", INVALID_CSV)

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_schema("staging/bad.csv", s3)
