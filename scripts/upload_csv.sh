#!/bin/bash
set -e

CSV_FILE="${1:-data/sample_churn.csv}"
S3_KEY="raw/$(basename "$CSV_FILE")"

echo "Uploading $CSV_FILE to s3://churn-pipeline/$S3_KEY ..."
aws --endpoint-url=http://localhost:4566 s3 cp "$CSV_FILE" "s3://churn-pipeline/$S3_KEY"
echo "Done. Pipeline should start shortly — check http://localhost:8080"
