#!/bin/bash
awslocal s3 mb s3://churn-pipeline
awslocal sqs create-queue --queue-name s3-notifications
awslocal s3api put-bucket-notification-configuration \
  --bucket churn-pipeline \
  --notification-configuration '{
    "QueueConfigurations": [{
      "QueueArn": "arn:aws:sqs:us-east-1:000000000000:s3-notifications",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{"Name": "prefix", "Value": "raw/"}]
        }
      }
    }]
  }'
echo "LocalStack initialized: S3 bucket + SQS queue + event notification"
