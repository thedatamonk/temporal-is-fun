resource "aws_sqs_queue" "s3_notifications" {
  name                       = "${var.project_name}-s3-notifications"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400

  tags = {
    Name = "${var.project_name}-s3-notifications"
  }
}

resource "aws_sqs_queue_policy" "allow_s3" {
  queue_url = aws_sqs_queue.s3_notifications.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action    = "sqs:SendMessage"
        Resource  = aws_sqs_queue.s3_notifications.arn
        Condition = {
          ArnEquals = {
            "aws:SourceArn" = aws_s3_bucket.pipeline.arn
          }
        }
      }
    ]
  })
}
