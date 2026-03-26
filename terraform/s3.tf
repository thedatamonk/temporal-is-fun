resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "pipeline" {
  bucket        = "${var.project_name}-${random_id.bucket_suffix.hex}"
  force_destroy = true

  tags = {
    Name = "${var.project_name}-bucket"
  }
}

resource "aws_s3_bucket_notification" "sqs" {
  bucket = aws_s3_bucket.pipeline.id

  queue {
    queue_arn     = aws_sqs_queue.s3_notifications.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "raw/"
  }

  depends_on = [aws_sqs_queue_policy.allow_s3]
}
