output "ec2_public_ip" {
  description = "Public IP of the EC2 instance"
  value       = aws_instance.pipeline.public_ip
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.pipeline.id
}

output "sqs_queue_name" {
  description = "Name of the SQS queue"
  value       = aws_sqs_queue.s3_notifications.name
}

output "ssh_command" {
  description = "SSH command to connect to the EC2 instance"
  value       = "ssh -i ${replace(var.ssh_public_key_path, ".pub", "")} ec2-user@${aws_instance.pipeline.public_ip}"
}

output "temporal_ui_url" {
  description = "URL for the Temporal Web UI"
  value       = "http://${aws_instance.pipeline.public_ip}:8080"
}

output "instance_id" {
  description = "EC2 instance ID (for stop/start commands)"
  value       = aws_instance.pipeline.id
}
