output "bucket_name" {
  description = "S3 bucket name — set S3_BUCKET in .env to this value"
  value       = aws_s3_bucket.raw.id
}

output "bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "aws_access_key_id" {
  description = "Set AWS_ACCESS_KEY_ID in .env to this value"
  value       = aws_iam_access_key.poller.id
}

output "aws_secret_access_key" {
  description = "Set AWS_SECRET_ACCESS_KEY in .env to this value — treat as a secret"
  value       = aws_iam_access_key.poller.secret
  sensitive   = true
}
