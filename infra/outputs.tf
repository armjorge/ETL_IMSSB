output "bucket_name" {
  description = "Name of the created S3 bucket"
  value       = aws_s3_bucket.data.bucket
}

output "bucket_arn" {
  description = "ARN of the created S3 bucket"
  value       = aws_s3_bucket.data.arn
}

output "root_prefix" {
  description = "Root prefix for IMSS Bienestar sources"
  value       = var.root_prefix
}

output "source_prefixes" {
  description = "Full S3 prefixes for each source folder"
  value = [
    for folder in var.source_folders :
    "s3://${aws_s3_bucket.data.bucket}/${var.root_prefix}/${folder}/"
  ]
}
