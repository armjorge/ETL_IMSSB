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

output "snowflake_iam_role_arn" {
  description = "IAM role ARN for Snowflake STORAGE_AWS_ROLE_ARN (empty if disabled)"
  value       = try(aws_iam_role.snowflake[0].arn, "")
}

output "snowflake_iam_role_name" {
  description = "IAM role name for Snowflake"
  value       = try(aws_iam_role.snowflake[0].name, "")
}

output "s3_stage_url" {
  description = "S3 URL to use on the Snowflake external stage"
  value       = "s3://${aws_s3_bucket.data.bucket}/${var.root_prefix}/"
}
