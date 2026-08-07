variable "aws_region" {
  description = "AWS region for the S3 bucket"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "Optional AWS CLI profile. Leave empty to use the default credential chain."
  type        = string
  default     = ""
}

variable "bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
  default     = "so3-data"
}

variable "root_prefix" {
  description = "Root prefix under which source folders live"
  type        = string
  default     = "imss_bienestar"
}

variable "source_folders" {
  description = "Folder prefixes created under root_prefix"
  type        = list(string)
  default = [
    "camunda",
    "sagi",
    "invoicing",
    "payments",
    "banking",
    "institution_status",
  ]
}

variable "enable_snowpipe_sns" {
  description = "Create SNS topics + S3 event notifications for Snowpipe auto-ingest"
  type        = bool
  default     = true
}

variable "snowpipe_folders" {
  description = "Map of S3 folder under root_prefix → Iceberg table name (Snowpipe targets)"
  type        = map(string)
  default = {
    camunda   = "camunda_orders"
    payments  = "payments"
    invoicing = "invoicing"
    sagi      = "sagi"
  }
}

variable "snowflake_aws_account_id" {
  description = "Snowflake-managed AWS account ID (externalstages IAM users; SNS:Subscribe)"
  type        = string
  default     = "974916068036"
}
