# IAM role + policy so Snowflake can LIST/GET objects under
# s3://{bucket}/{root_prefix}/ via a storage integration.
#
# Bootstrap (first apply): trust uses this AWS account + placeholder external ID.
# After CREATE STORAGE INTEGRATION + DESC in Snowflake, set:
#   snowflake_iam_user_arn  = <STORAGE_AWS_IAM_USER_ARN>
#   snowflake_external_id   = <STORAGE_AWS_EXTERNAL_ID>
# then terraform apply again (or let setup_s3_stage.sh do it).

variable "enable_snowflake_s3_access" {
  description = "Create IAM role/policy for Snowflake storage integration"
  type        = bool
  default     = true
}

variable "snowflake_role_name" {
  description = "IAM role name assumed by Snowflake"
  type        = string
  default     = "snowflake-s3-imss-bienestar"
}

variable "snowflake_iam_user_arn" {
  description = "STORAGE_AWS_IAM_USER_ARN from DESC INTEGRATION (empty = bootstrap trust)"
  type        = string
  default     = ""
}

variable "snowflake_external_id" {
  description = "STORAGE_AWS_EXTERNAL_ID from DESC INTEGRATION (placeholder until DESC)"
  type        = string
  default     = "0000"
}

data "aws_caller_identity" "current" {}

locals {
  snowflake_prefix_arn = "${aws_s3_bucket.data.arn}/${var.root_prefix}/*"
  snowflake_trust_principal = (
    var.snowflake_iam_user_arn != ""
    ? var.snowflake_iam_user_arn
    : "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
  )
}

resource "aws_iam_policy" "snowflake_s3" {
  count = var.enable_snowflake_s3_access ? 1 : 0

  name        = "${var.snowflake_role_name}-policy"
  description = "Read/list (and optional write) for Snowflake on ${var.bucket_name}/${var.root_prefix}/"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:DeleteObjectVersion",
        ]
        Resource = local.snowflake_prefix_arn
      },
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = aws_s3_bucket.data.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["${var.root_prefix}/*"]
          }
        }
      },
    ]
  })
}

resource "aws_iam_role" "snowflake" {
  count = var.enable_snowflake_s3_access ? 1 : 0

  name        = var.snowflake_role_name
  description = "Assumed by Snowflake storage integration for IMSS Bienestar S3"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = local.snowflake_trust_principal
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.snowflake_external_id
          }
        }
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "snowflake_s3" {
  count = var.enable_snowflake_s3_access ? 1 : 0

  role       = aws_iam_role.snowflake[0].name
  policy_arn = aws_iam_policy.snowflake_s3[0].arn
}
