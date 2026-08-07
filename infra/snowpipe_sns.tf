# SNS topics + S3 ObjectCreated notifications for Snowpipe AUTO_INGEST.
# One topic per folder so a Camunda upload does not wake payments/invoicing pipes.
# Iceberg writes under {root_prefix}/iceberg/ are intentionally not notified.

resource "aws_sns_topic" "snowpipe" {
  for_each = var.enable_snowpipe_sns ? var.snowpipe_folders : {}

  name = "${var.bucket_name}-${replace(var.root_prefix, "/", "-")}-${each.key}-snowpipe"

  tags = {
    Project    = "ETL_IMSSB"
    ManagedBy  = "terraform"
    RootPrefix = var.root_prefix
    Source     = each.key
    Table      = each.value
  }
}

# CREATE PIPE AWS_SNS_TOPIC needs SNS:Subscribe for Snowflake's SQS endpoint
# (principal account: var.snowflake_aws_account_id).

data "aws_iam_policy_document" "snowpipe_sns" {
  for_each = aws_sns_topic.snowpipe

  statement {
    sid    = "AllowS3Publish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    actions   = ["SNS:Publish"]
    resources = [each.value.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.data.arn]
    }
  }

  statement {
    sid    = "AllowSnowflakeSubscribe"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.snowflake_aws_account_id}:root"]
    }

    actions = [
      "SNS:Subscribe",
      "SNS:GetTopicAttributes",
    ]
    resources = [each.value.arn]
  }
}

resource "aws_sns_topic_policy" "snowpipe" {
  for_each = aws_sns_topic.snowpipe

  arn    = each.value.arn
  policy = data.aws_iam_policy_document.snowpipe_sns[each.key].json
}

resource "aws_s3_bucket_notification" "snowpipe" {
  count = var.enable_snowpipe_sns && length(var.snowpipe_folders) > 0 ? 1 : 0

  bucket = aws_s3_bucket.data.id

  dynamic "topic" {
    for_each = aws_sns_topic.snowpipe

    content {
      topic_arn     = topic.value.arn
      events        = ["s3:ObjectCreated:*"]
      filter_prefix = "${var.root_prefix}/${topic.key}/"
      filter_suffix = ".csv"
    }
  }

  depends_on = [aws_sns_topic_policy.snowpipe]
}
