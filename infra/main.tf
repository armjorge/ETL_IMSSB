resource "aws_s3_bucket" "data" {
  bucket = var.bucket_name

  tags = {
    Project     = "ETL_IMSSB"
    ManagedBy   = "terraform"
    RootPrefix  = var.root_prefix
  }
}

resource "aws_s3_bucket_public_access_block" "data" {
  bucket = aws_s3_bucket.data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_ownership_controls" "data" {
  bucket = aws_s3_bucket.data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Zero-byte placeholders so prefixes appear as folders in the console
resource "aws_s3_object" "source_prefixes" {
  for_each = toset(var.source_folders)

  bucket  = aws_s3_bucket.data.id
  key     = "${var.root_prefix}/${each.value}/"
  content = ""
}
