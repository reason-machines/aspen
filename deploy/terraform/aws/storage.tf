# The pack bucket: private, encrypted, versioned, TLS-only. Only the Aspen
# task role reads and writes it (compute.tf); clients reach packs through
# presigned URLs, the servers' /-/express, or CloudFront in front of
# /-/express, never through a bucket policy.

resource "aws_s3_bucket" "packs" {
  bucket_prefix = "${var.name}-packs-"
  force_destroy = var.bucket_force_destroy
}

resource "aws_s3_bucket_ownership_controls" "packs" {
  bucket = aws_s3_bucket.packs.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "packs" {
  bucket = aws_s3_bucket.packs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "packs" {
  bucket = aws_s3_bucket.packs.id

  rule {
    # SSE-S3: free, and presigned URLs need no KMS key grants.
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "packs" {
  bucket = aws_s3_bucket.packs.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "packs" {
  bucket = aws_s3_bucket.packs.id

  # Interrupted pushes leave multipart uploads behind; their parts are billed
  # until aborted.
  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_incomplete_multipart_days
    }
  }

  # Versioning protects against accidental deletes; keep old versions for a
  # bounded time only.
  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}

    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }

    expiration {
      expired_object_delete_marker = true
    }
  }

  depends_on = [aws_s3_bucket_versioning.packs]
}

data "aws_iam_policy_document" "packs_bucket" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.packs.arn,
      "${aws_s3_bucket.packs.arn}/*",
    ]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "packs" {
  bucket = aws_s3_bucket.packs.id
  policy = data.aws_iam_policy_document.packs_bucket.json

  depends_on = [aws_s3_bucket_public_access_block.packs]
}

