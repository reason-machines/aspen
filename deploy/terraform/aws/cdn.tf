# CloudFront in front of Aspen's /-/express, for packfile-uri downloads
# (node C2, docs/EXPRESS.md "Putting a CDN in front").
#
# The origin is the Git hostname itself (the ALB), and every task serves
# /-/express there (ASPEN_EXPRESS_SERVE=true). Express URLs carry their own
# signature and expiry as path segments; the origin verifies both, so
# CloudFront needs no signing keys, and the bucket stays private to the task
# role. Git's smart-HTTP traffic never goes through CloudFront: the viewer
# function below answers 404 for every path outside /-/express.
#
# Caching follows the origin: express answers `Cache-Control: public,
# max-age=<seconds until the URL expires>, immutable, must-revalidate,
# no-transform` for a pack and `no-store` for everything else. CloudFront
# serves an expired object when the origin errors, whatever Cache-Control
# says, so the viewer function also refuses every URL past its expiry before
# the cache is consulted.

resource "aws_acm_certificate" "cdn" {
  count    = var.enable_cdn ? 1 : 0
  provider = aws.us_east_1

  domain_name       = local.cdn_domain_name
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "cdn_validation" {
  # Keyed by the hostname, which is known at plan time.
  for_each = var.enable_cdn ? toset([local.cdn_domain_name]) : toset([])

  zone_id = var.route53_zone_id
  name    = local.cdn_validation[each.key].resource_record_name
  type    = local.cdn_validation[each.key].resource_record_type
  records = [local.cdn_validation[each.key].resource_record_value]
  ttl     = 300
}

locals {
  cdn_validation = var.enable_cdn ? {
    for o in aws_acm_certificate.cdn[0].domain_validation_options : o.domain_name => o
  } : {}
}

resource "aws_acm_certificate_validation" "cdn" {
  count    = var.enable_cdn ? 1 : 0
  provider = aws.us_east_1

  certificate_arn         = aws_acm_certificate.cdn[0].arn
  validation_record_fqdns = [for r in aws_route53_record.cdn_validation : r.fqdn]
}

resource "aws_cloudfront_function" "express" {
  count = var.enable_cdn ? 1 : 0

  name    = "${var.name}-express-viewer-request"
  runtime = "cloudfront-js-2.0"
  comment = "Serves only unexpired /-/express URLs"
  publish = true
  code    = file("${path.module}/cdn/viewer-request.js")
}

# The cache key is the URL path alone: the signature and expiry are path
# segments, and the query string is never used. TTLs come from the origin's
# Cache-Control, bounded by the longest life of an express URL (1 hour).
resource "aws_cloudfront_cache_policy" "express" {
  count = var.enable_cdn ? 1 : 0

  name        = "${var.name}-express"
  comment     = "Aspen express packs: path-only key, origin Cache-Control"
  min_ttl     = 0
  default_ttl = 0
  max_ttl     = 3600

  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_gzip   = false
    enable_accept_encoding_brotli = false

    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
  }
}

# ---------------------------------------------------------------------------
# Access logs. Cache hits never reach the origin, so CloudFront's own logs
# are the only record of what the CDN served: who downloaded what, and how
# often (the first thing to read when a CloudFront budget alert fires).
# Standard logs need a bucket with ACLs enabled; CloudFront grants its log
# delivery account write access when the distribution is created. Kept for
# cdn_log_retention_days, then deleted. The logs hold client addresses and
# express URLs, whose signatures have expired by the time anyone reads them.
# ---------------------------------------------------------------------------

locals {
  cdn_logs = var.enable_cdn && var.cdn_log_retention_days > 0
}

resource "aws_s3_bucket" "cdn_logs" {
  count = local.cdn_logs ? 1 : 0

  bucket_prefix = "${var.name}-cdn-logs-"
  force_destroy = true # logs only; they expire anyway
}

resource "aws_s3_bucket_ownership_controls" "cdn_logs" {
  count  = local.cdn_logs ? 1 : 0
  bucket = aws_s3_bucket.cdn_logs[0].id

  rule {
    object_ownership = "BucketOwnerPreferred" # CloudFront standard logs need ACLs
  }
}

resource "aws_s3_bucket_public_access_block" "cdn_logs" {
  count  = local.cdn_logs ? 1 : 0
  bucket = aws_s3_bucket.cdn_logs[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "cdn_logs" {
  count  = local.cdn_logs ? 1 : 0
  bucket = aws_s3_bucket.cdn_logs[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256" # CloudFront cannot write to SSE-KMS buckets
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "cdn_logs" {
  count  = local.cdn_logs ? 1 : 0
  bucket = aws_s3_bucket.cdn_logs[0].id

  rule {
    id     = "expire-logs"
    status = "Enabled"
    filter {}

    expiration {
      days = var.cdn_log_retention_days
    }
  }
}

resource "aws_cloudfront_distribution" "packs" {
  count = var.enable_cdn ? 1 : 0

  enabled         = true
  comment         = "${var.name} pack downloads (origin: /-/express)"
  aliases         = [local.cdn_domain_name]
  price_class     = var.cdn_price_class
  is_ipv6_enabled = true
  http_version    = "http2and3"

  origin {
    origin_id   = "express"
    domain_name = var.domain_name

    custom_origin_config {
      http_port                = 80
      https_port               = 443
      origin_protocol_policy   = "https-only"
      origin_ssl_protocols     = ["TLSv1.2"]
      origin_read_timeout      = 60
      origin_keepalive_timeout = 60
    }
  }

  # Range, If-None-Match and If-Range are CloudFront's own business: it
  # serves ranges and conditional requests from its cache and asks the
  # origin for ranges or the whole pack on a miss. Nothing from the viewer
  # (cookies, Authorization, query) is forwarded.
  default_cache_behavior {
    target_origin_id       = "express"
    viewer_protocol_policy = "https-only"
    allowed_methods        = ["GET", "HEAD"]
    cached_methods         = ["GET", "HEAD"]
    cache_policy_id        = aws_cloudfront_cache_policy.express[0].id
    compress               = false # packs are already zlib-compressed

    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.express[0].arn
    }
  }

  # Do not hold on to origin errors: a 5xx says nothing about the pack, and
  # the servers' health probe runs through this distribution.
  dynamic "custom_error_response" {
    for_each = [500, 502, 503, 504]

    content {
      error_code            = custom_error_response.value
      error_caching_min_ttl = 0
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.cdn[0].certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }

  dynamic "logging_config" {
    for_each = local.cdn_logs ? [1] : []

    content {
      bucket          = aws_s3_bucket.cdn_logs[0].bucket_domain_name
      prefix          = "cloudfront/"
      include_cookies = false
    }
  }

  depends_on = [aws_s3_bucket_ownership_controls.cdn_logs]
}

resource "aws_route53_record" "cdn" {
  for_each = var.enable_cdn ? toset(["A", "AAAA"]) : toset([])

  zone_id = var.route53_zone_id
  name    = local.cdn_domain_name
  type    = each.value

  alias {
    name                   = aws_cloudfront_distribution.packs[0].domain_name
    zone_id                = aws_cloudfront_distribution.packs[0].hosted_zone_id
    evaluate_target_health = false
  }
}
