# A CDN for the live canvas image at live.<place host>, for pages that embed
# the canvas and poll it (Reason's blog post on Aspen does, every 2 seconds,
# with thousands of readers at once).
#
# The place task renders /api/v1/canvas.svg once per merge and answers
# `Cache-Control: no-cache, max-age=0`, which is right for its own page and
# for GitHub's camo proxy of place.<host>/api/v1/canvas.svg (the
# place-canvas README): both must always see the latest merge. That URL and
# its headers stay as they are. This distribution serves the same image
# under another hostname, cached for 2 seconds at most per key, so however
# many viewers poll it the origin sees about one request per 2 seconds:
#
# - Only GET and HEAD of /api/v1/canvas.svg; the viewer function answers 404
#   for every other path, so nothing else on place (the API, canvas.git,
#   the admin endpoints) is reachable through it.
# - The cache key is the path and the query parameter `t`, which the viewer
#   function sets to the current 2-second bucket, floor(now / 2000 ms),
#   whatever the viewer sent. Every viewer in the same 2 seconds shares one
#   object, and a viewer cannot bust the cache with its own `t` values.
#   Pages may still send ?t=<floor(Date.now()/2000)>, which keeps the
#   browser's own cache out of the way.
# - min_ttl > 0 makes CloudFront cache the image despite the origin's
#   no-cache; a key lives at most 2 seconds, and the next bucket's key misses
#   and fetches the latest render. A merge is visible within about one
#   bucket plus the page's poll interval.
# - Origin Shield in the origin's region collapses the edge locations'
#   misses into one origin request per key.
# - CloudFront compresses the SVG (gzip or brotli, about 42 KB to 7 KB);
#   the origin does not.
#
# The account stack's aspen-cloudfront-monthly budget filters by service, so
# it watches this distribution as well as the pack CDN. To stop it, set
# place_live_cdn = false and apply: the distribution goes away and pages
# that embed it show a broken image until they fall back.

locals {
  place_live      = local.place && var.place_live_cdn
  place_live_host = "live.${local.place_host}"
}

resource "aws_acm_certificate" "place_live" {
  count    = local.place_live ? 1 : 0
  provider = aws.us_east_1 # CloudFront certificates live in us-east-1

  domain_name       = local.place_live_host
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "place_live_validation" {
  # Keyed by the hostname, which is known at plan time.
  for_each = local.place_live ? toset([local.place_live_host]) : toset([])

  zone_id = var.route53_zone_id
  name    = local.place_live_validation[each.key].resource_record_name
  type    = local.place_live_validation[each.key].resource_record_type
  records = [local.place_live_validation[each.key].resource_record_value]
  ttl     = 300
}

locals {
  place_live_validation = local.place_live ? {
    for o in aws_acm_certificate.place_live[0].domain_validation_options : o.domain_name => o
  } : {}
}

resource "aws_acm_certificate_validation" "place_live" {
  count    = local.place_live ? 1 : 0
  provider = aws.us_east_1

  certificate_arn         = aws_acm_certificate.place_live[0].arn
  validation_record_fqdns = [for r in aws_route53_record.place_live_validation : r.fqdn]
}

resource "aws_cloudfront_function" "place_live" {
  count = local.place_live ? 1 : 0

  name    = "${local.name}-place-live-viewer-request"
  runtime = "cloudfront-js-2.0"
  comment = "Serves only /api/v1/canvas.svg, keyed by the current 2-second bucket"
  publish = true
  code    = file("${path.module}/place-live-viewer-request.js")
}

resource "aws_cloudfront_cache_policy" "place_live" {
  count = local.place_live ? 1 : 0

  name        = "${local.name}-place-live"
  comment     = "place canvas image: path and t, 2 s"
  min_ttl     = 2
  default_ttl = 2
  max_ttl     = 10

  parameters_in_cache_key_and_forwarded_to_origin {
    enable_accept_encoding_gzip   = true
    enable_accept_encoding_brotli = true

    cookies_config {
      cookie_behavior = "none"
    }
    headers_config {
      header_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "whitelist"
      query_strings {
        items = ["t"]
      }
    }
  }
}

# The origin already sends Access-Control-Allow-Origin: *; this keeps it on
# every response, including ones CloudFront makes itself.
resource "aws_cloudfront_response_headers_policy" "place_live" {
  count = local.place_live ? 1 : 0

  name    = "${local.name}-place-live"
  comment = "place canvas image: readable from any page"

  cors_config {
    access_control_allow_credentials = false
    origin_override                  = false

    access_control_allow_headers {
      items = ["*"]
    }
    access_control_allow_methods {
      items = ["GET", "HEAD"]
    }
    access_control_allow_origins {
      items = ["*"]
    }
  }
}

resource "aws_cloudfront_distribution" "place_live" {
  count = local.place_live ? 1 : 0

  enabled         = true
  comment         = "${local.name} live canvas image (origin: ${local.place_host}/api/v1/canvas.svg)"
  aliases         = [local.place_live_host]
  price_class     = "PriceClass_100"
  is_ipv6_enabled = true
  http_version    = "http2and3"

  # The place hostname itself, through the ALB's host rule: CloudFront sends
  # the origin's hostname as Host, and the ALB's certificate covers it.
  origin {
    origin_id   = "place"
    domain_name = local.place_host

    custom_origin_config {
      http_port                = 80
      https_port               = 443
      origin_protocol_policy   = "https-only"
      origin_ssl_protocols     = ["TLSv1.2"]
      origin_read_timeout      = 10
      origin_keepalive_timeout = 60
    }

    origin_shield {
      enabled              = true
      origin_shield_region = var.region
    }
  }

  default_cache_behavior {
    target_origin_id           = "place"
    viewer_protocol_policy     = "redirect-to-https"
    allowed_methods            = ["GET", "HEAD"]
    cached_methods             = ["GET", "HEAD"]
    cache_policy_id            = aws_cloudfront_cache_policy.place_live[0].id
    response_headers_policy_id = aws_cloudfront_response_headers_policy.place_live[0].id
    compress                   = true

    function_association {
      event_type   = "viewer-request"
      function_arn = aws_cloudfront_function.place_live[0].arn
    }
  }

  # An origin error is held for one bucket, so a failing or restarting place
  # task (a deploy takes it down for about a minute) still sees about one
  # request per 2 seconds.
  dynamic "custom_error_response" {
    for_each = [500, 502, 503, 504]

    content {
      error_code            = custom_error_response.value
      error_caching_min_ttl = 2
    }
  }

  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }

  viewer_certificate {
    acm_certificate_arn      = aws_acm_certificate_validation.place_live[0].certificate_arn
    ssl_support_method       = "sni-only"
    minimum_protocol_version = "TLSv1.2_2021"
  }
}

resource "aws_route53_record" "place_live" {
  for_each = local.place_live ? toset(["A", "AAAA"]) : toset([])

  zone_id = var.route53_zone_id
  name    = local.place_live_host
  type    = each.value

  alias {
    name                   = aws_cloudfront_distribution.place_live[0].domain_name
    zone_id                = aws_cloudfront_distribution.place_live[0].hosted_zone_id
    evaluate_target_health = false
  }
}
