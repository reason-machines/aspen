# A global external HTTPS load balancer in front of Cloud Run. Git traffic
# (authenticated, uncacheable) goes to a backend without CDN; /-/express/
# (signed, immutable pack URLs, node C2) goes to the same service through a
# backend with Cloud CDN, which caches each pack once per signing window.

resource "google_compute_global_address" "lb" {
  project = var.project_id
  name    = "${var.name}-lb"

  depends_on = [google_project_service.this]
}

resource "google_compute_managed_ssl_certificate" "aspen" {
  project = var.project_id
  name    = "${var.name}-cert"

  managed {
    domains = [var.domain_name]
  }
}

resource "google_compute_region_network_endpoint_group" "run" {
  project               = var.project_id
  name                  = "${var.name}-run"
  region                = var.region
  network_endpoint_type = "SERVERLESS"

  cloud_run {
    service = google_cloud_run_v2_service.aspen.name
  }
}

resource "google_compute_backend_service" "git" {
  project               = var.project_id
  name                  = "${var.name}-git"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  protocol              = "HTTPS"
  enable_cdn            = false

  backend {
    group = google_compute_region_network_endpoint_group.run.id
  }

  log_config {
    enable      = true
    sample_rate = 1
  }
}

resource "google_compute_backend_service" "express" {
  count = local.cdn ? 1 : 0

  project               = var.project_id
  name                  = "${var.name}-express"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  protocol              = "HTTPS"
  enable_cdn            = true

  backend {
    group = google_compute_region_network_endpoint_group.run.id
  }

  cdn_policy {
    # Express answers with Cache-Control max-age up to the URL's expiry and
    # no-store on refusals; follow it.
    cache_mode = "USE_ORIGIN_HEADERS"
    # Never serve a stale pack when the origin fails (C2 obligation, #45).
    serve_while_stale = 0
    cache_key_policy {
      include_host         = true
      include_protocol     = true
      include_query_string = false
    }
  }

  log_config {
    enable      = true
    sample_rate = 1
  }
}

resource "google_compute_url_map" "https" {
  project         = var.project_id
  name            = "${var.name}-https"
  default_service = google_compute_backend_service.git.id

  dynamic "host_rule" {
    for_each = local.cdn ? [1] : []

    content {
      hosts        = [var.domain_name]
      path_matcher = "aspen"
    }
  }

  dynamic "path_matcher" {
    for_each = local.cdn ? [1] : []

    content {
      name            = "aspen"
      default_service = google_compute_backend_service.git.id

      path_rule {
        paths   = ["/-/express/*"]
        service = google_compute_backend_service.express[0].id
      }
    }
  }
}

resource "google_compute_ssl_policy" "modern" {
  project         = var.project_id
  name            = "${var.name}-tls"
  profile         = "MODERN"
  min_tls_version = "TLS_1_2"
}

resource "google_compute_target_https_proxy" "aspen" {
  project          = var.project_id
  name             = "${var.name}-https"
  url_map          = google_compute_url_map.https.id
  ssl_certificates = [google_compute_managed_ssl_certificate.aspen.id]
  ssl_policy       = google_compute_ssl_policy.modern.id
}

resource "google_compute_global_forwarding_rule" "https" {
  project               = var.project_id
  name                  = "${var.name}-https"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  ip_address            = google_compute_global_address.lb.address
  port_range            = "443"
  target                = google_compute_target_https_proxy.aspen.id
}

# Plain HTTP only redirects to HTTPS.
resource "google_compute_url_map" "redirect" {
  project = var.project_id
  name    = "${var.name}-redirect"

  default_url_redirect {
    https_redirect         = true
    strip_query            = false
    redirect_response_code = "MOVED_PERMANENTLY_DEFAULT"
  }
}

resource "google_compute_target_http_proxy" "redirect" {
  project = var.project_id
  name    = "${var.name}-redirect"
  url_map = google_compute_url_map.redirect.id
}

resource "google_compute_global_forwarding_rule" "http" {
  project               = var.project_id
  name                  = "${var.name}-http"
  load_balancing_scheme = "EXTERNAL_MANAGED"
  ip_address            = google_compute_global_address.lb.address
  port_range            = "80"
  target                = google_compute_target_http_proxy.redirect.id
}

resource "google_dns_record_set" "a" {
  count = var.dns_managed_zone == null ? 0 : 1

  project      = var.project_id
  managed_zone = var.dns_managed_zone
  name         = "${var.domain_name}."
  type         = "A"
  ttl          = 300
  rrdatas      = [google_compute_global_address.lb.address]
}
