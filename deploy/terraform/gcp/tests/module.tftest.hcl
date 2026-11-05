# Tests with mocked providers: no Google credentials, no API calls.
#
#   cd deploy/terraform/gcp
#   terraform init -backend=false
#   terraform test

mock_provider "google" {
  mock_data "google_project" {
    defaults = { number = "123456789012" }
  }
  mock_resource "google_sql_database_instance" {
    defaults = { private_ip_address = "10.99.0.3", connection_name = "p:us-central1:aspen" }
  }
  mock_resource "google_compute_network" {
    defaults = { id = "projects/aspen-test/global/networks/aspen" }
  }
  mock_resource "google_compute_subnetwork" {
    defaults = { id = "projects/aspen-test/regions/us-central1/subnetworks/aspen-run" }
  }
  mock_resource "google_compute_global_address" {
    defaults = { address = "203.0.113.20" }
  }
  mock_resource "google_storage_hmac_key" {
    defaults = { access_id = "GOOG1EEXAMPLEACCESSID", secret = "mock-hmac-secret" }
  }
  mock_resource "google_service_account" {
    defaults = { email = "aspen-server@p.iam.gserviceaccount.com", member = "serviceAccount:aspen-server@p.iam.gserviceaccount.com" }
  }
}

mock_provider "random" {}

variables {
  project_id          = "aspen-test"
  domain_name         = "git.example.test"
  container_image     = "us-docker.pkg.dev/aspen-test/ghcr/reason-machines/aspen:v1.2.3"
  billing_account     = "000000-000000-000000"
  budget_alert_emails = ["ops@example.test"]
}

run "defaults" {
  command = apply

  # Cloud Run: aspen serve on h2c, reachable only through the load balancer.
  assert {
    condition     = google_cloud_run_v2_service.aspen.ingress == "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"
    error_message = "The Cloud Run service is reachable only through the load balancer."
  }
  assert {
    condition     = google_cloud_run_v2_service.aspen.invoker_iam_disabled == true
    error_message = "Cloud Run's invoker IAM check is off: the load balancer sends no identity, so it would refuse every Git request with 403."
  }
  assert {
    condition     = google_cloud_run_v2_service.aspen.template[0].containers[0].args == tolist(["serve"])
    error_message = "The service runs aspen serve."
  }
  assert {
    condition     = google_cloud_run_v2_service.aspen.template[0].containers[0].ports[0].name == "h2c"
    error_message = "The port is h2c, so pushes are not capped at Cloud Run's 32 MiB HTTP/1 body limit."
  }
  assert {
    condition     = google_cloud_run_v2_service.aspen.template[0].containers[0].liveness_probe[0].http_get[0].path == "/healthz" && google_cloud_run_v2_service.aspen.template[0].containers[0].startup_probe[0].http_get[0].path == "/readyz"
    error_message = "Liveness is /healthz (not the database); startup waits for /readyz."
  }

  # Settings and secrets.
  # Issue #125: read and push requests are rate limited by default.
  assert {
    condition     = jsondecode(local.env.ASPEN_LIMITS).read.per_second > 0 && jsondecode(local.env.ASPEN_LIMITS).push.per_second > 0
    error_message = "ASPEN_LIMITS limits read and push requests per caller by default."
  }
  assert {
    condition     = local.env.DATABASE_URL == "postgres://aspen@10.99.0.3:5432/aspen?sslmode=require"
    error_message = "DATABASE_URL is the private IP, TLS required, without a password."
  }
  assert {
    condition     = local.env.ASPEN_BLOB_URL == "s3://aspen-test-aspen-packs" && local.env.AWS_ENDPOINT_URL == "https://storage.googleapis.com"
    error_message = "Packs go to the GCS bucket through its S3 interoperability endpoint."
  }
  assert {
    condition     = local.env.ASPEN_TRUSTED_PROXY_HOPS == "1" && local.env.ASPEN_PUBLIC_URL == "https://git.example.test"
    error_message = "Aspen trusts the load balancer as one proxy and knows its URL."
  }
  assert {
    condition     = alltrue([for k, v in local.env : !can(regex("(?i)(password|secret)", k))])
    error_message = "No password or secret is a plain environment variable."
  }
  assert {
    condition     = toset(keys(local.secret_env)) == toset(["AWS_SECRET_ACCESS_KEY", "PGPASSWORD"])
    error_message = "PGPASSWORD and the HMAC secret come from Secret Manager; no express keys without the CDN."
  }
  assert {
    condition     = google_sql_user.aspen.password == null && google_sql_user.aspen.password_wo_version == 1
    error_message = "The database password is write-only: it never enters state."
  }

  # The spool is memory on Cloud Run, sized for the push limit.
  assert {
    condition     = google_cloud_run_v2_service.aspen.template[0].volumes[0].empty_dir[0].size_limit == "2048Mi" && local.env.ASPEN_MAX_PUSH_BYTES == "1073741824"
    error_message = "A 2 GiB spool holds a 1 GiB push."
  }

  # Database and bucket.
  assert {
    condition     = !google_sql_database_instance.aspen.settings[0].ip_configuration[0].ipv4_enabled && google_sql_database_instance.aspen.settings[0].ip_configuration[0].ssl_mode == "ENCRYPTED_ONLY"
    error_message = "Cloud SQL has no public IP and requires TLS."
  }
  assert {
    condition     = google_sql_database_instance.aspen.deletion_protection && google_cloud_run_v2_service.aspen.deletion_protection && !google_storage_bucket.packs.force_destroy
    error_message = "Database, service and packs are protected by default."
  }
  assert {
    condition     = google_storage_bucket.packs.public_access_prevention == "enforced" && google_storage_bucket.packs.uniform_bucket_level_access
    error_message = "The pack bucket is private."
  }
  assert {
    condition     = anytrue([for r in google_storage_bucket.packs.lifecycle_rule : one(r.action).type == "AbortIncompleteMultipartUpload"])
    error_message = "Unfinished multipart uploads are cleaned up."
  }
  assert {
    condition     = google_storage_bucket_iam_member.aspen.role == "roles/storage.objectUser"
    error_message = "Aspen may use objects in the bucket, not administer it."
  }

  # No CDN without express keys.
  assert {
    condition     = length(google_compute_backend_service.express) == 0 && length(google_compute_url_map.https.path_matcher) == 0
    error_message = "No CDN backend without express_keys_secret_id."
  }
  assert {
    condition     = !google_compute_backend_service.git.enable_cdn
    error_message = "Git traffic is never cached."
  }

  # Budget.
  assert {
    condition     = google_billing_budget.aspen.amount[0].specified_amount[0].units == "300" && length(google_billing_budget.aspen.threshold_rules) == 3
    error_message = "A $300 monthly budget with three alert thresholds."
  }
  assert {
    condition     = toset(google_billing_budget.aspen.budget_filter[0].projects) == toset(["projects/123456789012"])
    error_message = "The budget covers this project."
  }
  assert {
    condition     = length(google_billing_budget.aspen.all_updates_rule[0].monitoring_notification_channels) == 1
    error_message = "Budget alerts go to the email channels."
  }
  assert {
    condition     = length(google_dns_record_set.a) == 0
    error_message = "No DNS record without dns_managed_zone."
  }
}

run "cdn_and_dns" {
  command = apply

  variables {
    express_keys_secret_id = "aspen-express-keys"
    dns_managed_zone       = "example-test"
  }

  assert {
    condition     = google_compute_backend_service.express[0].enable_cdn && google_compute_backend_service.express[0].cdn_policy[0].serve_while_stale == 0
    error_message = "The express backend is cached, and never serves a stale pack."
  }
  assert {
    condition     = google_compute_url_map.https.path_matcher[0].path_rule[0].paths == toset(["/-/express/*"])
    error_message = "Only /-/express/ goes through the CDN."
  }
  assert {
    condition     = local.env.ASPEN_CDN_URL == "https://git.example.test/-/express" && local.env.ASPEN_EXPRESS_SERVE == "true" && local.env.ASPEN_CDN_PAST_END == "416"
    error_message = "Aspen signs CDN URIs (416 tier, git 2.56+) and serves the express origin itself."
  }
  assert {
    condition     = local.secret_env.ASPEN_EXPRESS_KEYS == "aspen-express-keys"
    error_message = "Express keys come from the named secret."
  }
  assert {
    condition     = google_dns_record_set.a[0].rrdatas == tolist(["203.0.113.20"]) && google_dns_record_set.a[0].name == "git.example.test."
    error_message = "The A record points the domain at the load balancer."
  }
}

run "push_limit_must_fit_the_spool" {
  command = plan

  variables {
    max_push_bytes = 2147483648
  }

  expect_failures = [google_cloud_run_v2_service.aspen]
}

run "ghcr_images_are_refused" {
  command = plan

  variables {
    container_image = "ghcr.io/reason-machines/aspen:v1.2.3"
  }

  expect_failures = [var.container_image]
}

run "a_budget_needs_a_recipient" {
  command = plan

  variables {
    budget_alert_emails = []
  }

  expect_failures = [var.budget_alert_emails]
}
