# Aspen on Google Cloud: Cloud Run (aspen serve), Cloud SQL for PostgreSQL
# on a private IP, a GCS pack bucket reached through its S3 interoperability
# API, and a global HTTPS load balancer whose /-/express/ path is cached by
# Cloud CDN. Deploy it into a dedicated project.

data "google_project" "this" {
  project_id = var.project_id
}

locals {
  labels = merge({ app = "aspen", managed-by = "terraform" }, var.labels)
  url    = "https://${var.domain_name}"
  cdn    = var.express_keys_secret_id != null

  services = [
    "billingbudgets.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "servicenetworking.googleapis.com",
    "sqladmin.googleapis.com",
    "storage.googleapis.com",
  ]
}

resource "google_project_service" "this" {
  for_each = toset(local.services)

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_service_account" "aspen" {
  project      = var.project_id
  account_id   = "${var.name}-server"
  display_name = "Aspen server (Cloud Run)"
}
