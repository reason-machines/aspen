# The pack bucket. Aspen talks to it through GCS's S3-compatible XML API with
# an HMAC key of its own service account, limited to objects in this bucket.

resource "google_storage_bucket" "packs" {
  project                     = var.project_id
  name                        = "${var.project_id}-${var.name}-packs"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = !var.deletion_protection
  labels                      = local.labels

  versioning {
    enabled = true
  }

  # A crashed push leaves an unfinished multipart upload; drop it after a day.
  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

  # Packs are write-once; old versions exist only after a duplicate upload.
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 30
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.this]
}

# Read, write, list and delete objects; no bucket administration.
resource "google_storage_bucket_iam_member" "aspen" {
  bucket = google_storage_bucket.packs.name
  role   = "roles/storage.objectUser"
  member = google_service_account.aspen.member
}

# GCS returns an HMAC secret only when the key is created, so Terraform keeps
# it in state: store state in a private, access-controlled bucket.
resource "google_storage_hmac_key" "aspen" {
  project               = var.project_id
  service_account_email = google_service_account.aspen.email
}

resource "google_secret_manager_secret" "hmac" {
  project   = var.project_id
  secret_id = "${var.name}-gcs-hmac-secret"
  labels    = local.labels

  replication {
    auto {}
  }

  depends_on = [google_project_service.this]
}

resource "google_secret_manager_secret_version" "hmac" {
  secret                 = google_secret_manager_secret.hmac.id
  secret_data_wo         = google_storage_hmac_key.aspen.secret
  secret_data_wo_version = 1
}
