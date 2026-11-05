# Cloud SQL for PostgreSQL 17 on a private IP, TLS required. The password is
# generated ephemerally and written with write-only attributes, so it never
# enters Terraform state; Cloud Run reads it from Secret Manager as PGPASSWORD.

ephemeral "random_password" "db" {
  length  = 40
  special = false
}

resource "google_sql_database_instance" "aspen" {
  project             = var.project_id
  name                = var.name
  region              = var.region
  database_version    = "POSTGRES_17"
  deletion_protection = var.deletion_protection

  settings {
    tier              = var.db_tier
    edition           = "ENTERPRISE"
    availability_type = var.db_high_availability ? "REGIONAL" : "ZONAL"
    disk_type         = "PD_SSD"
    disk_size         = var.db_disk_size_gb
    disk_autoresize   = true
    user_labels       = local.labels

    deletion_protection_enabled = var.deletion_protection

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.aspen.id
      ssl_mode        = "ENCRYPTED_ONLY"
    }

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      start_time                     = "03:00"
    }
  }

  depends_on = [google_service_networking_connection.private_services]
}

resource "google_sql_database" "aspen" {
  project  = var.project_id
  name     = "aspen"
  instance = google_sql_database_instance.aspen.name
}

resource "google_sql_user" "aspen" {
  project             = var.project_id
  name                = "aspen"
  instance            = google_sql_database_instance.aspen.name
  password_wo         = ephemeral.random_password.db.result
  password_wo_version = var.db_password_version
}

resource "google_secret_manager_secret" "db_password" {
  project   = var.project_id
  secret_id = "${var.name}-db-password"
  labels    = local.labels

  replication {
    auto {}
  }

  depends_on = [google_project_service.this]
}

resource "google_secret_manager_secret_version" "db_password" {
  secret                 = google_secret_manager_secret.db_password.id
  secret_data_wo         = ephemeral.random_password.db.result
  secret_data_wo_version = var.db_password_version
}
