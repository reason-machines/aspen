# `aspen serve` on Cloud Run, reachable only through the load balancer.

locals {
  # Plain settings (configuration contract, deploy/README.md).
  env = merge(
    {
      ASPEN_PUBLIC_URL            = local.url
      DATABASE_URL                = "postgres://aspen@${google_sql_database_instance.aspen.private_ip_address}:5432/aspen?sslmode=require"
      ASPEN_BLOB_URL              = "s3://${google_storage_bucket.packs.name}"
      AWS_ENDPOINT_URL            = "https://storage.googleapis.com"
      AWS_REGION                  = "auto"
      AWS_ACCESS_KEY_ID           = google_storage_hmac_key.aspen.access_id
      ASPEN_POLICY                = var.aspen_policy_json
      ASPEN_LIMITS                = var.aspen_limits_json
      ASPEN_TRUSTED_PROXY_HOPS    = "1"
      ASPEN_SPOOL_DIR             = "/var/lib/aspen/spool"
      ASPEN_MAX_PUSH_BYTES        = tostring(var.max_push_bytes)
      ASPEN_MAX_OBJECT_BYTES      = tostring(var.max_object_bytes)
      ASPEN_MAX_INFLATED_BYTES    = tostring(var.max_push_bytes * 2)
      ASPEN_PREFETCH_MEMORY_BYTES = tostring(var.prefetch_memory_bytes)
    },
    local.cdn ? {
      ASPEN_CDN_URL       = "${local.url}/-/express"
      ASPEN_CDN_PAST_END  = "416"
      ASPEN_EXPRESS_SERVE = "true"
    } : {},
  )

  # Secrets, from Secret Manager.
  secret_env = merge(
    {
      PGPASSWORD            = google_secret_manager_secret.db_password.secret_id
      AWS_SECRET_ACCESS_KEY = google_secret_manager_secret.hmac.secret_id
    },
    local.cdn ? { ASPEN_EXPRESS_KEYS = var.express_keys_secret_id } : {},
  )
}

resource "google_secret_manager_secret_iam_member" "aspen" {
  for_each = local.secret_env

  project   = var.project_id
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.aspen.member
}

resource "google_cloud_run_v2_service" "aspen" {
  project             = var.project_id
  name                = var.name
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"
  deletion_protection = var.deletion_protection
  labels              = local.labels
  # The load balancer sends no Google identity, so Cloud Run's default
  # invoker check would answer 403 to every Git request. Ingress above keeps
  # the service reachable only through the load balancer; Aspen's tokens
  # and policy decide access.
  invoker_iam_disabled = true

  template {
    service_account = google_service_account.aspen.email
    # Long clones and pushes: Cloud Run's maximum.
    timeout                          = "3600s"
    max_instance_request_concurrency = 40
    execution_environment            = "EXECUTION_ENVIRONMENT_GEN2"

    scaling {
      min_instance_count = var.min_instances
      max_instance_count = var.max_instances
    }

    vpc_access {
      egress = "PRIVATE_RANGES_ONLY"
      network_interfaces {
        network    = google_compute_network.aspen.id
        subnetwork = google_compute_subnetwork.run.id
      }
    }

    containers {
      image = var.container_image
      args  = ["serve"]

      # h2c: Cloud Run caps HTTP/1 request bodies at 32 MiB, and a push is
      # one request body. `aspen serve` speaks both on the same port.
      ports {
        name           = "h2c"
        container_port = 7070
      }

      resources {
        limits = {
          cpu    = var.cpu
          memory = var.memory
        }
        startup_cpu_boost = true
      }

      dynamic "env" {
        for_each = local.env

        content {
          name  = env.key
          value = env.value
        }
      }

      dynamic "env" {
        for_each = local.secret_env

        content {
          name = env.key
          value_source {
            secret_key_ref {
              secret  = env.value
              version = "latest"
            }
          }
        }
      }

      volume_mounts {
        name       = "spool"
        mount_path = "/var/lib/aspen/spool"
      }

      startup_probe {
        http_get {
          path = "/readyz"
          port = 7070
        }
        period_seconds    = 2
        failure_threshold = 60
      }

      liveness_probe {
        # Not the database: an outage must not restart every instance.
        http_get {
          path = "/healthz"
          port = 7070
        }
        period_seconds = 10
      }
    }

    # Cloud Run's only local storage is memory: the spool counts against the
    # instance's memory limit, which is why max_push_bytes is small here.
    volumes {
      name = "spool"
      empty_dir {
        medium     = "MEMORY"
        size_limit = "${var.spool_size_mib}Mi"
      }
    }
  }

  lifecycle {
    precondition {
      condition     = var.max_push_bytes * 2 <= var.spool_size_mib * 1048576
      error_message = "max_push_bytes must be at most half of the spool (spool_size_mib): a push needs about twice its size in the spool."
    }
  }

  depends_on = [
    google_project_service.this,
    google_secret_manager_secret_iam_member.aspen,
    google_secret_manager_secret_version.db_password,
    google_secret_manager_secret_version.hmac,
    google_sql_user.aspen,
  ]
}

# Operator commands (`aspen repo create`, `aspen migrate`, ...) run as a Cloud
# Run job in the same network with the same settings:
#
#   gcloud run jobs execute aspen-admin --region <region> --wait \
#     --args=repo,create,acme/app
#
# A job's output goes to Cloud Logging, so prefer OIDC grants
# (aspen_policy_json) over API tokens here; a token created this way is in
# that execution's log.
resource "google_cloud_run_v2_job" "admin" {
  project             = var.project_id
  name                = "${var.name}-admin"
  location            = var.region
  deletion_protection = false
  labels              = local.labels

  template {
    task_count = 1

    template {
      service_account = google_service_account.aspen.email
      max_retries     = 0
      timeout         = "600s"

      vpc_access {
        egress = "PRIVATE_RANGES_ONLY"
        network_interfaces {
          network    = google_compute_network.aspen.id
          subnetwork = google_compute_subnetwork.run.id
        }
      }

      containers {
        image = var.container_image
        args  = ["migrate"]

        env {
          name  = "DATABASE_URL"
          value = local.env.DATABASE_URL
        }

        env {
          name = "PGPASSWORD"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.db_password.secret_id
              version = "latest"
            }
          }
        }
      }
    }
  }

  depends_on = [google_secret_manager_secret_iam_member.aspen]
}
