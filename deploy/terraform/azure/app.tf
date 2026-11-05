# `aspen serve` on Container Apps. Operator commands run inside a replica:
#
#   az containerapp exec -g <name> -n <name> --command "aspen repo create acme/app"

locals {
  env = merge(
    {
      ASPEN_LISTEN             = "0.0.0.0:7070"
      DATABASE_URL             = "postgres://aspen@${azurerm_postgresql_flexible_server.aspen.fqdn}:5432/aspen?sslmode=require"
      ASPEN_BLOB_URL           = "s3://${var.s3_bucket}"
      AWS_REGION               = var.s3_region
      AWS_ACCESS_KEY_ID        = var.s3_access_key_id
      ASPEN_POLICY             = var.aspen_policy_json
      ASPEN_LIMITS             = var.aspen_limits_json
      ASPEN_TRUSTED_PROXY_HOPS = "1"
      ASPEN_SPOOL_DIR          = "/var/lib/aspen/spool"
      ASPEN_MAX_PUSH_BYTES     = tostring(var.max_push_bytes)
    },
    var.s3_endpoint == null ? {} : { AWS_ENDPOINT_URL = var.s3_endpoint },
    var.domain_name == null ? {} : { ASPEN_PUBLIC_URL = "https://${var.domain_name}" },
  )

  secret_env = {
    PGPASSWORD            = "db-password"
    AWS_SECRET_ACCESS_KEY = "s3-secret-access-key"
  }
}

resource "azurerm_container_app_environment" "aspen" {
  name                       = var.name
  resource_group_name        = azurerm_resource_group.aspen.name
  location                   = azurerm_resource_group.aspen.location
  log_analytics_workspace_id = azurerm_log_analytics_workspace.aspen.id
  infrastructure_subnet_id   = azurerm_subnet.apps.id
  tags                       = local.tags

  workload_profile {
    name                  = "Consumption"
    workload_profile_type = "Consumption"
  }
}

resource "azurerm_container_app" "aspen" {
  name                         = var.name
  resource_group_name          = azurerm_resource_group.aspen.name
  container_app_environment_id = azurerm_container_app_environment.aspen.id
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  tags                         = local.tags

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.aspen.id]
  }

  secret {
    name                = "db-password"
    key_vault_secret_id = azurerm_key_vault_secret.db_password.versionless_id
    identity            = azurerm_user_assigned_identity.aspen.id
  }

  secret {
    name                = "s3-secret-access-key"
    key_vault_secret_id = azurerm_key_vault_secret.s3_secret.versionless_id
    identity            = azurerm_user_assigned_identity.aspen.id
  }

  ingress {
    external_enabled           = true
    target_port                = 7070
    transport                  = "auto"
    allow_insecure_connections = false

    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }

  template {
    min_replicas                     = var.min_replicas
    max_replicas                     = var.max_replicas
    termination_grace_period_seconds = 300

    container {
      name   = "aspen"
      image  = var.container_image
      args   = ["serve"]
      cpu    = var.cpu
      memory = var.memory

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
          name        = env.key
          secret_name = env.value
        }
      }

      volume_mounts {
        name = "spool"
        path = "/var/lib/aspen/spool"
      }

      startup_probe {
        transport               = "HTTP"
        port                    = 7070
        path                    = "/readyz"
        interval_seconds        = 2
        failure_count_threshold = 30
      }

      readiness_probe {
        transport = "HTTP"
        port      = 7070
        path      = "/readyz"
      }

      liveness_probe {
        # Not the database: an outage must not restart every replica.
        transport = "HTTP"
        port      = 7070
        path      = "/healthz"
      }
    }

    # The replica's ephemeral disk, not memory.
    volume {
      name         = "spool"
      storage_type = "EmptyDir"
    }
  }

  depends_on = [azurerm_role_assignment.app_reads_secrets]
}

resource "azurerm_dns_txt_record" "verification" {
  count = var.domain_name != null && var.dns_zone_name != null ? 1 : 0

  name                = "asuid.${trimsuffix(var.domain_name, ".${var.dns_zone_name}")}"
  zone_name           = var.dns_zone_name
  resource_group_name = var.dns_zone_resource_group
  ttl                 = 300
  tags                = local.tags

  record {
    value = azurerm_container_app.aspen.custom_domain_verification_id
  }
}

resource "azurerm_dns_cname_record" "aspen" {
  count = var.domain_name != null && var.dns_zone_name != null ? 1 : 0

  name                = trimsuffix(var.domain_name, ".${var.dns_zone_name}")
  zone_name           = var.dns_zone_name
  resource_group_name = var.dns_zone_resource_group
  ttl                 = 300
  record              = azurerm_container_app.aspen.ingress[0].fqdn
  tags                = local.tags
}

# The host name on the app. Bind its free managed certificate once DNS
# resolves: az containerapp hostname bind -g <name> -n <name> \
#   --hostname <domain_name> --environment <name> --validation-method CNAME
resource "azurerm_container_app_custom_domain" "aspen" {
  count = var.domain_name == null ? 0 : 1

  name             = var.domain_name
  container_app_id = azurerm_container_app.aspen.id

  lifecycle {
    # The managed certificate is bound outside Terraform (above).
    ignore_changes = [certificate_binding_type, container_app_environment_certificate_id]
  }

  depends_on = [azurerm_dns_txt_record.verification, azurerm_dns_cname_record.aspen]
}
