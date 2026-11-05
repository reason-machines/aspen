# Tests with mocked providers: no Azure credentials, no API calls.
#
#   cd deploy/terraform/azure
#   terraform init -backend=false
#   terraform test

mock_provider "azurerm" {
  mock_data "azurerm_client_config" {
    defaults = {
      tenant_id = "00000000-0000-0000-0000-000000000001"
      object_id = "00000000-0000-0000-0000-000000000002"
    }
  }
  mock_resource "azurerm_resource_group" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen" }
  }
  mock_resource "azurerm_subnet" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen/providers/Microsoft.Network/virtualNetworks/aspen/subnets/x" }
  }
  mock_resource "azurerm_virtual_network" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen/providers/Microsoft.Network/virtualNetworks/aspen" }
  }
  mock_resource "azurerm_private_dns_zone" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen/providers/Microsoft.Network/privateDnsZones/aspen.private.postgres.database.azure.com" }
  }
  mock_resource "azurerm_log_analytics_workspace" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen/providers/Microsoft.OperationalInsights/workspaces/aspen" }
  }
  mock_resource "azurerm_user_assigned_identity" {
    defaults = {
      id           = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen/providers/Microsoft.ManagedIdentity/userAssignedIdentities/aspen-server"
      principal_id = "00000000-0000-0000-0000-000000000003"
    }
  }
  mock_resource "azurerm_key_vault" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen/providers/Microsoft.KeyVault/vaults/aspen-12345678" }
  }
  mock_resource "azurerm_key_vault_secret" {
    defaults = { versionless_id = "https://aspen-12345678.vault.azure.net/secrets/x" }
  }
  mock_resource "azurerm_postgresql_flexible_server" {
    defaults = {
      id   = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen/providers/Microsoft.DBforPostgreSQL/flexibleServers/aspen-abc123"
      fqdn = "aspen-abc123.postgres.database.azure.com"
    }
  }
  mock_resource "azurerm_container_app_environment" {
    defaults = { id = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen/providers/Microsoft.App/managedEnvironments/aspen" }
  }
  mock_resource "azurerm_container_app" {
    defaults = {
      id                            = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/aspen/providers/Microsoft.App/containerApps/aspen"
      custom_domain_verification_id = "VERIFICATIONID"
    }
  }
}

mock_provider "random" {}

variables {
  container_image      = "ghcr.io/reason-machines/aspen:v1.2.3"
  s3_bucket            = "aspen-packs"
  s3_endpoint          = "https://account.r2.cloudflarestorage.com"
  s3_access_key_id     = "AKIDEXAMPLE"
  s3_secret_access_key = "test-only-secret"
  budget_alert_emails  = ["ops@example.test"]
}

run "defaults" {
  command = apply

  assert {
    condition     = azurerm_container_app.aspen.template[0].container[0].args == tolist(["serve"]) && azurerm_container_app.aspen.template[0].container[0].image == "ghcr.io/reason-machines/aspen:v1.2.3"
    error_message = "The app runs the pinned image as aspen serve."
  }
  assert {
    condition     = local.env.DATABASE_URL == "postgres://aspen@aspen-abc123.postgres.database.azure.com:5432/aspen?sslmode=require"
    error_message = "DATABASE_URL is the private server, TLS required, without a password."
  }
  assert {
    condition     = local.env.ASPEN_BLOB_URL == "s3://aspen-packs" && local.env.AWS_ENDPOINT_URL == "https://account.r2.cloudflarestorage.com"
    error_message = "Packs go to the S3-compatible bucket."
  }
  # Issue #125: read and push requests are rate limited by default.
  assert {
    condition     = jsondecode(local.env.ASPEN_LIMITS).read.per_second > 0 && jsondecode(local.env.ASPEN_LIMITS).push.per_second > 0
    error_message = "ASPEN_LIMITS limits read and push requests per caller by default."
  }
  assert {
    condition     = alltrue([for k, v in local.env : !can(regex("(?i)(password|secret)", k))])
    error_message = "No password or secret is a plain environment variable."
  }
  assert {
    condition     = toset([for s in azurerm_container_app.aspen.secret : s.name]) == toset(values(local.secret_env)) && alltrue([for s in azurerm_container_app.aspen.secret : s.key_vault_secret_id != null && s.value == null])
    error_message = "Every app secret is a Key Vault reference, never an inline value."
  }
  assert {
    condition     = azurerm_postgresql_flexible_server.aspen.administrator_password == null && azurerm_key_vault_secret.db_password.value == null && azurerm_key_vault_secret.s3_secret.value == null
    error_message = "Passwords and keys are write-only: never in state."
  }
  assert {
    condition     = !azurerm_postgresql_flexible_server.aspen.public_network_access_enabled && azurerm_postgresql_flexible_server.aspen.version == "17"
    error_message = "PostgreSQL 17 on a private address only."
  }
  assert {
    condition     = length(azurerm_management_lock.database) == 1
    error_message = "The database is locked against deletion by default."
  }
  assert {
    condition     = one(azurerm_container_app.aspen.template[0].volume).storage_type == "EmptyDir"
    error_message = "The push spool is the replica's ephemeral disk."
  }
  assert {
    condition     = azurerm_container_app.aspen.template[0].container[0].liveness_probe[0].path == "/healthz" && azurerm_container_app.aspen.template[0].container[0].readiness_probe[0].path == "/readyz"
    error_message = "Liveness is /healthz (not the database), readiness /readyz."
  }
  assert {
    condition     = azurerm_container_app.aspen.ingress[0].external_enabled && !azurerm_container_app.aspen.ingress[0].allow_insecure_connections && azurerm_container_app.aspen.ingress[0].target_port == 7070
    error_message = "HTTPS ingress to port 7070."
  }
  assert {
    condition     = azurerm_role_assignment.app_reads_secrets.role_definition_name == "Key Vault Secrets User" && azurerm_key_vault.aspen.rbac_authorization_enabled
    error_message = "The app's identity can only read secrets."
  }
  assert {
    condition     = azurerm_consumption_budget_resource_group.aspen.amount == 300 && length(azurerm_consumption_budget_resource_group.aspen.notification) == 3
    error_message = "A $300 monthly budget with three alerts."
  }
  assert {
    condition     = alltrue([for n in azurerm_consumption_budget_resource_group.aspen.notification : n.contact_emails == tolist(["ops@example.test"])])
    error_message = "Budget alerts go to budget_alert_emails."
  }
  assert {
    condition     = length(azurerm_container_app_custom_domain.aspen) == 0 && !contains(keys(local.env), "ASPEN_PUBLIC_URL")
    error_message = "No custom domain unless domain_name is set."
  }
}

run "custom_domain" {
  command = apply

  variables {
    domain_name             = "git.example.test"
    dns_zone_name           = "example.test"
    dns_zone_resource_group = "dns"
  }

  assert {
    condition     = local.env.ASPEN_PUBLIC_URL == "https://git.example.test" && output.url == "https://git.example.test"
    error_message = "The custom domain is Aspen's public URL."
  }
  assert {
    condition     = azurerm_dns_cname_record.aspen[0].name == "git" && azurerm_dns_txt_record.verification[0].name == "asuid.git"
    error_message = "CNAME and verification records are created in the zone."
  }
}

run "unpinned_image_is_refused" {
  command = plan

  variables {
    container_image = "ghcr.io/reason-machines/aspen:latest"
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
