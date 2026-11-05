# Key Vault with RBAC: the deployer writes secrets, the app's identity reads
# them. Secret values are written with write-only attributes.

resource "azurerm_key_vault" "aspen" {
  name                       = "${var.name}-${substr(md5(azurerm_resource_group.aspen.id), 0, 8)}"
  resource_group_name        = azurerm_resource_group.aspen.name
  location                   = azurerm_resource_group.aspen.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 7
  tags                       = local.tags
}

resource "azurerm_role_assignment" "deployer_writes_secrets" {
  scope                = azurerm_key_vault.aspen.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_role_assignment" "app_reads_secrets" {
  scope                = azurerm_key_vault.aspen.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.aspen.principal_id
}

resource "azurerm_key_vault_secret" "db_password" {
  name             = "db-password"
  key_vault_id     = azurerm_key_vault.aspen.id
  value_wo         = ephemeral.random_password.db.result
  value_wo_version = var.db_password_version

  depends_on = [azurerm_role_assignment.deployer_writes_secrets]
}

resource "azurerm_key_vault_secret" "s3_secret" {
  name             = "s3-secret-access-key"
  key_vault_id     = azurerm_key_vault.aspen.id
  value_wo         = var.s3_secret_access_key
  value_wo_version = var.s3_secret_version

  depends_on = [azurerm_role_assignment.deployer_writes_secrets]
}
