# PostgreSQL 17 Flexible Server on a private address (TLS is required by
# default). The admin password is generated ephemerally and written with
# write-only attributes to the server and to Key Vault: never in state.

ephemeral "random_password" "db" {
  length  = 40
  special = false
}

resource "azurerm_postgresql_flexible_server" "aspen" {
  name                              = "${var.name}-${substr(md5(azurerm_resource_group.aspen.id), 0, 6)}"
  resource_group_name               = azurerm_resource_group.aspen.name
  location                          = azurerm_resource_group.aspen.location
  version                           = "17"
  sku_name                          = var.db_sku
  storage_mb                        = var.db_storage_mb
  auto_grow_enabled                 = true
  backup_retention_days             = 7
  delegated_subnet_id               = azurerm_subnet.database.id
  private_dns_zone_id               = azurerm_private_dns_zone.postgres.id
  public_network_access_enabled     = false
  administrator_login               = "aspen"
  administrator_password_wo         = ephemeral.random_password.db.result
  administrator_password_wo_version = var.db_password_version
  tags                              = local.tags

  lifecycle {
    # Azure picks the zone; do not fight it on later plans.
    ignore_changes = [zone]
  }

  depends_on = [azurerm_private_dns_zone_virtual_network_link.postgres]
}

resource "azurerm_postgresql_flexible_server_database" "aspen" {
  name      = "aspen"
  server_id = azurerm_postgresql_flexible_server.aspen.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

resource "azurerm_management_lock" "database" {
  count = var.deletion_protection ? 1 : 0

  name       = "${var.name}-database"
  scope      = azurerm_postgresql_flexible_server.aspen.id
  lock_level = "CanNotDelete"
  notes      = "Aspen's refs and history. Set deletion_protection = false to remove."
}
