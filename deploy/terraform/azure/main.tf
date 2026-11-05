# Aspen on Azure: Container Apps (aspen serve) in a VNet, PostgreSQL Flexible
# Server on a private address, secrets in Key Vault, a budget alarm on the
# resource group. Packs go to an S3-compatible bucket (variables.tf).

data "azurerm_client_config" "current" {}

locals {
  tags       = merge({ app = "aspen", managed-by = "terraform" }, var.tags)
  public_url = var.domain_name == null ? "https://${azurerm_container_app.aspen.ingress[0].fqdn}" : "https://${var.domain_name}"
}

resource "azurerm_resource_group" "aspen" {
  name     = var.name
  location = var.location
  tags     = local.tags
}

resource "azurerm_log_analytics_workspace" "aspen" {
  name                = var.name
  resource_group_name = azurerm_resource_group.aspen.name
  location            = azurerm_resource_group.aspen.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.tags
}

resource "azurerm_user_assigned_identity" "aspen" {
  name                = "${var.name}-server"
  resource_group_name = azurerm_resource_group.aspen.name
  location            = azurerm_resource_group.aspen.location
  tags                = local.tags
}
