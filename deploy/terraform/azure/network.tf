resource "azurerm_virtual_network" "aspen" {
  name                = var.name
  resource_group_name = azurerm_resource_group.aspen.name
  location            = azurerm_resource_group.aspen.location
  address_space       = ["10.43.0.0/16"]
  tags                = local.tags
}

resource "azurerm_subnet" "apps" {
  name                 = "apps"
  resource_group_name  = azurerm_resource_group.aspen.name
  virtual_network_name = azurerm_virtual_network.aspen.name
  address_prefixes     = ["10.43.0.0/23"]

  delegation {
    name = "container-apps"
    service_delegation {
      name    = "Microsoft.App/environments"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_subnet" "database" {
  name                 = "database"
  resource_group_name  = azurerm_resource_group.aspen.name
  virtual_network_name = azurerm_virtual_network.aspen.name
  address_prefixes     = ["10.43.2.0/24"]

  delegation {
    name = "postgres"
    service_delegation {
      name    = "Microsoft.DBforPostgreSQL/flexibleServers"
      actions = ["Microsoft.Network/virtualNetworks/subnets/join/action"]
    }
  }
}

resource "azurerm_private_dns_zone" "postgres" {
  name                = "${var.name}.private.postgres.database.azure.com"
  resource_group_name = azurerm_resource_group.aspen.name
  tags                = local.tags
}

resource "azurerm_private_dns_zone_virtual_network_link" "postgres" {
  name                  = "${var.name}-postgres"
  resource_group_name   = azurerm_resource_group.aspen.name
  private_dns_zone_name = azurerm_private_dns_zone.postgres.name
  virtual_network_id    = azurerm_virtual_network.aspen.id
  tags                  = local.tags
}
