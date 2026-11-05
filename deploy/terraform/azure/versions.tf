terraform {
  # 1.11+ for write-only attributes, ephemeral resources and ephemeral input
  # variables: no password or secret key enters state or a saved plan.
  required_version = ">= 1.11"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 4.40, < 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.7, < 4.0"
    }
  }
}
