# Example root module: Aspen in a dedicated Azure subscription.
#
#   cp backend.hcl.example backend.hcl            # state storage account
#   cp terraform.tfvars.example terraform.tfvars  # image, bucket, emails
#   export TF_VAR_s3_secret_access_key=...        # from your secret store, every run
#   terraform init -backend-config=backend.hcl
#   terraform plan
#
# See deploy/README.md, "Azure (Terraform)".

terraform {
  required_version = ">= 1.11"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.40"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }

  backend "azurerm" {}
}

provider "azurerm" {
  # Refuse to run against any other subscription.
  subscription_id = var.subscription_id
  features {}
}

module "aspen" {
  source = "../.."

  location             = var.location
  container_image      = var.container_image
  domain_name          = var.domain_name
  s3_bucket            = var.s3_bucket
  s3_endpoint          = var.s3_endpoint
  s3_region            = var.s3_region
  s3_access_key_id     = var.s3_access_key_id
  s3_secret_access_key = var.s3_secret_access_key
  budget_alert_emails  = var.budget_alert_emails
}
