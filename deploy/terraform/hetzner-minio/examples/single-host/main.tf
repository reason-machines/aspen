# Example root module: Aspen on one Hetzner Cloud server.
#
#   export HCLOUD_TOKEN=...                       # a token for a dedicated project
#   cp terraform.tfvars.example terraform.tfvars  # edit: domain, email, keys
#   terraform init
#   terraform plan
#
# See deploy/README.md, "Hetzner (Terraform)".

terraform {
  required_version = ">= 1.11"

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.52"
    }
  }
}

provider "hcloud" {}

module "aspen" {
  source = "../.."

  domain_name           = var.domain_name
  acme_email            = var.acme_email
  aspen_image           = var.aspen_image
  admin_ssh_public_keys = var.admin_ssh_public_keys
  admin_cidrs           = var.admin_cidrs
  dns_zone              = var.dns_zone
  location              = var.location
  server_type           = var.server_type
}
