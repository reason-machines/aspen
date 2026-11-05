# Example root module: Aspen in a dedicated Google Cloud project.
#
#   cp backend.hcl.example backend.hcl            # state bucket and prefix
#   cp terraform.tfvars.example terraform.tfvars  # project, domain, image, emails
#   terraform init -backend-config=backend.hcl
#   terraform plan
#
# See deploy/README.md, "Google Cloud (Terraform)".

terraform {
  required_version = ">= 1.11"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
  }

  # Partial configuration: bucket and prefix come from backend.hcl. Keep the
  # state bucket private: it holds the GCS HMAC secret.
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region

  # Budgets are created on the billing account, billed to this project.
  user_project_override = true
  billing_project       = var.project_id
}

module "aspen" {
  source = "../.."

  project_id             = var.project_id
  region                 = var.region
  domain_name            = var.domain_name
  dns_managed_zone       = var.dns_managed_zone
  container_image        = var.container_image
  express_keys_secret_id = var.express_keys_secret_id
  billing_account        = var.billing_account
  budget_alert_emails    = var.budget_alert_emails
}
