# Example root module: one Aspen deployment in a dedicated AWS account.
#
#   cp backend.hcl.example backend.hcl            # edit: state bucket, key, region
#   cp terraform.tfvars.example terraform.tfvars  # edit: account, domain, emails
#   terraform init -backend-config=backend.hcl
#   terraform plan
#
# See deploy/README.md for the full walkthrough.

terraform {
  # 1.11+ for write-only attributes; S3 native state locking needs 1.10+.
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.71"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.7"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.1"
    }
  }

  # Partial configuration: bucket, key, region and locking come from
  # `-backend-config=backend.hcl`, so no account details are committed.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  # Refuse to run against any other account, so a stray profile can never
  # point this at a production account.
  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = local.tags
  }
}

# CloudFront certificates must live in us-east-1.
provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"

  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = local.tags
  }
}

locals {
  tags = merge({
    Project   = "aspen"
    ManagedBy = "terraform"
    Name      = var.name
  }, var.extra_tags)
}

module "aspen" {
  source = "../.."

  providers = {
    aws           = aws
    aws.us_east_1 = aws.us_east_1
  }

  name            = var.name
  domain_name     = var.domain_name
  route53_zone_id = var.route53_zone_id

  container_image     = var.container_image
  ecr_repository_arns = var.ecr_repository_arns

  nat_gateway  = var.nat_gateway
  enable_ssh   = var.enable_ssh
  enable_cdn   = var.enable_cdn
  fargate_spot = var.fargate_spot

  compactor_count = var.compactor_count

  db_deletion_protection = var.db_deletion_protection
  bucket_force_destroy   = var.bucket_force_destroy

  budget_limit_usd     = var.budget_limit_usd
  budget_alert_emails  = var.budget_alert_emails
  budget_sns_topic_arn = var.budget_sns_topic_arn
}
