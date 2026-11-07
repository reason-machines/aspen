# One environment of Aspen's public demo (docs/PLAN.md section 6): staging
# first, then the public demo, from this same root with different variable
# and backend files (staging.tfvars, demo.tfvars; see ../README.md).
#
# - Aspen itself, through this repository's own AWS module (../..), at
#   git.<base_domain>, with CloudFront at cdn.git.<base_domain>;
# - the place demo (demo/place) at place.<base_domain> (or place_hostname,
#   for example base_domain itself): one aspen-place
#   task serving the page, its API and canvas.git (place.tf), on the same
#   cluster, load balancer and database instance as Aspen.
#
# It runs in the account the account stack (../account) set up: that stack
# owns the budget alarm, the DNS zone and the image repositories.

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

  # Partial configuration: `terraform init -backend-config=<env>.backend.hcl`.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  # Refuse every other account, so a stray profile can never point this at
  # Reason's production account.
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
  name       = "aspen-${var.environment}"
  git_host   = "git.${var.base_domain}"
  place_host = coalesce(var.place_hostname, "place.${var.base_domain}")

  # How large the place task and the database are, per environment. A
  # variable set to anything but null overrides its row.
  #
  # The demo is sized for a public share where people push it to its limit
  # from their own agents: about 500 pushes/s in total (docs/PLAN.md decisions
  # log, 2026-11-06, "public-share sizing"). The throughput bench reached 567
  # pushes/s with 8 cores for aspen-place and a 4-core PostgreSQL at about
  # 85%, so the demo gets about twice both: aspen-place runs as one task
  # (it keeps the canvas, merge queue and event feed in memory and never
  # runs twice, place.tf), at Fargate's largest size, 16 vCPU, with 64 GiB;
  # the database is an 8-vCPU Graviton instance with 400 GiB of gp3, the
  # smallest gp3 size RDS gives 12,000 IOPS and 500 MiB/s (below 400 GiB it
  # is 3,000 IOPS whatever the size). Staging keeps the small sizes.
  sizing = {
    staging = {
      place_cpu                = 512
      place_memory             = 1024
      place_db_max_connections = 10
      db_instance_class        = "db.t4g.micro"
      db_allocated_storage     = 20
      db_max_allocated_storage = 100
    }
    demo = {
      place_cpu                = 16384
      place_memory             = 65536
      place_db_max_connections = 64
      db_instance_class        = "db.m7g.2xlarge"
      db_allocated_storage     = 400
      db_max_allocated_storage = 1000
    }
  }

  # Aspen's task count, here so place.tf's quota check sums the same number
  # the module runs.
  aspen_desired_count = 1

  place_cpu                = coalesce(var.place_cpu, local.sizing[var.environment].place_cpu)
  place_memory             = coalesce(var.place_memory, local.sizing[var.environment].place_memory)
  place_db_max_connections = coalesce(var.place_db_max_connections, local.sizing[var.environment].place_db_max_connections)
  db_instance_class        = coalesce(var.db_instance_class, local.sizing[var.environment].db_instance_class)
  db_allocated_storage     = coalesce(var.db_allocated_storage, local.sizing[var.environment].db_allocated_storage)
  db_max_allocated_storage = coalesce(var.db_max_allocated_storage, local.sizing[var.environment].db_max_allocated_storage)

  tags = {
    Project     = "aspen"
    Stack       = "public-demo"
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  # Limits for a server strangers can reach (deploy/README.md, configuration
  # contract). Every request needs a token (the default policy); these bound
  # what one token, or one address, can cost. The task has 2 GiB: one push
  # at a time holds delta bases (1 GiB, aspen-pack's per-push cap), and
  # pushes are at most 512 MiB, so the 20 GiB spool holds many at once.
  aspen_environment = merge({
    ASPEN_MAX_PUSH_BYTES        = tostring(512 * 1024 * 1024)
    ASPEN_MAX_INFLATED_BYTES    = tostring(2 * 1024 * 1024 * 1024)
    ASPEN_MAX_OBJECT_BYTES      = tostring(100 * 1024 * 1024)
    ASPEN_PREFETCH_MEMORY_BYTES = tostring(1024 * 1024 * 1024)
    ASPEN_LIMITS = jsonencode({
      read  = { burst = 60, per_second = 10 }
      push  = { burst = 20, per_second = 2 }
      quota = { max_pack_bytes = 512 * 1024 * 1024, bytes_per_hour = 2 * 1024 * 1024 * 1024 }
    })
  }, var.aspen_environment)
}

# The account stack's budgets are the only thing watching spend (the module's
# own budgets are off below). Refuse to plan an environment in an account
# without the monthly one, or with one above what the account stack allows
# (../account-budget).
module "account_budget" {
  source = "../account-budget"

  name = var.account_budget_name
}

module "aspen" {
  source = "../.."

  providers = {
    aws           = aws
    aws.us_east_1 = aws.us_east_1
  }

  name            = local.name
  domain_name     = local.git_host
  route53_zone_id = var.route53_zone_id

  container_image       = var.aspen_image
  ecr_repository_arns   = var.ecr_repository_arns
  container_cpu         = var.aspen_cpu
  desired_count         = local.aspen_desired_count
  container_memory      = var.aspen_memory
  container_environment = local.aspen_environment
  fargate_spot          = var.fargate_spot

  enable_ssh           = var.enable_ssh
  enable_cdn           = var.enable_cdn
  http_ingress_cidrs   = var.http_ingress_cidrs
  express_keys_version = var.express_keys_version
  ssh_host_key_version = var.ssh_host_key_version

  db_instance_class        = local.db_instance_class
  db_allocated_storage     = local.db_allocated_storage
  db_max_allocated_storage = local.db_max_allocated_storage
  # Changes here are made on purpose, by an operator who is applying now: a
  # resize waiting days for the maintenance window would leave the demo
  # undersized while the plan reads as applied. A class change restarts the
  # single-AZ database (a few minutes without it for both services).
  db_apply_immediately   = true
  db_deletion_protection = var.db_deletion_protection
  db_password_version    = var.db_password_version
  bucket_force_destroy   = var.bucket_force_destroy

  # The account stack's budget covers every environment in the account.
  budget_enabled = false
}
