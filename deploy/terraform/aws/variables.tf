# ---------------------------------------------------------------------------
# Naming and DNS
# ---------------------------------------------------------------------------

variable "name" {
  description = "Name prefix for every resource. Use a different value per deployment (for example aspen-staging)."
  type        = string
  default     = "aspen"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,18}[a-z0-9]$", var.name))
    error_message = "name must be 3-20 characters: lowercase letters, digits and hyphens, starting with a letter."
  }
}

variable "domain_name" {
  description = "Hostname that serves Git over HTTPS, for example git.example.com. Must sit inside route53_zone_id's zone. Nothing is hardcoded: pass a subdomain you already own."
  type        = string
}

variable "route53_zone_id" {
  description = "ID of an existing public Route53 hosted zone that contains domain_name. The module creates records and ACM validation records in it."
  type        = string
}

variable "ssh_domain_name" {
  description = "Hostname for Git over SSH. Defaults to ssh.<domain_name>."
  type        = string
  default     = null
}

variable "cdn_domain_name" {
  description = "Hostname for the pack CDN (CloudFront). Defaults to cdn.<domain_name>."
  type        = string
  default     = null
}

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block of the dedicated VPC the module creates."
  type        = string
  default     = "10.42.0.0/16"

  validation {
    condition     = can(cidrnetmask(var.vpc_cidr)) && tonumber(split("/", var.vpc_cidr)[1]) <= 20
    error_message = "vpc_cidr must be a valid IPv4 CIDR of /20 or larger."
  }
}

variable "az_count" {
  description = "Number of availability zones to spread subnets across. The ALB and RDS subnet group need at least 2."
  type        = number
  default     = 2

  validation {
    condition     = var.az_count >= 2 && var.az_count <= 4
    error_message = "az_count must be between 2 and 4."
  }
}

variable "nat_gateway" {
  description = "Create one NAT gateway and run tasks in private subnets. false (the default, about $33/month cheaper) runs tasks in public subnets with a public IP; their security group still only admits traffic from the load balancers."
  type        = bool
  default     = false
}

variable "http_ingress_cidrs" {
  description = "IPv4 CIDRs allowed to reach the HTTPS load balancer."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "ssh_ingress_cidrs" {
  description = "IPv4 CIDRs allowed to reach the SSH load balancer."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

# ---------------------------------------------------------------------------
# Container
# ---------------------------------------------------------------------------

variable "container_image" {
  description = "Aspen container image (for example an ECR image built from deploy/docker/Dockerfile), pinned to a version tag (:v1.2.3) or a digest (@sha256:...). null runs a placeholder BusyBox server that answers /healthz, so the infrastructure can be brought up before Aspen serves Git."
  type        = string
  default     = null

  validation {
    condition     = var.container_image == null || can(regex("(:v?[0-9]+\\.[0-9]+\\.[0-9]+[^:/]*|@sha256:[0-9a-f]{64})$", var.container_image))
    error_message = "container_image must be pinned to a version tag (:v1.2.3) or a digest (@sha256:...), never :latest or untagged, so a new task cannot run different code."
  }
}

variable "container_command" {
  description = "Arguments passed to the image's entrypoint. Ignored for the placeholder. null uses the image's CMD."
  type        = list(string)
  default     = null
}

variable "container_cpu" {
  description = "Fargate task CPU units (256 = 0.25 vCPU)."
  type        = number
  default     = 256
}

variable "container_memory" {
  description = "Fargate task memory in MiB. Must be a valid pairing with container_cpu."
  type        = number
  default     = 512
}

variable "cpu_architecture" {
  description = "Fargate CPU architecture: X86_64 or ARM64. ARM64 is about 20% cheaper; the image must be built for it."
  type        = string
  default     = "X86_64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.cpu_architecture)
    error_message = "cpu_architecture must be X86_64 or ARM64."
  }
}

variable "desired_count" {
  description = "Number of Aspen tasks."
  type        = number
  default     = 1
}

variable "compactor_count" {
  description = "Tasks of a separate compaction service running `aspen serve` with ASPEN_ROLE=compact: compaction, commit indexing and garbage collection, scaled apart from the Git tasks, which then run ASPEN_ROLE=serve (deploy/README.md, \"Process roles\"). 0 (the default) runs the background worker in every Git task (ASPEN_ROLE=all). Ignored while container_image is unset. Scale it on the aspen_compaction_backlog metric."
  type        = number
  default     = 0

  validation {
    condition     = var.compactor_count >= 0 && floor(var.compactor_count) == var.compactor_count
    error_message = "compactor_count must be a whole number, 0 or more."
  }
}

variable "compactor_cpu" {
  description = "Fargate CPU units per compaction task."
  type        = number
  default     = 1024
}

variable "compactor_memory" {
  description = "Fargate memory per compaction task in MiB."
  type        = number
  default     = 2048
}

variable "compactor_environment" {
  description = "Extra or overriding environment for the compaction tasks only, such as ASPEN_COMPACT_WORKERS. container_environment is not applied to them: it may hold Git-only settings a compaction task refuses."
  type        = map(string)
  default     = {}
}

variable "fargate_spot" {
  description = "Run tasks on Fargate Spot (about 70% cheaper, can be interrupted with two minutes' notice)."
  type        = bool
  default     = false
}

variable "http_port" {
  description = "Port Aspen listens on for Git over HTTP inside the container (ASPEN_LISTEN)."
  type        = number
  default     = 7070
}

variable "ssh_port" {
  description = "Port Aspen listens on for Git over SSH inside the container (ASPEN_SSH_LISTEN)."
  type        = number
  default     = 2222
}

variable "aspen_limits_json" {
  description = "Aspen rate limits and push quotas (aspen_auth::LimitsConfig) as JSON, set as ASPEN_LIMITS. The default limits read and push requests per caller (deploy/README.md); \"{}\" turns request rates off."
  type        = string
  default     = "{\"read\":{\"burst\":100,\"per_second\":20},\"push\":{\"burst\":20,\"per_second\":5}}"

  validation {
    condition     = can(jsondecode(var.aspen_limits_json))
    error_message = "aspen_limits_json must be valid JSON."
  }
}

variable "container_environment" {
  description = "Extra environment variables for the Aspen container. They override the module's defaults with the same name."
  type        = map(string)
  default     = {}
}

variable "container_secrets" {
  description = "Extra environment variables filled from Secrets Manager: variable name => secret ARN. The task execution role is granted read access to exactly these secrets."
  type        = map(string)
  default     = {}
}

variable "ecr_repository_arns" {
  description = "Private ECR repositories the tasks may pull from. Leave empty for public images."
  type        = list(string)
  default     = []
}

variable "ephemeral_storage_gib" {
  description = "Fargate ephemeral storage per task in GiB (20 to 200). The push spool lives there and needs about twice the largest accepted push (ASPEN_MAX_PUSH_BYTES)."
  type        = number
  default     = 20

  validation {
    condition     = var.ephemeral_storage_gib >= 20 && var.ephemeral_storage_gib <= 200
    error_message = "ephemeral_storage_gib must be between 20 and 200."
  }
}

variable "enable_ecs_exec" {
  description = "Allow `aws ecs execute-command` into the tasks, which is how operators run `aspen repo create`, `aspen token create` and the other operator commands. Who may use it is decided by the operators' own IAM permissions."
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for the Aspen log group."
  type        = number
  default     = 14
}

variable "container_insights" {
  description = "ECS Container Insights: disabled, enabled or enhanced. Enabled costs extra CloudWatch metrics."
  type        = string
  default     = "disabled"

  validation {
    condition     = contains(["disabled", "enabled", "enhanced"], var.container_insights)
    error_message = "container_insights must be disabled, enabled or enhanced."
  }
}

# ---------------------------------------------------------------------------
# Load balancers
# ---------------------------------------------------------------------------

variable "enable_ssh" {
  description = "Create the network load balancer for Git over SSH (about $16/month plus a public IPv4 address per AZ). Its health check probes the HTTP port, so it is safe to enable before Aspen serves SSH."
  type        = bool
  default     = true
}

variable "ssh_host_key_version" {
  description = "Bump to generate a new SSH host key. Clients pin the host key, so every client must update known_hosts afterwards; force a new ECS deployment after applying."
  type        = number
  default     = 1
}

variable "ssh_public_port" {
  description = "Port clients connect to for Git over SSH."
  type        = number
  default     = 22
}

variable "alb_idle_timeout" {
  description = "Seconds the ALB keeps an idle connection open. Large pushes and clones stream for a long time."
  type        = number
  default     = 600
}

variable "load_balancer_deletion_protection" {
  description = "Turn on deletion protection for the load balancers."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

variable "db_engine_version" {
  description = "RDS PostgreSQL major (or major.minor) version."
  type        = string
  default     = "17"
}

variable "db_instance_class" {
  description = "RDS instance class."
  type        = string
  default     = "db.t4g.micro"
}

variable "db_allocated_storage" {
  description = "Initial RDS storage in GiB (gp3)."
  type        = number
  default     = 20
}

variable "db_max_allocated_storage" {
  description = "Upper bound for RDS storage autoscaling in GiB. Set equal to db_allocated_storage to disable autoscaling."
  type        = number
  default     = 100
}

variable "db_multi_az" {
  description = "Run RDS as Multi-AZ (doubles the instance cost)."
  type        = bool
  default     = false
}

variable "db_backup_retention_days" {
  description = "Days of automated RDS backups to keep."
  type        = number
  default     = 7
}

variable "db_password_version" {
  description = "Bump to generate and apply a new database password (it is write-only and never stored in state)."
  type        = number
  default     = 1
}

variable "db_apply_immediately" {
  description = "Apply database changes (instance class, storage) as soon as Terraform applies them, rather than in the next maintenance window. A class change restarts a single-AZ instance, a few minutes without a database."
  type        = bool
  default     = false
}

variable "db_deletion_protection" {
  description = "Protect the database from deletion. Turn off (and apply) before `terraform destroy`."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Object storage and CDN
# ---------------------------------------------------------------------------

variable "bucket_force_destroy" {
  description = "Allow `terraform destroy` to delete the pack bucket while it still holds objects. Leave false for anything you care about."
  type        = bool
  default     = false
}

variable "abort_incomplete_multipart_days" {
  description = "Days after which incomplete multipart uploads (interrupted pushes) are aborted and their parts deleted."
  type        = number
  default     = 1
}

variable "noncurrent_version_expiration_days" {
  description = "Days to keep overwritten or deleted object versions before they are removed."
  type        = number
  default     = 30
}

variable "enable_express" {
  description = "Serve packfile URIs from every task's /-/express (docs/EXPRESS.md), signed with keys the module generates into Secrets Manager. Clients that opt in with `git config fetch.uriprotocols https` then download packs over plain HTTPS instead of inline."
  type        = bool
  default     = true
}

variable "express_keys_version" {
  description = "Bump to generate a new express signing key and replace the old one (it is write-only and never stored in state). URLs signed with the old key stop working at once, so fetches in flight during the next ten minutes may fail and be retried; force a new ECS deployment after applying."
  type        = number
  default     = 1
}

variable "enable_cdn" {
  description = "Put a CloudFront distribution at cdn_domain_name in front of the tasks' /-/express. Needs enable_express. CloudFront answers 416 for a range past the end of a cached pack, so only git 2.56 and later get CDN URLs; older clients use /-/express directly."
  type        = bool
  default     = true

  validation {
    condition     = !var.enable_cdn || var.enable_express
    error_message = "enable_cdn = true needs enable_express = true: the CDN's origin is /-/express."
  }
}

variable "cdn_log_retention_days" {
  description = "Days to keep the CDN's CloudFront standard logs in their own bucket (the only record of cache hits, which never reach the origin). 0 turns them off."
  type        = number
  default     = 7

  validation {
    condition     = var.cdn_log_retention_days >= 0 && floor(var.cdn_log_retention_days) == var.cdn_log_retention_days
    error_message = "cdn_log_retention_days must be a whole number of days, 0 or more."
  }
}

variable "cdn_price_class" {
  description = "CloudFront price class. PriceClass_100 (North America and Europe edges) is cheapest."
  type        = string
  default     = "PriceClass_100"

  validation {
    condition     = contains(["PriceClass_100", "PriceClass_200", "PriceClass_All"], var.cdn_price_class)
    error_message = "cdn_price_class must be PriceClass_100, PriceClass_200 or PriceClass_All."
  }
}

# ---------------------------------------------------------------------------
# Budget
# ---------------------------------------------------------------------------

variable "budget_enabled" {
  description = "Create the AWS Budgets alarms below. Turn off only when something else already watches the whole account, such as the account stack in deploy/terraform/aws/public-demo/account."
  type        = bool
  default     = true
}

variable "budget_limit_usd" {
  description = "Monthly cost budget in USD. Alerts fire at 80% and 100% of actual spend and at 100% of forecast spend."
  type        = number
  default     = 300
}

variable "budget_alert_emails" {
  description = "Email addresses that receive budget alerts. Set this, budget_sns_topic_arn, or both."
  type        = list(string)
  default     = []
}

variable "budget_sns_topic_arn" {
  description = "SNS topic that receives budget alerts. Its access policy must allow budgets.amazonaws.com to publish."
  type        = string
  default     = null
}

variable "budget_cost_filters" {
  description = "Cost filters for the budget, for example { TagKeyValue = [\"user:Project$aspen\"] }. Leave empty in a dedicated account so the budget covers everything in it."
  type        = map(list(string))
  default     = {}
}

variable "cdn_budget_limit_usd" {
  description = "Separate monthly budget for CloudFront spend (egress), in USD. 0 disables it."
  type        = number
  default     = 100
}
