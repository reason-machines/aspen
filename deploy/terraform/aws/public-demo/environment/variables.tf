variable "aws_account_id" {
  description = "ID of the AWS account dedicated to Aspen's public demo (the account stack's account). Terraform refuses every other account. Never Reason's production account."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be a 12-digit AWS account ID."
  }
}

variable "region" {
  description = "AWS region for everything except CloudFront's certificate."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "staging or demo. Names every resource (aspen-<environment>); bring staging up first."
  type        = string

  validation {
    condition     = contains(["staging", "demo"], var.environment)
    error_message = "environment must be staging or demo."
  }
}

variable "account_budget_name" {
  description = "Name of the account stack's budget (../account). Planning fails unless it exists and is a monthly cost budget over the whole account of at most what the owner approved (../account-budget, max_limit_usd)."
  type        = string
  default     = "aspen-account-monthly"
}

variable "base_domain" {
  description = "Domain the environment's hostnames sit under: git.<base_domain>, cdn.git.<base_domain>, ssh.git.<base_domain> and place.<base_domain>. The account stack's zone_name for the demo, or staging.<zone_name> for staging."
  type        = string
}

variable "route53_zone_id" {
  description = "The account stack's zone_id: the hosted zone that contains base_domain."
  type        = string
}

variable "ecr_repository_arns" {
  description = "The account stack's ecr_repository_arns: the repositories the tasks pull from."
  type        = list(string)
  default     = []
}

# ---------------------------------------------------------------------------
# Aspen (git.<base_domain>)
# ---------------------------------------------------------------------------

variable "aspen_image" {
  description = "Aspen image, pinned by tag or digest (for example <account>.dkr.ecr.us-east-1.amazonaws.com/aspen:v0.1.0). null runs the module's placeholder health server."
  type        = string
  default     = null
}

variable "aspen_cpu" {
  description = "Fargate CPU units for Aspen (1024 = 1 vCPU)."
  type        = number
  default     = 1024
}

variable "aspen_memory" {
  description = "Fargate memory for Aspen in MiB. The push limits in main.tf are sized for 2048."
  type        = number
  default     = 2048
}

variable "aspen_environment" {
  description = "Extra Aspen settings (deploy/README.md, configuration contract); they override the defaults in main.tf, for example ASPEN_POLICY."
  type        = map(string)
  default     = {}
}

variable "enable_ssh" {
  description = "Serve Git over SSH at ssh.git.<base_domain> (an NLB, about $24/month). Off by default: SSH has no per-address connection limit yet (docs/PLAN.md, C3 obligations for D4), and the demo needs only HTTPS. Turn it on in staging to test it."
  type        = bool
  default     = false
}

variable "enable_cdn" {
  description = "CloudFront in front of /-/express for pack downloads (cdn.git.<base_domain>)."
  type        = bool
  default     = true
}

variable "db_password_version" {
  description = "Bump to generate and set a new database password (the module's db_password_version); then force new deployments of both services."
  type        = number
  default     = 1
}

variable "express_keys_version" {
  description = "Bump to replace the express URL signing key (the module's express_keys_version)."
  type        = number
  default     = 1
}

variable "ssh_host_key_version" {
  description = "Bump to replace the SSH host key; every client's known_hosts must change (the module's ssh_host_key_version)."
  type        = number
  default     = 1
}

variable "fargate_spot" {
  description = "Run Aspen on Fargate Spot. The place task never runs on Spot."
  type        = bool
  default     = false
}

variable "http_ingress_cidrs" {
  description = "IPv4 CIDRs allowed to reach the load balancer. Narrow it in staging if you like, but keep the tasks' own public addresses in (or use nat_gateway): the servers probe their /-/express through it."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "db_instance_class" {
  description = "RDS instance class of the database Aspen and place share. null takes the environment's size from main.tf (demo db.m7g.2xlarge, staging db.t4g.micro). A change is applied at once and restarts the database."
  type        = string
  default     = null
}

variable "db_allocated_storage" {
  description = "Database storage in GiB (gp3). null takes the environment's size from main.tf (demo 400, staging 20). RDS can grow storage but never shrink it."
  type        = number
  default     = null
}

variable "db_max_allocated_storage" {
  description = "Upper bound for RDS storage autoscaling in GiB. null takes the environment's size from main.tf (demo 1000, staging 100)."
  type        = number
  default     = null
}

variable "db_deletion_protection" {
  description = "Protect the database from deletion. Turn off (and apply) before destroying the environment."
  type        = bool
  default     = true
}

variable "bucket_force_destroy" {
  description = "Let `terraform destroy` delete the pack and place buckets with their objects. true is fine for staging."
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# place (place.<base_domain>)
# ---------------------------------------------------------------------------

variable "place_image" {
  description = "The place image (deploy/docker/Dockerfile, target place), pinned by tag or digest. null leaves place out entirely."
  type        = string
  default     = null

  validation {
    condition     = var.place_image == null || can(regex("(:v?[0-9]+\\.[0-9]+\\.[0-9]+[^:/]*|@sha256:[0-9a-f]{64})$", var.place_image))
    error_message = "place_image must be pinned to a version tag (:v1.2.3) or a digest (@sha256:...), never :latest or untagged."
  }
}

variable "place_hostname" {
  description = "Hostname of the place page, its API and canvas.git. Null means place.<base_domain>. It may be base_domain itself (the demo serves place at the delegated zone's apex, with Aspen at git.<base_domain>) or any name under it, and must differ from Aspen's hostnames."
  type        = string
  default     = null

  validation {
    condition = var.place_hostname == null || (
      can(regex("^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\\.)+[a-z]{2,63}$", var.place_hostname)) &&
      (var.place_hostname == var.base_domain || endswith(var.place_hostname, ".${var.base_domain}")) &&
      !contains(["git.${var.base_domain}", "cdn.git.${var.base_domain}", "ssh.git.${var.base_domain}"], var.place_hostname)
    )
    error_message = "place_hostname must be base_domain or a name under it, and not one of Aspen's hostnames (git., cdn.git., ssh.git.)."
  }
}

variable "place_running" {
  description = "Run the place task (one instance; place never runs more). false keeps everything but stops the demo, a cost and abuse stop besides the kill switch."
  type        = bool
  default     = true
}

variable "place_live_cdn" {
  description = "CloudFront at live.<place host> serving only /api/v1/canvas.svg, cached per 2-second bucket, for pages that embed and poll the canvas (place-live.tf). false removes the distribution."
  type        = bool
  default     = true
}

variable "place_cpu" {
  description = "Fargate CPU units for the place task (1024 = 1 vCPU). null takes the environment's size from main.tf (demo 16384, staging 512)."
  type        = number
  default     = null
}

variable "place_memory" {
  description = "Fargate memory for the place task in MiB; it must be a size Fargate allows with place_cpu. null takes the environment's size from main.tf (demo 65536, staging 1024)."
  type        = number
  default     = null
}

variable "place_db_max_connections" {
  description = "Connections in aspen-place's database pool (PLACE_DB_MAX_CONNECTIONS). null takes the environment's size from main.tf (demo 64, staging 10)."
  type        = number
  default     = null

  validation {
    condition     = var.place_db_max_connections == null || try(var.place_db_max_connections >= 1 && floor(var.place_db_max_connections) == var.place_db_max_connections, false)
    error_message = "place_db_max_connections must be a whole number of at least 1."
  }
}

variable "place_port" {
  description = "Port aspen-place listens on inside the task (PLACE_LISTEN)."
  type        = number
  default     = 7080
}

variable "place_dashboard_url" {
  description = "Where the page links its live dashboard, or none to hide the link until the dashboard exists."
  type        = string
  default     = "none"
}

variable "place_environment" {
  description = "Extra aspen-place settings (demo/place/server/README.md); they override the defaults in place.tf, for example PLACE_SWARM_MAX_AGENTS."
  type        = map(string)
  default     = {}
}

variable "place_db_password_version" {
  description = "Bump to generate a new password for the `place` database role; then force a new deployment of the place service."
  type        = number
  default     = 1
}

variable "place_listener_rule_priority" {
  description = "Priority of the ALB rule that sends place.<base_domain> to the place task."
  type        = number
  default     = 100
}

variable "place_database_init_image" {
  description = "Image whose psql (15 or later, for \\getenv) creates the place database on first start."
  type        = string
  default     = "public.ecr.aws/docker/library/postgres:17-alpine@sha256:266d3c257594e195b309ae9d1805e54c788f1d7b375f56bf6c367aac6c917e7b"
}

variable "place_canvas_mirror" {
  description = "A GitHub repository (owner/name) that a sidecar of the place task keeps a read-only copy of the canvas's main in, pushing with a deploy key or a repository-scoped token, forced so it follows main when place starts main's history again. null turns it off. The credential goes into the place_canvas_mirror_key output's secret by hand (../README.md, \"Mirror the canvas to GitHub\")."
  type        = string
  default     = null

  validation {
    condition     = var.place_canvas_mirror == null || can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.place_canvas_mirror))
    error_message = "place_canvas_mirror must be owner/name."
  }
}

variable "place_canvas_mirror_interval_seconds" {
  description = "How often the mirror sidecar fetches main and, when it moved, pushes it (one forced push per run; the first run is at start). 15 minutes keeps the public mirror's history to a commit or so per quarter hour."
  type        = number
  default     = 900

  validation {
    condition     = var.place_canvas_mirror_interval_seconds >= 30
    error_message = "place_canvas_mirror_interval_seconds must be at least 30."
  }
}

variable "place_canvas_mirror_image" {
  description = "Image of the mirror sidecar: Alpine, which installs git and openssh-client at start."
  type        = string
  default     = "public.ecr.aws/docker/library/alpine:3.22@sha256:2e0b81d60597f449baa56dc25d355367869d2dca4cd57e866f1e2b52af3eb55f"
}
