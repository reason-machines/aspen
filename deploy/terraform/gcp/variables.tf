variable "project_id" {
  description = "A dedicated Google Cloud project for this deployment."
  type        = string
}

variable "region" {
  description = "Region for Cloud Run, Cloud SQL and the pack bucket."
  type        = string
  default     = "us-central1"
}

variable "name" {
  description = "Prefix for resource names."
  type        = string
  default     = "aspen"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.name))
    error_message = "name must be 2-21 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "domain_name" {
  description = "Host name clients use (git.example.com). A Google-managed certificate is issued for it once it resolves to the load balancer."
  type        = string

  validation {
    condition     = can(regex("^([a-z0-9]([a-z0-9-]*[a-z0-9])?\\.)+[a-z]{2,}$", var.domain_name))
    error_message = "domain_name must be a lowercase DNS name such as git.example.com."
  }
}

variable "dns_managed_zone" {
  description = "Name of an existing Cloud DNS managed zone in this project that holds domain_name, to create its A record in. Null to manage DNS yourself (point domain_name at the ip_address output)."
  type        = string
  default     = null
}

variable "container_image" {
  description = "Aspen image in Artifact Registry (Cloud Run cannot pull from ghcr.io; mirror it with an Artifact Registry remote repository), pinned to a version tag or digest."
  type        = string

  validation {
    condition     = can(regex("(:v?[0-9]+\\.[0-9]+\\.[0-9]+[^:/]*|@sha256:[0-9a-f]{64})$", var.container_image))
    error_message = "container_image must be pinned to a version tag (:v1.2.3) or a digest (@sha256:...)."
  }
  validation {
    condition     = !startswith(var.container_image, "ghcr.io/")
    error_message = "Cloud Run cannot pull from ghcr.io; mirror the image through an Artifact Registry remote repository (deploy/README.md)."
  }
}

variable "cpu" {
  description = "vCPUs per Cloud Run instance."
  type        = string
  default     = "2"
}

variable "memory" {
  description = "Memory per Cloud Run instance. Cloud Run's only local disk is memory, so the push spool (spool_size_mib) counts against it."
  type        = string
  default     = "4Gi"
}

variable "spool_size_mib" {
  description = "Size of the in-memory push spool, in MiB. A push needs about twice its pack size here."
  type        = number
  default     = 2048
}

variable "max_push_bytes" {
  description = "Largest pack one push may send (ASPEN_MAX_PUSH_BYTES). Keep it at most half of the spool (spool_size_mib); larger pushes need the Helm chart on GKE, whose spool is a real disk."
  type        = number
  default     = 1073741824
}

variable "max_object_bytes" {
  description = "Largest object Aspen inflates (ASPEN_MAX_OBJECT_BYTES)."
  type        = number
  default     = 268435456
}

variable "prefetch_memory_bytes" {
  description = "Memory concurrent pushes may hold for delta bases (ASPEN_PREFETCH_MEMORY_BYTES)."
  type        = number
  default     = 1073741824
}

variable "min_instances" {
  description = "Instances kept warm. 0 lets the service scale to zero (first request after idle waits for a cold start)."
  type        = number
  default     = 1
}

variable "max_instances" {
  description = "Most Cloud Run instances."
  type        = number
  default     = 10
}

variable "aspen_policy_json" {
  description = "Aspen access policy (aspen_auth::PolicyConfig) as JSON. The default lets nobody in without a token."
  type        = string
  default     = "{}"

  validation {
    condition     = can(jsondecode(var.aspen_policy_json))
    error_message = "aspen_policy_json must be valid JSON."
  }
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

variable "express_keys_secret_id" {
  description = "Secret Manager secret ID (in this project) holding ASPEN_EXPRESS_KEYS (`id:hexsecret`). Set it to serve packs through Cloud CDN; null keeps every pack inline or on a presigned bucket URL."
  type        = string
  default     = null
}

variable "db_tier" {
  description = "Cloud SQL machine tier."
  type        = string
  default     = "db-custom-1-3840"
}

variable "db_disk_size_gb" {
  description = "Initial Cloud SQL disk size; it grows automatically."
  type        = number
  default     = 20
}

variable "db_high_availability" {
  description = "Regional (HA) Cloud SQL instead of zonal."
  type        = bool
  default     = false
}

variable "db_password_version" {
  description = "Bump to generate and set a new database password."
  type        = number
  default     = 1
}

variable "deletion_protection" {
  description = "Protect the database, the Cloud Run service and the bucket's objects from deletion. Turn off before `terraform destroy`."
  type        = bool
  default     = true
}

variable "billing_account" {
  description = "Billing account ID (XXXXXX-XXXXXX-XXXXXX) the budget is created on."
  type        = string
}

variable "budget_limit_usd" {
  description = "Monthly budget for the project, in USD."
  type        = number
  default     = 300
}

variable "budget_alert_emails" {
  description = "Addresses that get budget alerts (80% and 100% of actual spend, 100% of forecast)."
  type        = list(string)

  validation {
    condition     = length(var.budget_alert_emails) > 0
    error_message = "Give at least one budget_alert_emails address: an alarm nobody receives is not an alarm."
  }
}

variable "labels" {
  description = "Labels added to every resource that takes them."
  type        = map(string)
  default     = {}
}
