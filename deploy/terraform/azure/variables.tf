variable "name" {
  description = "Prefix for resource names."
  type        = string
  default     = "aspen"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,14}$", var.name))
    error_message = "name must be 2-15 lowercase letters, digits or hyphens, starting with a letter (Key Vault names are short)."
  }
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "westeurope"
}

variable "container_image" {
  description = "Aspen image, pinned to a version tag or digest (ghcr.io/reason-machines/aspen:v1.2.3)."
  type        = string

  validation {
    condition     = can(regex("(:v?[0-9]+\\.[0-9]+\\.[0-9]+[^:/]*|@sha256:[0-9a-f]{64})$", var.container_image))
    error_message = "container_image must be pinned to a version tag (:v1.2.3) or a digest (@sha256:...)."
  }
}

variable "domain_name" {
  description = "Custom host name (git.example.com). Null serves on the Container App's own *.azurecontainerapps.io name."
  type        = string
  default     = null
}

variable "dns_zone_name" {
  description = "Azure DNS zone (in dns_zone_resource_group) holding domain_name, for its CNAME and verification records. Null to manage DNS yourself."
  type        = string
  default     = null
}

variable "dns_zone_resource_group" {
  description = "Resource group of dns_zone_name."
  type        = string
  default     = null
}

# Packs. Aspen's blob layer speaks S3 (and the local file system); a native
# Azure Blob backend is issue #60. Point it at any
# S3-compatible bucket: AWS S3, Cloudflare R2, MinIO, ...
variable "s3_bucket" {
  description = "S3-compatible bucket for packs (ASPEN_BLOB_URL = s3://<bucket>)."
  type        = string
}

variable "s3_endpoint" {
  description = "Endpoint URL of the S3-compatible store (https://<account>.r2.cloudflarestorage.com). Null for AWS S3."
  type        = string
  default     = null
}

variable "s3_region" {
  description = "Region of the bucket (`auto` for R2)."
  type        = string
  default     = "auto"
}

variable "s3_access_key_id" {
  description = "Access key ID of credentials limited to objects in s3_bucket."
  type        = string
}

variable "s3_secret_access_key" {
  description = "Secret access key for s3_access_key_id (pass it as TF_VAR_s3_secret_access_key on every run). Ephemeral: written to Key Vault with a write-only attribute when s3_secret_version changes, never stored in state or a plan."
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "s3_secret_version" {
  description = "Bump (and pass s3_secret_access_key) to write a new S3 secret key."
  type        = number
  default     = 1
}

variable "cpu" {
  description = "vCPUs per replica (Consumption profile: 0.25 to 4)."
  type        = number
  default     = 2
}

variable "memory" {
  description = "Memory per replica (2 GiB per vCPU on Consumption)."
  type        = string
  default     = "4Gi"
}

variable "min_replicas" {
  description = "Replicas kept running."
  type        = number
  default     = 1
}

variable "max_replicas" {
  description = "Most replicas."
  type        = number
  default     = 5
}

variable "max_push_bytes" {
  description = "Largest pack per push (ASPEN_MAX_PUSH_BYTES). The spool is the replica's ephemeral disk (8 GiB on Consumption), and a push needs about twice its size."
  type        = number
  default     = 3221225472
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

variable "db_sku" {
  description = "Flexible Server SKU."
  type        = string
  default     = "B_Standard_B1ms"
}

variable "db_storage_mb" {
  description = "Flexible Server storage, in MB."
  type        = number
  default     = 32768
}

variable "db_password_version" {
  description = "Bump to generate and set a new database password."
  type        = number
  default     = 1
}

variable "deletion_protection" {
  description = "Put a CanNotDelete lock on the database server. Remove it (false) before `terraform destroy`."
  type        = bool
  default     = true
}

variable "budget_limit_usd" {
  description = "Monthly budget for the resource group, in USD."
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

variable "tags" {
  description = "Tags added to every resource."
  type        = map(string)
  default     = {}
}
