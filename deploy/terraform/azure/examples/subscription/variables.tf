variable "subscription_id" {
  description = "The dedicated subscription."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
  default     = "westeurope"
}

variable "container_image" {
  description = "Aspen image, pinned."
  type        = string
}

variable "domain_name" {
  description = "Custom host name, or null."
  type        = string
  default     = null
}

variable "s3_bucket" {
  description = "S3-compatible bucket for packs."
  type        = string
}

variable "s3_endpoint" {
  description = "S3-compatible endpoint, or null for AWS S3."
  type        = string
  default     = null
}

variable "s3_region" {
  description = "Bucket region."
  type        = string
  default     = "auto"
}

variable "s3_access_key_id" {
  description = "Access key ID for the bucket."
  type        = string
}

variable "s3_secret_access_key" {
  description = "Secret access key for the bucket (TF_VAR_s3_secret_access_key)."
  type        = string
  sensitive   = true
  ephemeral   = true
}

variable "budget_alert_emails" {
  description = "Budget alert recipients."
  type        = list(string)
}
