variable "project_id" {
  description = "The dedicated project."
  type        = string
}

variable "region" {
  description = "Region."
  type        = string
  default     = "us-central1"
}

variable "domain_name" {
  description = "Host name clients use."
  type        = string
}

variable "dns_managed_zone" {
  description = "Cloud DNS zone name, or null."
  type        = string
  default     = null
}

variable "container_image" {
  description = "Aspen image in Artifact Registry, pinned."
  type        = string
}

variable "express_keys_secret_id" {
  description = "Secret holding ASPEN_EXPRESS_KEYS, to turn on Cloud CDN; or null."
  type        = string
  default     = null
}

variable "billing_account" {
  description = "Billing account for the budget."
  type        = string
}

variable "budget_alert_emails" {
  description = "Budget alert recipients."
  type        = list(string)
}
