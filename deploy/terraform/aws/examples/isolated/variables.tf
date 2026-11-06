variable "aws_account_id" {
  description = "ID of the dedicated AWS account to deploy into. Terraform refuses to touch any other account."
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

variable "name" {
  description = "Name prefix for resources, unique per deployment in the account."
  type        = string
  default     = "aspen"
}

variable "domain_name" {
  description = "Hostname for Git over HTTPS, inside route53_zone_id's zone (for example git.example.com)."
  type        = string
}

variable "route53_zone_id" {
  description = "Existing public Route53 hosted zone that contains domain_name."
  type        = string
}

variable "container_image" {
  description = "Aspen image. null runs the placeholder health server."
  type        = string
  default     = null
}

variable "ecr_repository_arns" {
  description = "Private ECR repositories the tasks may pull from."
  type        = list(string)
  default     = []
}

variable "nat_gateway" {
  description = "Run tasks in private subnets behind a NAT gateway (about $33/month more)."
  type        = bool
  default     = false
}

variable "enable_ssh" {
  description = "Create the SSH network load balancer."
  type        = bool
  default     = true
}

variable "enable_cdn" {
  description = "Create the CloudFront distribution in front of the tasks' /-/express for pack downloads."
  type        = bool
  default     = true
}

variable "fargate_spot" {
  description = "Run tasks on Fargate Spot."
  type        = bool
  default     = false
}

variable "compactor_count" {
  description = "Tasks of a separate compaction service (ASPEN_ROLE=compact); 0 runs compaction in the Git tasks."
  type        = number
  default     = 0
}

variable "db_deletion_protection" {
  description = "Protect the database from deletion."
  type        = bool
  default     = true
}

variable "bucket_force_destroy" {
  description = "Let `terraform destroy` empty and delete the pack bucket."
  type        = bool
  default     = false
}

variable "budget_limit_usd" {
  description = "Monthly cost budget in USD."
  type        = number
  default     = 300
}

variable "budget_alert_emails" {
  description = "Email addresses for budget alerts."
  type        = list(string)
  default     = []
}

variable "budget_sns_topic_arn" {
  description = "SNS topic for budget alerts."
  type        = string
  default     = null
}

variable "extra_tags" {
  description = "Additional tags applied to every resource."
  type        = map(string)
  default     = {}
}
