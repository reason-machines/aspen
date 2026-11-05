variable "aws_account_id" {
  description = "ID of the AWS account dedicated to Aspen's public demo. Terraform refuses to touch any other account. Never Reason's production account."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be a 12-digit AWS account ID."
  }
}

variable "region" {
  description = "Region for the regional resources (SNS, ECR). Budgets and Route53 are global."
  type        = string
  default     = "us-east-1"
}

variable "zone_name" {
  description = "Subdomain the demo lives under, for example aspen.example.com. It must be a subdomain of a domain Reason already owns: this stack never registers or buys a domain. The parent zone's owner adds the NS records from the name_servers output."
  type        = string

  validation {
    condition     = can(regex("^([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\\.){2,}[a-z]{2,63}$", var.zone_name))
    error_message = "zone_name must be a lowercase subdomain with at least three labels (for example aspen.example.com), never a registrable domain itself."
  }
}

variable "budget_limit_usd" {
  description = "Monthly cost budget for the whole account, in USD. Alerts at 50%, 80% and 100% of actual spend and at 100% of forecast spend."
  type        = number
  default     = 300

  # Spending beyond $300/month is the owner's decision (docs/PLAN.md section
  # 6): raising this limit is a reviewed code change, not a variable.
  validation {
    condition     = var.budget_limit_usd > 0 && var.budget_limit_usd <= 300
    error_message = "budget_limit_usd must be between 0 and 300; a larger budget needs the owner's approval and a change to this validation."
  }
}

variable "cdn_budget_limit_usd" {
  description = "Monthly budget for CloudFront alone (egress), in USD, inside the account budget."
  type        = number
  default     = 100

  validation {
    condition     = var.cdn_budget_limit_usd > 0 && var.cdn_budget_limit_usd <= var.budget_limit_usd
    error_message = "cdn_budget_limit_usd must be positive and at most budget_limit_usd."
  }
}

variable "budget_alert_emails" {
  description = "Addresses that receive budget alerts (directly, and through the SNS topic once they confirm its subscription email)."
  type        = list(string)

  validation {
    condition     = length(var.budget_alert_emails) > 0 && alltrue([for e in var.budget_alert_emails : can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", e))])
    error_message = "Set at least one budget_alert_emails address: a budget alarm nobody receives is not an alarm."
  }
}

variable "ecr_repositories" {
  description = "ECR repositories to create: the Aspen server image and the place demo image."
  type        = list(string)
  default     = ["aspen", "aspen-place"]
}

variable "existing_account_budget_name" {
  description = "Name of an account budget that already exists (for example one an organization admin created with the account). When set, this stack imports that budget instead of creating aspen-account-monthly and manages it from then on: its limit becomes budget_limit_usd and its alerts the ones this stack sets. The stack never deletes it; renaming or repointing it fails the plan (prevent_destroy). Pass the same name to ../environment as account_budget_name."
  type        = string
  default     = null
}

variable "github_repository" {
  description = "GitHub repository (owner/name) whose Actions workflow publishes images to this account's ECR through OIDC, or null for no publisher role."
  type        = string
  default     = null

  validation {
    condition     = var.github_repository == null || can(regex("^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", var.github_repository))
    error_message = "github_repository must be owner/name."
  }
}

variable "github_sub_claim_prefix" {
  description = "The repository part of the GitHub OIDC token's sub claim, before `:ref:`. null means GitHub's default, repo:<github_repository>. A repository with immutable subjects needs its sub_claim_prefix from `gh api repos/<owner>/<name>/actions/oidc/customization/sub`, for example repo:owner@123/name@456."
  type        = string
  default     = null

  validation {
    condition     = var.github_sub_claim_prefix == null || can(regex("^repo:[A-Za-z0-9_.-]+(@[0-9]+)?/[A-Za-z0-9_.-]+(@[0-9]+)?$", var.github_sub_claim_prefix))
    error_message = "github_sub_claim_prefix must look like repo:owner/name or repo:owner@<id>/name@<id>, with no wildcards."
  }
}

variable "github_image_refs" {
  description = "Git refs whose workflow runs may assume the image publisher role."
  type        = list(string)
  default     = ["refs/heads/main"]

  validation {
    condition     = length(var.github_image_refs) > 0 && alltrue([for r in var.github_image_refs : can(regex("^refs/(heads|tags)/[^*]+$", r))])
    error_message = "github_image_refs must list exact refs/heads/... or refs/tags/... names, with no wildcards."
  }
}
