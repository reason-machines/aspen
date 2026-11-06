# Aspen's public demo: what the dedicated AWS account holds once, shared by
# every environment in it (staging and the public demo, ../environment):
#
# - the account-wide AWS Budgets alarms (monthly, and a daily one that
#   catches a runaway day within it) and a CloudFront budget, which go to an
#   SNS topic and email. They only notify: nothing here stops or scales down
#   anything;
# - the Route53 hosted zone for the demo's subdomain, which the parent
#   domain's owner delegates to with the NS records this stack outputs;
# - ECR repositories for the aspen and aspen-place images.
#
# Runbook: ../README.md. Nothing here may run in an account that holds
# anything else: the provider refuses any account but aws_account_id.

terraform {
  # 1.11+ to match the module's write-only attributes; S3 native state
  # locking needs 1.10+.
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.71"
    }
  }

  # Partial configuration from `-backend-config=backend.hcl` (see
  # backend.hcl.example), so no account details are committed.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = {
      Project   = "aspen"
      Stack     = "public-demo-account"
      ManagedBy = "terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# Budget alarms
#
# Budgets are global but billed per account, so one budget with no cost
# filters covers everything in this dedicated account: every environment,
# every region, CloudFront and data transfer included. The environments turn
# their own budgets off (budget_enabled = false).
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "budget" {
  name = "aspen-budget-alerts"
}

data "aws_iam_policy_document" "budget_topic" {
  statement {
    sid       = "AwsBudgetsPublish"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.budget.arn]

    principals {
      type        = "Service"
      identifiers = ["budgets.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_sns_topic_policy" "budget" {
  arn    = aws_sns_topic.budget.arn
  policy = data.aws_iam_policy_document.budget_topic.json
}

# AWS sends each address one confirmation email; alerts start once it is
# confirmed.
resource "aws_sns_topic_subscription" "budget_email" {
  for_each = toset(var.budget_alert_emails)

  topic_arn = aws_sns_topic.budget.arn
  protocol  = "email"
  endpoint  = each.value
}

locals {
  # Early warnings on actual spend, and a forecast alarm that fires as soon
  # as the month is on course to cross the limit.
  budget_notifications = [
    { threshold = 50, type = "ACTUAL" },
    { threshold = 80, type = "ACTUAL" },
    { threshold = 100, type = "ACTUAL" },
    { threshold = 100, type = "FORECASTED" },
  ]
}

locals {
  account_budget_name = coalesce(var.existing_account_budget_name, "aspen-account-monthly")
}

# A budget that already exists (an organization admin may create one with the
# account) is adopted: imported into this resource and from then on managed
# like one this stack created, with the limit, alerts and recipients below.
# It is never checked and then left out of the configuration, since a budget
# this stack had created and that was then named here would have been planned
# for destruction (count 1 -> 0) in the same apply that checked it.
import {
  for_each = var.existing_account_budget_name == null ? toset([]) : toset([var.existing_account_budget_name])

  to = aws_budgets_budget.account
  id = "${var.aws_account_id}:${each.value}"
}

resource "aws_budgets_budget" "account" {
  name         = local.account_budget_name
  budget_type  = "COST"
  limit_amount = format("%.2f", var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "notification" {
    for_each = local.budget_notifications

    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value.threshold
      threshold_type             = "PERCENTAGE"
      notification_type          = notification.value.type
      subscriber_email_addresses = var.budget_alert_emails
      subscriber_sns_topic_arns  = [aws_sns_topic.budget.arn]
    }
  }

  # The account's only spending alarm. A change that would delete or replace
  # it (renaming it, or pointing existing_account_budget_name at another
  # budget) fails the plan instead; retiring it is a deliberate state edit.
  lifecycle {
    prevent_destroy = true
  }

  depends_on = [aws_sns_topic_policy.budget]
}

# A day's spend, so a runaway day is seen within about a day, not when the
# month's total finally crosses a threshold. AWS Budgets evaluates a few
# times a day on billing data that lags by up to a day, and a daily budget
# has no forecast alerts. It notifies only: there is no budget action, so
# nothing is ever stopped by it.
resource "aws_budgets_budget" "daily" {
  name         = "aspen-account-daily"
  budget_type  = "COST"
  limit_amount = format("%.2f", var.daily_budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "DAILY"

  dynamic "notification" {
    for_each = [50, 80, 100]

    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = var.budget_alert_emails
      subscriber_sns_topic_arns  = [aws_sns_topic.budget.arn]
    }
  }

  depends_on = [aws_sns_topic_policy.budget]
}

# CloudFront egress is what a popular public demo spends fastest, so it gets
# its own, lower alarm inside the account budget.
resource "aws_budgets_budget" "cloudfront" {
  name         = "aspen-cloudfront-monthly"
  budget_type  = "COST"
  limit_amount = format("%.2f", var.cdn_budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "Service"
    values = ["Amazon CloudFront"]
  }

  dynamic "notification" {
    for_each = local.budget_notifications

    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value.threshold
      threshold_type             = "PERCENTAGE"
      notification_type          = notification.value.type
      subscriber_email_addresses = var.budget_alert_emails
      subscriber_sns_topic_arns  = [aws_sns_topic.budget.arn]
    }
  }

  depends_on = [aws_sns_topic_policy.budget]
}

# ---------------------------------------------------------------------------
# DNS: a hosted zone for a subdomain of a domain Reason already owns
# ---------------------------------------------------------------------------

resource "aws_route53_zone" "demo" {
  name    = var.zone_name
  comment = "Aspen public demo; delegated from the parent domain by NS records"
}

# ---------------------------------------------------------------------------
# Container images
# ---------------------------------------------------------------------------

resource "aws_ecr_repository" "images" {
  for_each = toset(var.ecr_repositories)

  name                 = each.value
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }
}

resource "aws_ecr_lifecycle_policy" "images" {
  # Keyed by the configured names, which are known before anything exists
  # (a for_each over the repositories themselves blocks `terraform import`).
  for_each = toset(var.ecr_repositories)

  repository = aws_ecr_repository.images[each.key].name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Drop untagged images after a week"
        selection = {
          tagStatus   = "untagged"
          countType   = "sinceImagePushed"
          countUnit   = "days"
          countNumber = 7
        }
        action = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Keep the newest 30 tagged images"
        selection = {
          tagStatus      = "tagged"
          tagPatternList = ["*"]
          countType      = "imageCountMoreThan"
          countNumber    = 30
        }
        action = { type = "expire" }
      },
    ]
  })
}

# ---------------------------------------------------------------------------
# Image publishing from GitHub Actions (.github/workflows/demo-images.yml)
#
# The workflow signs in with GitHub's OIDC token: no long-lived key exists.
# Only workflow runs on the listed refs of the listed repository can assume
# the role, and the role can only push to this stack's ECR repositories.
# ---------------------------------------------------------------------------

resource "aws_iam_openid_connect_provider" "github" {
  count = var.github_repository == null ? 0 : 1

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

locals {
  # GitHub's default subject names the repository by owner/name; a repository
  # with immutable subjects (`use_immutable_subject`, see
  # `gh api repos/<owner>/<name>/actions/oidc/customization/sub`) names it
  # owner@<owner id>/name@<repository id>, which a rename cannot reuse.
  github_sub_prefix = coalesce(var.github_sub_claim_prefix, "repo:${coalesce(var.github_repository, "-")}")
}

data "aws_iam_policy_document" "image_publisher_trust" {
  count = var.github_repository == null ? 0 : 1

  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.github[0].arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [for ref in var.github_image_refs : "${local.github_sub_prefix}:ref:${ref}"]
    }
  }
}

resource "aws_iam_role" "image_publisher" {
  count = var.github_repository == null ? 0 : 1

  name                 = "aspen-image-publisher"
  description          = "Pushes Aspen images to ECR from GitHub Actions (OIDC)."
  assume_role_policy   = data.aws_iam_policy_document.image_publisher_trust[0].json
  max_session_duration = 3600
}

data "aws_iam_policy_document" "image_publisher" {
  count = var.github_repository == null ? 0 : 1

  statement {
    sid       = "Login"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"]
  }

  statement {
    sid = "PushToTheDemoRepositories"
    actions = [
      "ecr:BatchCheckLayerAvailability",
      "ecr:BatchGetImage",
      "ecr:CompleteLayerUpload",
      "ecr:DescribeImages",
      "ecr:GetDownloadUrlForLayer",
      "ecr:InitiateLayerUpload",
      "ecr:PutImage",
      "ecr:UploadLayerPart",
    ]
    resources = [for r in aws_ecr_repository.images : r.arn]
  }
}

resource "aws_iam_role_policy" "image_publisher" {
  count = var.github_repository == null ? 0 : 1

  name   = "push-demo-images"
  role   = aws_iam_role.image_publisher[0].id
  policy = data.aws_iam_policy_document.image_publisher[0].json
}
