# Monthly cost alarms. In a dedicated account (the recommended setup) leave
# budget_cost_filters empty so the budget covers everything in it. With
# several deployments in one account, watch the account once instead
# (budget_enabled = false here; see deploy/terraform/aws/public-demo/account).

locals {
  budget_has_target = length(var.budget_alert_emails) > 0 || var.budget_sns_topic_arn != null

  budget_notifications = [
    { threshold = 80, type = "ACTUAL" },
    { threshold = 100, type = "ACTUAL" },
    { threshold = 100, type = "FORECASTED" },
  ]
}

resource "aws_budgets_budget" "total" {
  count = var.budget_enabled ? 1 : 0

  name         = "${var.name}-monthly"
  budget_type  = "COST"
  limit_amount = format("%.2f", var.budget_limit_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  dynamic "cost_filter" {
    for_each = var.budget_cost_filters

    content {
      name   = cost_filter.key
      values = cost_filter.value
    }
  }

  dynamic "notification" {
    for_each = local.budget_notifications

    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value.threshold
      threshold_type             = "PERCENTAGE"
      notification_type          = notification.value.type
      subscriber_email_addresses = var.budget_alert_emails
      subscriber_sns_topic_arns  = var.budget_sns_topic_arn == null ? [] : [var.budget_sns_topic_arn]
    }
  }

  lifecycle {
    precondition {
      condition     = local.budget_has_target
      error_message = "Set budget_alert_emails or budget_sns_topic_arn: a budget alarm nobody receives is not an alarm."
    }
  }
}

# CloudFront egress is the cost most likely to spike on a public deployment,
# so it gets its own, lower budget.
resource "aws_budgets_budget" "cdn" {
  count = var.budget_enabled && var.enable_cdn && var.cdn_budget_limit_usd > 0 ? 1 : 0

  name         = "${var.name}-cloudfront-monthly"
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
      subscriber_sns_topic_arns  = var.budget_sns_topic_arn == null ? [] : [var.budget_sns_topic_arn]
    }
  }
}
