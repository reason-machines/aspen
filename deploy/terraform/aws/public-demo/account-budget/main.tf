# The check both public-demo roots run on the account's budget: it must be a
# monthly cost budget in USD over the whole account (no cost filters), of at
# most max_limit_usd, whose alerts all reach someone, and at least one of
# which fires by the time spend passes max_limit_usd. Spend beyond that is
# the owner's decision (docs/PLAN.md section 6): the owner approved about
# $650/day (about $20,000 a month) for the public share from 2026-11-06.
#
# The budget is read through Cloud Control, not the aws_budgets_budget data
# source: provider 6.71's data source returns time_unit as null and no
# notifications even for a monthly budget with alerts, so a check built on it
# can never pass against a real budget (or would have to ignore both).

terraform {
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.71"
    }
  }
}

variable "name" {
  description = "Name of the account budget to check."
  type        = string
}

variable "max_limit_usd" {
  description = "Largest monthly limit the check accepts: what the owner approved (account stack's budget_limit_usd ceiling)."
  type        = number
  default     = 20000
  nullable    = false
}

data "aws_cloudcontrolapi_resource" "budget" {
  type_name  = "AWS::Budgets::Budget"
  identifier = var.name

  lifecycle {
    postcondition {
      condition = try(
        jsondecode(self.properties).Budget.BudgetType == "COST" &&
        jsondecode(self.properties).Budget.TimeUnit == "MONTHLY" &&
        length(try(jsondecode(self.properties).Budget.CostFilters, {})) == 0 &&
        jsondecode(self.properties).Budget.BudgetLimit.Unit == "USD" &&
        tonumber(jsondecode(self.properties).Budget.BudgetLimit.Amount) <= var.max_limit_usd &&
        length(jsondecode(self.properties).NotificationsWithSubscribers) > 0 &&
        alltrue([for n in jsondecode(self.properties).NotificationsWithSubscribers : length(n.Subscribers) > 0]) &&
        # At least one alert must fire by the time spend (actual or
        # forecast) passes max_limit_usd. A limit under max_limit_usd is not
        # enough on its own: a $300 budget whose only alert is at 1000%
        # stays silent until $3,000. Thresholds are a percentage of the
        # limit unless ThresholdType says ABSOLUTE_VALUE (dollars).
        anytrue([
          for n in jsondecode(self.properties).NotificationsWithSubscribers :
          n.Notification.ComparisonOperator == "GREATER_THAN" && (
            try(n.Notification.ThresholdType, "PERCENTAGE") == "ABSOLUTE_VALUE"
            ? tonumber(n.Notification.Threshold)
            : tonumber(n.Notification.Threshold) * tonumber(jsondecode(self.properties).Budget.BudgetLimit.Amount) / 100
          ) <= var.max_limit_usd
        ]),
        false
      )
      error_message = "The account budget ${var.name} must be a monthly cost budget in USD over the whole account (no cost filters) of at most ${var.max_limit_usd} USD, with every alert reaching a subscriber and at least one alert that fires once spend passes ${var.max_limit_usd} USD. Apply ../account first (README.md, \"2. The account stack\")."
    }
  }
}

output "name" {
  description = "The checked budget's name."
  value       = var.name
}

output "limit_usd" {
  description = "The checked budget's monthly limit in USD."
  value       = tonumber(jsondecode(data.aws_cloudcontrolapi_resource.budget.properties).Budget.BudgetLimit.Amount)
}
