# Monthly budget alarm on the resource group ($300 by default): 80% and 100%
# of actual spend and 100% of forecast, mailed to budget_alert_emails.

locals {
  budget_notifications = [
    { threshold = 80, type = "Actual" },
    { threshold = 100, type = "Actual" },
    { threshold = 100, type = "Forecasted" },
  ]
}

resource "azurerm_consumption_budget_resource_group" "aspen" {
  name              = "${var.name}-monthly"
  resource_group_id = azurerm_resource_group.aspen.id
  amount            = var.budget_limit_usd
  time_grain        = "Monthly"

  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", plantimestamp())
  }

  dynamic "notification" {
    for_each = local.budget_notifications

    content {
      enabled        = true
      operator       = "GreaterThan"
      threshold      = notification.value.threshold
      threshold_type = notification.value.type
      contact_emails = var.budget_alert_emails
    }
  }

  lifecycle {
    # The start date is fixed when the budget is created.
    ignore_changes = [time_period]
  }
}
