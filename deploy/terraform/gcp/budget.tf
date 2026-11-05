# Monthly budget alarm for the project ($300 by default), mailed to
# budget_alert_emails at 80% and 100% of actual spend and 100% of forecast.

resource "google_monitoring_notification_channel" "budget" {
  for_each = toset(var.budget_alert_emails)

  project      = var.project_id
  display_name = "${var.name} budget: ${each.value}"
  type         = "email"
  labels = {
    email_address = each.value
  }

  depends_on = [google_project_service.this]
}

resource "google_billing_budget" "aspen" {
  billing_account = var.billing_account
  display_name    = "${var.name}-monthly"

  budget_filter {
    projects = ["projects/${data.google_project.this.number}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(floor(var.budget_limit_usd))
    }
  }

  threshold_rules {
    threshold_percent = 0.8
  }
  threshold_rules {
    threshold_percent = 1.0
  }
  threshold_rules {
    threshold_percent = 1.0
    spend_basis       = "FORECASTED_SPEND"
  }

  all_updates_rule {
    monitoring_notification_channels = [for c in google_monitoring_notification_channel.budget : c.id]
    disable_default_iam_recipients   = false
  }
}
