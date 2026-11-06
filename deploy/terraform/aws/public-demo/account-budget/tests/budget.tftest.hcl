# Tests with mocked providers: no AWS credentials, no API calls.
#
#   cd deploy/terraform/aws/public-demo/account-budget
#   terraform init -backend=false
#   terraform test

mock_provider "aws" {}

variables {
  name = "aspen-account-monthly"
  # The rule at a $300 ceiling; the runs at the end check the default one.
  max_limit_usd = 300
}

run "a_monthly_300_dollar_budget_with_an_alert_passes" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  assert {
    condition     = output.limit_usd == 300
    error_message = "A monthly $300 cost budget with an alert must pass."
  }
}

run "a_budget_over_the_limit_is_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 1000, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "a_filtered_budget_is_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}, \"CostFilters\": {\"Service\": [\"Amazon CloudFront\"]}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "a_quarterly_budget_is_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"QUARTERLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "a_usage_budget_is_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"USAGE\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "a_budget_without_alerts_is_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": []}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "an_alert_nobody_receives_is_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": []}]}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "unreadable_properties_are_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "an_alarm_that_only_fires_at_3000_dollars_is_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 1000, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "an_absolute_alarm_above_300_dollars_is_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 3000, \"ThresholdType\": \"ABSOLUTE_VALUE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "an_alarm_on_spend_falling_below_is_refused" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"LESS_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}

run "an_absolute_alarm_at_300_dollars_passes" {
  command = plan

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 1000, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}, {\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"FORECASTED\", \"Threshold\": 300, \"ThresholdType\": \"ABSOLUTE_VALUE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  assert {
    condition     = output.limit_usd == 300
    error_message = "A $300 budget with one alert that fires at $300 must pass, even beside a later one."
  }
}

# The default ceiling is what the owner approved for the public share:
# about $650/day, $20,000 a month (docs/PLAN.md decisions log, 2026-11-06).
run "by_default_the_approved_20000_dollar_budget_passes" {
  command = plan

  variables {
    max_limit_usd = null
  }

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 20000, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  assert {
    condition     = output.limit_usd == 20000
    error_message = "The approved $20,000/month budget must pass the default check."
  }
}

run "by_default_a_budget_above_20000_dollars_is_refused" {
  command = plan

  variables {
    max_limit_usd = null
  }

  override_data {
    target = data.aws_cloudcontrolapi_resource.budget
    values = { properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 20001, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}" }
  }

  expect_failures = [data.aws_cloudcontrolapi_resource.budget]
}
