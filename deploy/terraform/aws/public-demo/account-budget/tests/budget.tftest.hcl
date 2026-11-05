# Tests with mocked providers: no AWS credentials, no API calls.
#
#   cd deploy/terraform/aws/public-demo/account-budget
#   terraform init -backend=false
#   terraform test

mock_provider "aws" {}

variables {
  name = "aspen-account-monthly"
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
