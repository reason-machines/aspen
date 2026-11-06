# Tests with mocked providers: no AWS credentials, no API calls.
#
#   cd deploy/terraform/aws/public-demo/account
#   terraform init -backend=false
#   terraform test

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = { account_id = "111111111111" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  mock_resource "aws_sns_topic" {
    defaults = { arn = "arn:aws:sns:us-east-1:111111111111:aspen-budget-alerts" }
  }
  mock_resource "aws_ecr_repository" {
    defaults = { arn = "arn:aws:ecr:us-east-1:111111111111:repository/aspen" }
  }
}

variables {
  aws_account_id      = "111111111111"
  zone_name           = "aspen.example.test"
  budget_alert_emails = ["ops@example.test"]

  # Pinned, so a local terraform.tfvars cannot change what is tested.
  existing_account_budget_name = null
  github_repository            = null
}

run "one_budget_watches_the_whole_account" {
  command = plan

  # The owner's approval for the public share (about $650/day,
  # docs/PLAN.md decisions log, 2026-11-06).
  assert {
    condition     = aws_budgets_budget.account.limit_amount == "20000.00" && aws_budgets_budget.account.time_unit == "MONTHLY"
    error_message = "The account budget must be $20,000/month."
  }
  assert {
    condition     = length(aws_budgets_budget.account.cost_filter) == 0
    error_message = "The account budget must cover everything in the account: no cost filters."
  }
  assert {
    condition = (
      toset([for n in aws_budgets_budget.account.notification : "${n.threshold}% ${n.notification_type}"]) ==
      toset(["50% ACTUAL", "80% ACTUAL", "100% ACTUAL", "100% FORECASTED"])
    )
    error_message = "Alerts at 50/80/100% of actual and 100% of forecast spend."
  }
  assert {
    condition = alltrue([
      for n in aws_budgets_budget.account.notification :
      n.subscriber_email_addresses == toset(["ops@example.test"]) && length(n.subscriber_sns_topic_arns) == 1
    ])
    error_message = "Every alert must reach the emails and the SNS topic."
  }
  assert {
    condition = (
      aws_budgets_budget.cloudfront.limit_amount == "100.00" &&
      jsonencode([for f in aws_budgets_budget.cloudfront.cost_filter : [f.name, tolist(f.values)]]) == jsonencode([["Service", ["Amazon CloudFront"]]])
    )
    error_message = "CloudFront egress gets its own $100 alarm."
  }
}

run "the_zone_is_a_subdomain_and_images_are_immutable" {
  command = plan

  assert {
    condition     = aws_route53_zone.demo.name == "aspen.example.test"
    error_message = "The stack hosts the demo's subdomain."
  }
  assert {
    condition     = alltrue([for r in aws_ecr_repository.images : r.image_tag_mutability == "IMMUTABLE" && r.image_scanning_configuration[0].scan_on_push])
    error_message = "Image tags must be immutable and scanned, so a pinned tag always means the same image."
  }
  assert {
    condition     = keys(aws_ecr_repository.images) == ["aspen", "aspen-place"]
    error_message = "Repositories for both images."
  }
}

run "more_than_the_approved_20000_dollars_needs_a_code_change" {
  command = plan

  variables {
    budget_limit_usd = 20001
  }

  expect_failures = [var.budget_limit_usd]
}

run "a_daily_budget_catches_a_runaway_day_and_only_notifies" {
  command = plan

  assert {
    condition = (
      aws_budgets_budget.daily.limit_amount == "650.00" &&
      aws_budgets_budget.daily.time_unit == "DAILY" &&
      aws_budgets_budget.daily.budget_type == "COST" &&
      length(aws_budgets_budget.daily.cost_filter) == 0
    )
    error_message = "The daily budget must be $650/day of cost over the whole account."
  }
  assert {
    condition = (
      toset([for n in aws_budgets_budget.daily.notification : "${n.threshold}% ${n.notification_type}"]) ==
      toset(["50% ACTUAL", "80% ACTUAL", "100% ACTUAL"])
    )
    error_message = "Daily alerts at 50/80/100% of a day's actual spend (daily budgets have no forecast)."
  }
  assert {
    condition = alltrue([
      for n in aws_budgets_budget.daily.notification :
      n.subscriber_email_addresses == toset(["ops@example.test"]) && length(n.subscriber_sns_topic_arns) == 1
    ])
    error_message = "Every daily alert must reach the emails and the SNS topic."
  }
}

run "the_daily_budget_fits_inside_the_monthly_one" {
  command = plan

  variables {
    budget_limit_usd       = 300
    daily_budget_limit_usd = 650
  }

  expect_failures = [var.daily_budget_limit_usd]
}

run "a_budget_needs_a_recipient" {
  command = plan

  variables {
    budget_alert_emails = []
  }

  expect_failures = [var.budget_alert_emails]
}

run "a_registrable_domain_is_not_a_subdomain" {
  command = plan

  variables {
    zone_name = "example.test"
  }

  expect_failures = [var.zone_name]
}

# The stack creates its budget, then is pointed at it as an existing budget.
# That plan used to destroy the budget it had just checked (count 1 -> 0);
# now the import finds it already managed and keeps it. Importing a budget
# the stack does not yet manage cannot be mocked (mock providers do not
# import); README.md, "2. The account stack", shows the live plan.
run "create_the_budget" {
  command = apply
}

run "switching_to_the_created_budget_as_existing_keeps_it" {
  command = plan

  variables {
    existing_account_budget_name = "aspen-account-monthly"
  }

  assert {
    condition     = aws_budgets_budget.account.name == "aspen-account-monthly" && aws_budgets_budget.account.limit_amount == "20000.00"
    error_message = "The budget the stack created must still be planned, at its limit, once it is named as existing."
  }
}

run "no_image_publisher_unless_asked" {
  command = plan

  assert {
    condition     = length(aws_iam_role.image_publisher) == 0 && length(aws_iam_openid_connect_provider.github) == 0
    error_message = "Without github_repository there is no OIDC provider and no publisher role."
  }
}

run "the_image_publisher_trusts_only_the_listed_repository_and_refs" {
  command = plan

  variables {
    github_repository = "example/aspen"
  }

  override_data {
    target = data.aws_iam_policy_document.image_publisher_trust[0]
    values = { json = "{}" }
  }

  assert {
    condition = contains(
      flatten([for c in one(data.aws_iam_policy_document.image_publisher_trust[0].statement).condition : c.values if c.variable == "token.actions.githubusercontent.com:sub"]),
      "repo:example/aspen:ref:refs/heads/main"
    ) && length(flatten([for c in one(data.aws_iam_policy_document.image_publisher_trust[0].statement).condition : c.values if c.variable == "token.actions.githubusercontent.com:sub"])) == 1
    error_message = "Only main of the listed repository may assume the publisher role."
  }
  assert {
    condition     = aws_iam_role.image_publisher[0].name == "aspen-image-publisher"
    error_message = "The publisher role exists with a stable name."
  }
}

run "an_immutable_subject_is_trusted_instead_of_the_name" {
  command = plan

  variables {
    github_repository       = "example/aspen"
    github_sub_claim_prefix = "repo:example@1/aspen@2"
  }

  override_data {
    target = data.aws_iam_policy_document.image_publisher_trust[0]
    values = { json = "{}" }
  }

  assert {
    condition     = flatten([for c in one(data.aws_iam_policy_document.image_publisher_trust[0].statement).condition : c.values if c.variable == "token.actions.githubusercontent.com:sub"]) == ["repo:example@1/aspen@2:ref:refs/heads/main"]
    error_message = "With immutable subjects only the id-based subject of main may assume the role."
  }
}

run "a_wildcard_subject_is_refused" {
  command = plan

  variables {
    github_repository       = "example/aspen"
    github_sub_claim_prefix = "repo:example/*"
  }

  expect_failures = [var.github_sub_claim_prefix]
}
