# Tests with mocked providers: no AWS credentials, no API calls.
#
#   cd deploy/terraform/aws/public-demo/environment
#   terraform init -backend=false
#   terraform test

mock_provider "aws" {
  mock_data "aws_availability_zones" {
    defaults = { names = ["xx-test-1a", "xx-test-1b"] }
  }
  mock_data "aws_region" {
    defaults = { region = "us-east-1" }
  }
  mock_data "aws_caller_identity" {
    defaults = { account_id = "111111111111" }
  }
  mock_data "aws_partition" {
    defaults = { partition = "aws" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  # What the account stack creates (../account).
  mock_data "aws_cloudcontrolapi_resource" {
    defaults = {
      properties = "{\"Budget\": {\"BudgetName\": \"x\", \"BudgetType\": \"COST\", \"TimeUnit\": \"MONTHLY\", \"BudgetLimit\": {\"Amount\": 300, \"Unit\": \"USD\"}}, \"NotificationsWithSubscribers\": [{\"Notification\": {\"ComparisonOperator\": \"GREATER_THAN\", \"NotificationType\": \"ACTUAL\", \"Threshold\": 80, \"ThresholdType\": \"PERCENTAGE\"}, \"Subscribers\": [{\"Address\": \"ops@example.test\", \"SubscriptionType\": \"EMAIL\"}]}]}"
    }
  }
  mock_resource "aws_cloudfront_distribution" {
    defaults = { arn = "arn:aws:cloudfront::111111111111:distribution/EXAMPLE" }
  }
  mock_resource "aws_cloudfront_function" {
    defaults = { arn = "arn:aws:cloudfront::111111111111:function/aspen-express-viewer-request" }
  }
  mock_resource "aws_ssm_parameter" {
    defaults = { arn = "arn:aws:ssm:us-east-1:111111111111:parameter/aspen/ssh-host-public-key" }
  }
  mock_resource "aws_lb" {
    defaults = { arn = "arn:aws:elasticloadbalancing:us-east-1:111111111111:loadbalancer/app/aspen/0123456789abcdef" }
  }
  mock_resource "aws_lb_listener" {
    defaults = { arn = "arn:aws:elasticloadbalancing:us-east-1:111111111111:listener/app/aspen/0123456789abcdef/0123456789abcdef" }
  }
  mock_resource "aws_lb_target_group" {
    defaults = { arn = "arn:aws:elasticloadbalancing:us-east-1:111111111111:targetgroup/aspen/0123456789abcdef" }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::111111111111:role/aspen" }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = { arn = "arn:aws:logs:us-east-1:111111111111:log-group:/aspen/aspen" }
  }
  mock_resource "aws_secretsmanager_secret" {
    defaults = { arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:aspen-db-password-abcdef" }
  }
  mock_resource "aws_ecs_task_definition" {
    defaults = { arn = "arn:aws:ecs:us-east-1:111111111111:task-definition/aspen:1" }
  }
  mock_resource "aws_ecs_cluster" {
    defaults = { arn = "arn:aws:ecs:us-east-1:111111111111:cluster/aspen", id = "arn:aws:ecs:us-east-1:111111111111:cluster/aspen" }
  }
  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::aspen-bucket-example", id = "aspen-bucket-example" }
  }
  mock_resource "aws_acm_certificate" {
    defaults = { arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000000" }
  }
  mock_resource "aws_acm_certificate_validation" {
    defaults = { certificate_arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000000" }
  }
  mock_resource "aws_db_instance" {
    defaults = { address = "aspen-staging.abc.us-east-1.rds.amazonaws.com", port = 5432, username = "aspen", db_name = "aspen" }
  }
}

mock_provider "aws" {
  alias = "us_east_1"

  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000001"
      domain_validation_options = [{
        domain_name           = "cdn.git.staging.aspen.example.test"
        resource_record_name  = "_x.cdn.git.staging.aspen.example.test."
        resource_record_type  = "CNAME"
        resource_record_value = "_y.acm-validations.aws."
      }]
    }
  }
}

mock_provider "random" {}
mock_provider "tls" {}

# Each certificate's validation records are looked up by its own hostname.
override_resource {
  target = module.aspen.aws_acm_certificate.http
  values = {
    arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000002"
    domain_validation_options = [{
      domain_name           = "git.staging.aspen.example.test"
      resource_record_name  = "_x.git.staging.aspen.example.test."
      resource_record_type  = "CNAME"
      resource_record_value = "_y.acm-validations.aws."
    }]
  }
}

# Every aws_s3_bucket and aws_secretsmanager_secret mock has the same id and
# ARN; place's own get different ones, so tests can tell them apart.
override_resource {
  target = aws_s3_bucket.place
  values = { arn = "arn:aws:s3:::aspen-staging-place-example", id = "aspen-staging-place-example" }
}

override_resource {
  target = aws_secretsmanager_secret.place_db_password
  values = { arn = "arn:aws:secretsmanager:us-east-1:111111111111:secret:aspen-staging-place-db-password-abcdef" }
}

override_resource {
  target = aws_acm_certificate.place
  values = {
    arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000003"
    domain_validation_options = [{
      domain_name           = "place.staging.aspen.example.test"
      resource_record_name  = "_x.place.staging.aspen.example.test."
      resource_record_type  = "CNAME"
      resource_record_value = "_y.acm-validations.aws."
    }]
  }
}

override_resource {
  target = aws_acm_certificate.place_live
  values = {
    arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000004"
    domain_validation_options = [{
      domain_name           = "live.place.staging.aspen.example.test"
      resource_record_name  = "_x.live.place.staging.aspen.example.test."
      resource_record_type  = "CNAME"
      resource_record_value = "_y.acm-validations.aws."
    }]
  }
}

variables {
  aws_account_id      = "111111111111"
  environment         = "staging"
  base_domain         = "staging.aspen.example.test"
  route53_zone_id     = "ZEXAMPLE"
  ecr_repository_arns = ["arn:aws:ecr:us-east-1:111111111111:repository/aspen", "arn:aws:ecr:us-east-1:111111111111:repository/aspen-place"]
  aspen_image         = "111111111111.dkr.ecr.us-east-1.amazonaws.com/aspen:v0.1.0"
  place_image         = "111111111111.dkr.ecr.us-east-1.amazonaws.com/aspen-place:v0.1.0"
}

run "staging_serves_git_and_place_under_its_base_domain" {
  # apply against the mocks, so rendered task definitions are known.
  command = apply

  assert {
    condition     = output.git_url == "https://git.staging.aspen.example.test"
    error_message = "Aspen should serve git.<base_domain>."
  }
  assert {
    condition     = output.cdn_url == "https://cdn.git.staging.aspen.example.test/-/express"
    error_message = "The CDN should sit at cdn.git.<base_domain>, in front of /-/express."
  }
  assert {
    condition     = output.place_url == "https://place.staging.aspen.example.test" && output.place_clone_url == "https://place.staging.aspen.example.test/canvas.git"
    error_message = "place should serve place.<base_domain> and its canvas.git."
  }
  assert {
    condition     = one(one(aws_lb_listener_rule.place[0].condition).host_header).values == toset(["place.staging.aspen.example.test"])
    error_message = "Only place.<base_domain> may reach the place task."
  }
  assert {
    condition     = aws_lb_listener_certificate.place[0].listener_arn == module.aspen.https_listener_arn
    error_message = "place's certificate must be on the module's HTTPS listener."
  }
}

run "the_demo_can_serve_place_at_its_zone_apex" {
  command = apply
  # Its own state: certificates for other hostnames must not carry over.
  state_key = "apex"

  variables {
    environment    = "demo"
    base_domain    = "place.example.test"
    place_hostname = "place.example.test"
  }

  override_resource {
    target = aws_acm_certificate.place
    values = {
      arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000011"
      domain_validation_options = [{
        domain_name           = "place.example.test"
        resource_record_name  = "_x.place.example.test."
        resource_record_type  = "CNAME"
        resource_record_value = "_y.acm-validations.aws."
      }]
    }
  }

  override_resource {
    target = module.aspen.aws_acm_certificate.http
    values = {
      arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000012"
      domain_validation_options = [{
        domain_name           = "git.place.example.test"
        resource_record_name  = "_x.git.place.example.test."
        resource_record_type  = "CNAME"
        resource_record_value = "_y.acm-validations.aws."
      }]
    }
  }

  override_resource {
    target = aws_acm_certificate.place_live
    values = {
      arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000014"
      domain_validation_options = [{
        domain_name           = "live.place.example.test"
        resource_record_name  = "_x.live.place.example.test."
        resource_record_type  = "CNAME"
        resource_record_value = "_y.acm-validations.aws."
      }]
    }
  }

  override_resource {
    target = module.aspen.aws_acm_certificate.cdn
    values = {
      arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000013"
      domain_validation_options = [{
        domain_name           = "cdn.git.place.example.test"
        resource_record_name  = "_x.cdn.git.place.example.test."
        resource_record_type  = "CNAME"
        resource_record_value = "_y.acm-validations.aws."
      }]
    }
  }

  assert {
    condition     = output.place_url == "https://place.example.test" && output.place_clone_url == "https://place.example.test/canvas.git"
    error_message = "place should serve place_hostname and its canvas.git."
  }
  assert {
    condition     = output.git_url == "https://git.place.example.test"
    error_message = "Aspen stays at git.<base_domain>."
  }
  assert {
    condition     = aws_route53_record.place[0].name == "place.example.test" && aws_acm_certificate.place[0].domain_name == "place.example.test"
    error_message = "The record and certificate must be for place_hostname."
  }
  assert {
    condition     = one(one(aws_lb_listener_rule.place[0].condition).host_header).values == toset(["place.example.test"])
    error_message = "Only place_hostname may reach the place task."
  }
}

run "the_live_canvas_cdn_serves_only_the_image_cached_per_2_seconds" {
  command = apply

  assert {
    condition     = output.place_live_canvas_url == "https://live.place.staging.aspen.example.test/api/v1/canvas.svg"
    error_message = "The live canvas CDN should sit at live.<place host>."
  }
  assert {
    condition     = one(aws_cloudfront_distribution.place_live[0].origin).domain_name == "place.staging.aspen.example.test" && one(one(aws_cloudfront_distribution.place_live[0].origin).origin_shield).enabled
    error_message = "Its origin is the place hostname, behind Origin Shield."
  }
  assert {
    condition = (
      aws_cloudfront_cache_policy.place_live[0].min_ttl == 2 &&
      aws_cloudfront_cache_policy.place_live[0].default_ttl == 2 &&
      aws_cloudfront_cache_policy.place_live[0].max_ttl == 10 &&
      one(one(one(aws_cloudfront_cache_policy.place_live[0].parameters_in_cache_key_and_forwarded_to_origin).query_strings_config).query_strings).items == toset(["t"]) &&
      one(one(aws_cloudfront_cache_policy.place_live[0].parameters_in_cache_key_and_forwarded_to_origin).cookies_config).cookie_behavior == "none" &&
      one(one(aws_cloudfront_cache_policy.place_live[0].parameters_in_cache_key_and_forwarded_to_origin).headers_config).header_behavior == "none"
    )
    error_message = "The cache key is the path and t only, held 2 seconds even though the origin says no-cache."
  }
  assert {
    condition     = one(aws_cloudfront_distribution.place_live[0].default_cache_behavior).allowed_methods == toset(["GET", "HEAD"]) && one(one(aws_cloudfront_distribution.place_live[0].default_cache_behavior).function_association).event_type == "viewer-request"
    error_message = "Only GET and HEAD, through the viewer function that refuses every other path."
  }
}

run "the_live_canvas_cdn_can_be_turned_off" {
  command = plan

  variables {
    place_live_cdn = false
  }

  assert {
    condition     = length(aws_cloudfront_distribution.place_live) == 0 && output.place_live_canvas_url == null
    error_message = "place_live_cdn = false removes the distribution."
  }
}

run "place_cannot_take_aspens_hostname" {
  command = plan

  variables {
    place_hostname = "git.staging.aspen.example.test"
  }

  expect_failures = [var.place_hostname]
}

run "place_must_stay_under_the_base_domain" {
  command = plan

  variables {
    place_hostname = "place.elsewhere.test"
  }

  expect_failures = [var.place_hostname]
}

run "place_runs_as_exactly_one_instance" {
  command = apply

  assert {
    condition     = aws_ecs_service.place[0].desired_count == 1
    error_message = "place runs one task."
  }
  assert {
    condition     = aws_ecs_service.place[0].deployment_maximum_percent == 100 && aws_ecs_service.place[0].deployment_minimum_healthy_percent == 0
    error_message = "A deploy must stop the old place task before starting the new one: two at once would run two merger bots on two in-memory queues."
  }
  assert {
    condition     = aws_ecs_service.place[0].launch_type == "FARGATE" && length(aws_ecs_service.place[0].capacity_provider_strategy) == 0
    error_message = "place must not run on Spot: an interruption loses unmerged pushes."
  }
  assert {
    condition     = jsondecode(aws_ecs_task_definition.place[0].container_definitions)[1].stopTimeout == 120
    error_message = "SIGTERM must leave time for the drain and the last merge."
  }
}

run "place_task_gets_its_own_database_and_bucket_and_no_password_in_plain_text" {
  command = apply

  assert {
    condition = (
      { for e in jsondecode(aws_ecs_task_definition.place[0].container_definitions)[1].environment : e.name => e.value }["PLACE_DATABASE_URL"] ==
      "postgres://place@aspen-staging.abc.us-east-1.rds.amazonaws.com:5432/place?sslmode=require"
    )
    error_message = "place must log in as its own role, `place`, to its own database, `place`, without a password in the URL."
  }
  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.place[0].container_definitions)[1].secrets ==
      [{ name = "PGPASSWORD", valueFrom = aws_secretsmanager_secret.place_db_password[0].arn }]
    )
    error_message = "place's database password must arrive as a secret: its own role's, nothing else."
  }
  assert {
    condition = alltrue([
      for s in jsondecode(aws_ecs_task_definition.place[0].container_definitions)[1].secrets : s.valueFrom != module.aspen.database_password_secret_arn
    ]) && aws_secretsmanager_secret.place_db_password[0].arn != module.aspen.database_password_secret_arn
    error_message = "The internet-facing place container must never get the RDS master password."
  }
  assert {
    condition = (
      { for s in jsondecode(aws_ecs_task_definition.place[0].container_definitions)[0].secrets : s.name => s.valueFrom } ==
      { PGPASSWORD = module.aspen.database_password_secret_arn, PLACE_PASSWORD = aws_secretsmanager_secret.place_db_password[0].arn }
    )
    error_message = "Only the init container gets the master password, to create the place role with its password."
  }
  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.place[0].container_definitions)[0].command == ["sh", "-c", file("create-database.sh")] &&
      { for e in jsondecode(aws_ecs_task_definition.place[0].container_definitions)[0].environment : e.name => e.value }["ASPEN_DATABASE"] == "aspen"
    )
    error_message = "The init container must run create-database.sh, which keeps place out of Aspen's database."
  }
  assert {
    condition     = aws_secretsmanager_secret_version.place_db_password[0].secret_string_wo_version == 1
    error_message = "place's database password is written with a write-only attribute."
  }
  assert {
    condition = alltrue([
      for c in jsondecode(aws_ecs_task_definition.place[0].container_definitions) : alltrue([
        for e in c.environment : !contains(["PGPASSWORD", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"], e.name)
      ])
    ])
    error_message = "No credentials in plain environment variables."
  }
  assert {
    condition = (
      { for e in jsondecode(aws_ecs_task_definition.place[0].container_definitions)[1].environment : e.name => e.value }["PLACE_BLOB_URL"] == "s3://aspen-staging-place-example" &&
      aws_s3_bucket.place[0].id != module.aspen.pack_bucket
    )
    error_message = "place must store packs in its own bucket, not Aspen's."
  }
  assert {
    condition     = { for e in jsondecode(aws_ecs_task_definition.place[0].container_definitions)[1].environment : e.name => e.value }["PLACE_TRUSTED_PROXY_HOPS"] == "1"
    error_message = "Behind the ALB, place's per-address limits must read the client from X-Forwarded-For."
  }
  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.place[0].container_definitions)[0].name == "create-database" &&
      jsondecode(aws_ecs_task_definition.place[0].container_definitions)[0].essential == false &&
      jsondecode(aws_ecs_task_definition.place[0].container_definitions)[1].dependsOn == [{ containerName = "create-database", condition = "SUCCESS" }]
    )
    error_message = "place must start only after its database exists."
  }
  assert {
    condition     = aws_s3_bucket_public_access_block.place[0].restrict_public_buckets && aws_s3_bucket_public_access_block.place[0].block_public_policy
    error_message = "The place bucket must block public access."
  }
}

run "aspen_leaves_the_budget_to_the_account_and_gets_public_limits" {
  command = apply

  assert {
    condition     = output.placeholder == false
    error_message = "aspen_image should replace the placeholder."
  }
  assert {
    condition     = output.account_budget_usd == 300
    error_message = "The environment should report the account budget it checked."
  }
  assert {
    condition     = module.aspen.budget_names == []
    error_message = "The account stack's budget covers the environment; the module must not add its own."
  }
  assert {
    condition     = output.ssh_host == null
    error_message = "SSH is off unless asked for."
  }
  assert {
    condition     = can(jsondecode(local.aspen_environment.ASPEN_LIMITS).read.per_second) && can(jsondecode(local.aspen_environment.ASPEN_LIMITS).push.per_second)
    error_message = "A public Aspen needs read and push rate limits."
  }
  assert {
    condition     = tonumber(local.aspen_environment.ASPEN_MAX_PUSH_BYTES) * 2 <= module.aspen.ephemeral_storage_gib * 1024 * 1024 * 1024
    error_message = "The spool (the task's ephemeral storage) must hold twice the largest push."
  }
  assert {
    condition     = module.aspen.cdn_log_bucket != null
    error_message = "The CDN's logs are the only record of cache hits, which the budget runbook reads."
  }
}

run "place_can_be_left_out" {
  command = plan

  variables {
    place_image = null
  }

  assert {
    condition     = length(aws_ecs_service.place) == 0 && length(aws_s3_bucket.place) == 0 && length(aws_lb_listener_rule.place) == 0 && output.place_url == null
    error_message = "Without place_image nothing of place is created."
  }
}

run "the_canvas_mirror_is_off_by_default" {
  command = plan

  assert {
    condition     = length(jsondecode(aws_ecs_task_definition.place[0].container_definitions)) == 2 && length(aws_secretsmanager_secret.place_canvas_mirror_key) == 0
    error_message = "Without place_canvas_mirror the place task has no mirror sidecar and no key secret."
  }
}

run "the_canvas_mirror_gets_only_its_key" {
  command = apply

  variables {
    place_canvas_mirror = "example/place-canvas"
  }

  assert {
    condition = (
      jsondecode(aws_ecs_task_definition.place[0].container_definitions)[2].name == "canvas-mirror" &&
      jsondecode(aws_ecs_task_definition.place[0].container_definitions)[2].essential == false &&
      jsondecode(aws_ecs_task_definition.place[0].container_definitions)[2].secrets ==
      [{ name = "MIRROR_CREDENTIAL", valueFrom = aws_secretsmanager_secret.place_canvas_mirror_key[0].arn }]
    )
    error_message = "The mirror sidecar is not essential and gets its credential and no other secret."
  }
  assert {
    condition = (
      { for e in jsondecode(aws_ecs_task_definition.place[0].container_definitions)[1].environment : e.name => e.value }["PLACE_MIRROR_URL"] ==
      "https://github.com/example/place-canvas"
    )
    error_message = "place names the mirror in the canvas README."
  }
}

run "place_can_be_stopped_without_tearing_it_down" {
  command = plan

  variables {
    place_running = false
  }

  assert {
    condition     = aws_ecs_service.place[0].desired_count == 0
    error_message = "place_running = false should stop the task and keep the rest."
  }
}

run "the_environment_checks_the_named_account_budget" {
  # The rule itself is tested in ../account-budget/tests.
  command = plan

  variables {
    account_budget_name = "org-made-budget"
  }

  assert {
    condition     = module.account_budget.name == "org-made-budget" && output.account_budget_usd == 300
    error_message = "The environment must check the account budget it is given and report its limit."
  }
}

run "only_staging_and_demo" {
  command = plan

  variables {
    environment = "production"
  }

  expect_failures = [var.environment]
}
