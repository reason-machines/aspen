# Tests with mocked providers: no AWS credentials, no API calls. `apply`
# runs only "apply" against the mocks.
#
#   cd deploy/terraform/aws
#   terraform init -backend=false
#   terraform test

mock_provider "aws" {
  mock_data "aws_availability_zones" {
    defaults = { names = ["xx-test-1a", "xx-test-1b", "xx-test-1c"] }
  }
  mock_data "aws_region" {
    defaults = { region = "xx-test-1" }
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
  mock_resource "aws_cloudfront_distribution" {
    defaults = { arn = "arn:aws:cloudfront::111111111111:distribution/EXAMPLE" }
  }
  mock_resource "aws_cloudfront_function" {
    defaults = { arn = "arn:aws:cloudfront::111111111111:function/aspen-express-viewer-request" }
  }
  mock_resource "aws_ssm_parameter" {
    defaults = { arn = "arn:aws:ssm:xx-test-1:111111111111:parameter/aspen/ssh-host-public-key" }
  }
  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:xx-test-1:111111111111:certificate/00000000-0000-0000-0000-000000000000"
      domain_validation_options = [{
        domain_name           = "git.example.test"
        resource_record_name  = "_x.git.example.test."
        resource_record_type  = "CNAME"
        resource_record_value = "_y.acm-validations.aws."
      }]
    }
  }
  mock_resource "aws_lb" {
    defaults = { arn = "arn:aws:elasticloadbalancing:xx-test-1:111111111111:loadbalancer/app/aspen/0123456789abcdef" }
  }
  mock_resource "aws_lb_target_group" {
    defaults = { arn = "arn:aws:elasticloadbalancing:xx-test-1:111111111111:targetgroup/aspen/0123456789abcdef" }
  }
  mock_resource "aws_iam_role" {
    defaults = { arn = "arn:aws:iam::111111111111:role/aspen" }
  }
  mock_resource "aws_cloudwatch_log_group" {
    defaults = { arn = "arn:aws:logs:xx-test-1:111111111111:log-group:/aspen/aspen" }
  }
  mock_resource "aws_secretsmanager_secret" {
    defaults = { arn = "arn:aws:secretsmanager:xx-test-1:111111111111:secret:aspen-db-password-abcdef" }
  }
  mock_resource "aws_ecs_task_definition" {
    defaults = { arn = "arn:aws:ecs:xx-test-1:111111111111:task-definition/aspen:1" }
  }
  mock_resource "aws_ecs_cluster" {
    defaults = { arn = "arn:aws:ecs:xx-test-1:111111111111:cluster/aspen", id = "arn:aws:ecs:xx-test-1:111111111111:cluster/aspen" }
  }
  mock_resource "aws_s3_bucket" {
    defaults = { arn = "arn:aws:s3:::aspen-packs-example", id = "aspen-packs-example" }
  }
}

mock_provider "aws" {
  alias = "us_east_1"

  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:us-east-1:111111111111:certificate/00000000-0000-0000-0000-000000000001"
      domain_validation_options = [{
        domain_name           = "cdn.git.example.test"
        resource_record_name  = "_x.cdn.git.example.test."
        resource_record_type  = "CNAME"
        resource_record_value = "_y.acm-validations.aws."
      }]
    }
  }
}

mock_provider "random" {}
mock_provider "tls" {}

variables {
  domain_name         = "git.example.test"
  route53_zone_id     = "ZEXAMPLE"
  budget_alert_emails = ["ops@example.test"]
}

run "defaults_are_cheap_and_private" {
  command = plan

  # No NAT gateway by default; tasks run in public subnets with a public IP.
  assert {
    condition     = length(aws_nat_gateway.this) == 0
    error_message = "No NAT gateway should be created by default."
  }
  assert {
    condition     = aws_ecs_service.aspen.network_configuration[0].assign_public_ip
    error_message = "Without a NAT gateway, tasks need a public IP to reach AWS APIs."
  }

  assert {
    condition     = one(aws_db_parameter_group.this.parameter).name == "rds.force_ssl" && one(aws_db_parameter_group.this.parameter).value == "1" && one(aws_db_parameter_group.this.parameter).apply_method == "pending-reboot"
    error_message = "RDS must require SSL with pending-reboot application, matching the live parameter group."
  }

  # The database is private and encrypted.
  assert {
    condition     = aws_db_instance.this.publicly_accessible == false && aws_db_instance.this.storage_encrypted
    error_message = "RDS must be private and encrypted."
  }
  assert {
    condition     = aws_db_instance.this.instance_class == "db.t4g.micro" && aws_db_instance.this.multi_az == false
    error_message = "The default database should be the small single-AZ class."
  }
  assert {
    condition     = aws_db_instance.this.apply_immediately == false
    error_message = "By default database changes wait for the maintenance window."
  }
  assert {
    condition     = output.database_size.iops == null && output.database_size.storage_throughput == null
    error_message = "Below 400 GiB RDS fixes gp3 at 3,000 IOPS and refuses a value, so the module must not send one."
  }

  # The bucket is locked down and cleans up interrupted multipart uploads.
  assert {
    condition     = aws_s3_bucket_public_access_block.packs.block_public_policy && aws_s3_bucket_public_access_block.packs.restrict_public_buckets
    error_message = "The pack bucket must block public access."
  }
  assert {
    condition     = aws_s3_bucket_versioning.packs.versioning_configuration[0].status == "Enabled"
    error_message = "The pack bucket must be versioned."
  }
  assert {
    condition     = one([for r in aws_s3_bucket_lifecycle_configuration.packs.rule : r.abort_incomplete_multipart_upload[0].days_after_initiation if length(r.abort_incomplete_multipart_upload) > 0]) == 1
    error_message = "Incomplete multipart uploads must be aborted after a day."
  }

  # The budget alarm defaults to $300/month and alerts on actual and forecast.
  assert {
    condition     = aws_budgets_budget.total[0].limit_amount == "300.00" && aws_budgets_budget.total[0].time_unit == "MONTHLY"
    error_message = "The default budget must be $300/month."
  }
  assert {
    condition     = length(aws_budgets_budget.total[0].notification) == 3
    error_message = "The budget needs 80%/100% actual and 100% forecast notifications."
  }

  # SSH and the CDN are wired by default.
  assert {
    condition     = length(aws_lb.ssh) == 1 && aws_lb_listener.ssh[0].port == 22
    error_message = "The SSH NLB should listen on port 22."
  }

  # Aspen's SSH limits key on the TCP peer (docs/SSH.md, admission), so the
  # NLB must hand tasks the client's own address: with client IP preservation
  # off, every client would share the NLB's limits (issue #120). Aspen does
  # not parse proxy protocol, so it must stay off. The tasks still admit SSH
  # only from the NLB's security group (run "tasks_admit_only_the_load_balancers"),
  # which AWS matches with client IP preservation on.
  assert {
    condition     = aws_lb_target_group.ssh[0].preserve_client_ip == "true" && aws_lb_target_group.ssh[0].proxy_protocol_v2 == false
    error_message = "The SSH target group must preserve client addresses, without proxy protocol."
  }

  # The CDN's origin is the tasks' /-/express on the Git hostname, never the
  # bucket, and nothing but the path reaches the cache key or the origin.
  assert {
    condition     = length(aws_cloudfront_distribution.packs) == 1 && one(aws_cloudfront_distribution.packs[0].origin).domain_name == "git.example.test"
    error_message = "The CDN's origin must be the Git hostname, which serves /-/express."
  }
  assert {
    condition     = one(one(aws_cloudfront_distribution.packs[0].origin).custom_origin_config).origin_protocol_policy == "https-only" && one(aws_cloudfront_distribution.packs[0].origin).origin_access_control_id == null
    error_message = "CloudFront must reach express over HTTPS, and must not read the bucket."
  }
  assert {
    condition = alltrue([
      for p in aws_cloudfront_cache_policy.express[0].parameters_in_cache_key_and_forwarded_to_origin : (
        p.cookies_config[0].cookie_behavior == "none" &&
        p.headers_config[0].header_behavior == "none" &&
        p.query_strings_config[0].query_string_behavior == "none"
      )
    ])
    error_message = "The CDN cache key must be the path alone, with no cookies, headers or query forwarded."
  }
  assert {
    condition     = aws_cloudfront_cache_policy.express[0].max_ttl == 3600 && aws_cloudfront_cache_policy.express[0].default_ttl == 0
    error_message = "CloudFront must follow the origin's Cache-Control, at most an express URL's longest life."
  }

  # Expired URLs are refused at the edge, before the cache: CloudFront
  # would otherwise serve an expired pack while the origin errors.
  assert {
    condition = (
      one(aws_cloudfront_distribution.packs[0].default_cache_behavior[0].function_association).event_type == "viewer-request" &&
      aws_cloudfront_function.express[0].code == file("cdn/viewer-request.js")
    )
    error_message = "Every CDN request must pass the viewer-request function that refuses expired URLs."
  }

  # The bucket policy only refuses plain HTTP; no other principal is let in.
  assert {
    condition     = length(data.aws_iam_policy_document.packs_bucket.statement) == 1 && data.aws_iam_policy_document.packs_bucket.statement[0].effect == "Deny"
    error_message = "The pack bucket must stay private to the task role."
  }

  # Operators run `aspen repo create` and friends with ECS Exec.
  assert {
    condition     = aws_ecs_service.aspen.enable_execute_command && length(aws_iam_role_policy.task_exec) == 1
    error_message = "ECS Exec should be on by default, with the task role's SSM channel permissions."
  }
  assert {
    condition     = length(aws_ecs_task_definition.aspen.ephemeral_storage) == 0
    error_message = "The default ephemeral storage (20 GiB) needs no override."
  }
}

# Growing an existing 20 GiB database to 400 GiB failed in the live demo with
# "Invalid iops value for engine name postgres and storage type gp3: 3000":
# the provider re-sent the old IOPS. From 400 GiB the module names gp3's
# baseline itself.
run "gp3_from_400_gib_names_its_baseline_iops_and_throughput" {
  command = plan

  variables {
    db_allocated_storage     = 400
    db_max_allocated_storage = 1000
  }

  assert {
    condition     = aws_db_instance.this.iops == 12000 && aws_db_instance.this.storage_throughput == 500
    error_message = "From 400 GiB, gp3 must be sent with its 12,000 IOPS and 500 MiB/s baseline."
  }
  assert {
    condition     = output.database_size.iops == 12000 && output.database_size.storage_throughput == 500
    error_message = "database_size must report the IOPS and throughput sent."
  }
}

run "placeholder_until_an_image_is_set" {
  # apply against the mocks, so the rendered container definition is known.
  command = apply

  assert {
    condition     = output.placeholder
    error_message = "Without container_image the placeholder should run."
  }
  assert {
    condition     = jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].image == "public.ecr.aws/docker/library/busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec"
    error_message = "The placeholder image should be BusyBox from ECR Public, pinned by digest."
  }
  assert {
    condition     = jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].user == "65534:65534"
    error_message = "The placeholder must not run as root."
  }
}

run "real_image_gets_the_config_contract" {
  # apply against the mocks, so the rendered container definition is known.
  command = apply

  variables {
    container_image = "registry.example.test/aspen:1.2.3"
  }

  assert {
    condition     = output.placeholder == false
    error_message = "Setting container_image should replace the placeholder."
  }

  # Generated secrets are written with write-only attributes, never kept in
  # state.
  assert {
    condition = (
      aws_secretsmanager_secret_version.express_keys[0].secret_string_wo_version == 1 &&
      aws_secretsmanager_secret_version.ssh_host_key[0].secret_string_wo_version == 1 &&
      aws_ssm_parameter.ssh_host_public_key[0].value_wo_version == 1
    )
    error_message = "Express keys and the SSH host key must not be stored in Terraform state."
  }

  assert {
    condition     = jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].image == "registry.example.test/aspen:1.2.3"
    error_message = "The task should run the configured image."
  }

  assert {
    condition = alltrue([
      for name in ["ASPEN_LISTEN", "ASPEN_SSH_LISTEN", "ASPEN_PUBLIC_URL", "ASPEN_BLOB_URL", "DATABASE_URL", "AWS_REGION", "ASPEN_CDN_URL", "ASPEN_EXPRESS_URLS"] :
      contains([for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment : e.name], name)
    ])
    error_message = "The task is missing part of the configuration contract."
  }

  assert {
    condition = contains([
      for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment :
      e.value if e.name == "ASPEN_TRUSTED_PROXY_HOPS"
    ], "1")
    error_message = "Behind the ALB, anonymous rate limits must key on the X-Forwarded-For entry the ALB appends (ASPEN_TRUSTED_PROXY_HOPS = 1)."
  }

  # Issue #125: read and push requests are rate limited by default.
  assert {
    condition = alltrue([
      for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment :
      jsondecode(e.value).read.per_second > 0 && jsondecode(e.value).push.per_second > 0 if e.name == "ASPEN_LIMITS"
    ]) && contains([for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment : e.name], "ASPEN_LIMITS")
    error_message = "ASPEN_LIMITS limits read and push requests per caller by default."
  }

  assert {
    condition     = [for s in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].secrets : s.name] == ["ASPEN_EXPRESS_KEYS", "ASPEN_SSH_HOST_KEY", "PGPASSWORD"]
    error_message = "The database password, the express keys and the SSH host key must arrive as secrets, and nothing else by default."
  }

  assert {
    condition = (
      { for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment : e.name => e.value if startswith(e.name, "ASPEN_CDN") || startswith(e.name, "ASPEN_EXPRESS") } ==
      {
        ASPEN_CDN_URL       = "https://cdn.git.example.test/-/express"
        ASPEN_CDN_PAST_END  = "416"
        ASPEN_EXPRESS_URLS  = "https://git.example.test/-/express"
        ASPEN_EXPRESS_SERVE = "true"
      }
    )
    error_message = "Packfile URIs: CloudFront over /-/express for git 2.56+ (past end answered 416), the tasks' /-/express for everyone else, served by every task."
  }

  assert {
    condition = alltrue([
      for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment :
      !contains(["ASPEN_CDN_KEY_PAIR_ID", "ASPEN_CDN_PRIVATE_KEY", "RUST_LOG"], e.name)
    ])
    error_message = "Superseded settings (CloudFront key pairs, RUST_LOG) must not be set."
  }

  assert {
    condition = alltrue([
      for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment :
      !contains(["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "PGPASSWORD"], e.name)
    ])
    error_message = "No credentials may be passed as plain environment variables."
  }

  assert {
    condition     = !can(jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].user)
    error_message = "A real image should run as the user its Dockerfile sets."
  }
}

run "a_compaction_service_takes_the_background_worker" {
  command = apply

  variables {
    container_image       = "registry.example.test/aspen:1.2.3"
    compactor_count       = 2
    compactor_environment = { ASPEN_COMPACT_WORKERS = "16" }
  }

  assert {
    condition     = length(aws_ecs_service.compactor) == 1 && aws_ecs_service.compactor[0].desired_count == 2
    error_message = "compactor_count tasks of a separate compaction service should run."
  }
  assert {
    condition     = length(aws_ecs_service.compactor[0].load_balancer) == 0
    error_message = "The compaction service serves no Git, so no load balancer may reach it."
  }
  assert {
    condition = contains([
      for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment :
      e.value if e.name == "ASPEN_ROLE"
    ], "serve")
    error_message = "With a compaction service, the Git tasks run ASPEN_ROLE=serve."
  }
  assert {
    condition = (
      { for e in jsondecode(aws_ecs_task_definition.compactor[0].container_definitions)[0].environment : e.name => e.value if contains(["ASPEN_ROLE", "ASPEN_COMPACT_WORKERS", "ASPEN_BLOB_URL"], e.name) } ==
      { ASPEN_ROLE = "compact", ASPEN_COMPACT_WORKERS = "16", ASPEN_BLOB_URL = "s3://aspen-packs-example" }
    )
    error_message = "The compaction tasks run ASPEN_ROLE=compact on the same bucket, with their own environment."
  }
  assert {
    condition = (
      [for e in jsondecode(aws_ecs_task_definition.compactor[0].container_definitions)[0].environment : e.value if e.name == "DATABASE_URL"] ==
      [for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment : e.value if e.name == "DATABASE_URL"]
    )
    error_message = "The compaction tasks share the Git tasks' database."
  }
  assert {
    condition = alltrue([
      for e in jsondecode(aws_ecs_task_definition.compactor[0].container_definitions)[0].environment :
      !contains(["ASPEN_SSH_LISTEN", "ASPEN_MERGE_QUEUE", "ASPEN_EXPRESS_URLS", "ASPEN_LIMITS", "PGPASSWORD"], e.name)
    ])
    error_message = "The compaction tasks get no Git-only settings and no plain credentials."
  }
  assert {
    condition     = [for s in jsondecode(aws_ecs_task_definition.compactor[0].container_definitions)[0].secrets : s.name] == ["PGPASSWORD"]
    error_message = "The compaction tasks need only the database password."
  }
}

run "the_background_worker_runs_in_the_git_tasks_by_default" {
  command = apply

  variables {
    container_image = "registry.example.test/aspen:1.2.3"
  }

  assert {
    condition = length(aws_ecs_service.compactor) == 0 && !contains(
      [for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment : e.name], "ASPEN_ROLE"
    )
    error_message = "Without compactor_count, no compaction service runs and the Git tasks run the background worker (ASPEN_ROLE unset: all)."
  }
}

run "nat_gateway_moves_tasks_to_private_subnets" {
  command = plan

  variables {
    nat_gateway = true
  }

  assert {
    condition     = length(aws_nat_gateway.this) == 1
    error_message = "nat_gateway = true should create exactly one NAT gateway."
  }
  assert {
    condition     = aws_ecs_service.aspen.network_configuration[0].assign_public_ip == false
    error_message = "Tasks behind a NAT gateway must not get public IPs."
  }
}

run "ssh_and_cdn_can_be_turned_off" {
  # apply against the mocks, so the rendered container definition is known.
  command = apply

  variables {
    container_image = "registry.example.test/aspen:1.2.3"
    enable_ssh      = false
    enable_cdn      = false
  }

  assert {
    condition     = length(aws_lb.ssh) == 0 && length(aws_lb_target_group.ssh) == 0
    error_message = "enable_ssh = false should remove the NLB."
  }
  assert {
    condition     = length(aws_cloudfront_distribution.packs) == 0 && length(aws_budgets_budget.cdn) == 0 && length(aws_cloudfront_function.express) == 0
    error_message = "enable_cdn = false should remove the distribution, its function and its budget."
  }
  assert {
    condition     = length(aws_s3_bucket.cdn_logs) == 0 && output.cdn_log_bucket == null
    error_message = "enable_cdn = false should remove the CDN's log bucket."
  }
  assert {
    condition     = length(aws_ecs_service.aspen.load_balancer) == 1
    error_message = "Without SSH the service should register with the ALB only."
  }
  assert {
    condition     = length(aws_secretsmanager_secret.ssh_host_key) == 0 && length(aws_ssm_parameter.ssh_host_public_key) == 0
    error_message = "Without SSH there is no host key."
  }
  # aspen serve refuses ASPEN_SSH_LISTEN without a host key, and CDN
  # settings without a CDN would send clients to a host that does not exist.
  assert {
    condition = alltrue([
      for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment :
      !contains(["ASPEN_SSH_LISTEN", "ASPEN_SSH_PUBLIC_HOST", "ASPEN_CDN_URL", "ASPEN_CDN_PAST_END"], e.name)
    ])
    error_message = "SSH and CDN settings must be left out when SSH and the CDN are off."
  }
  assert {
    condition     = [for s in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].secrets : s.name] == ["ASPEN_EXPRESS_KEYS", "PGPASSWORD"]
    error_message = "Express still runs on the tasks without the CDN."
  }
}

run "express_can_be_turned_off" {
  command = apply

  variables {
    container_image = "registry.example.test/aspen:1.2.3"
    enable_express  = false
    enable_cdn      = false
  }

  assert {
    condition     = length(aws_secretsmanager_secret.express_keys) == 0
    error_message = "enable_express = false should not create signing keys."
  }
  assert {
    condition = alltrue([
      for e in jsondecode(aws_ecs_task_definition.aspen.container_definitions)[0].environment :
      !startswith(e.name, "ASPEN_EXPRESS") && !startswith(e.name, "ASPEN_CDN")
    ])
    error_message = "Without express no express or CDN settings may be set."
  }
}

run "cdn_requires_express" {
  command = plan

  variables {
    enable_express = false
  }

  expect_failures = [var.enable_cdn]
}

run "spool_disk_and_exec_are_configurable" {
  command = plan

  variables {
    ephemeral_storage_gib = 60
    enable_ecs_exec       = false
  }

  assert {
    condition     = aws_ecs_task_definition.aspen.ephemeral_storage[0].size_in_gib == 60
    error_message = "ephemeral_storage_gib should size the task's disk."
  }
  assert {
    condition     = aws_ecs_service.aspen.enable_execute_command == false && length(aws_iam_role_policy.task_exec) == 0
    error_message = "enable_ecs_exec = false should turn ECS Exec off."
  }
}

run "budget_requires_a_recipient" {
  command = plan

  variables {
    budget_alert_emails = []
  }

  expect_failures = [aws_budgets_budget.total]
}

run "budgets_can_be_left_to_the_account" {
  command = plan

  variables {
    budget_enabled      = false
    budget_alert_emails = []
  }

  assert {
    condition     = length(aws_budgets_budget.total) == 0 && length(aws_budgets_budget.cdn) == 0
    error_message = "budget_enabled = false should create no budgets, and need no recipient."
  }
}

# ASPEN_TRUSTED_PROXY_HOPS=1 trusts the last X-Forwarded-For entry, which is
# only right if every HTTP request reaches a task through the ALB.
run "tasks_admit_only_the_load_balancers" {
  command = plan

  variables {
    enable_ssh = true
  }

  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.tasks_from_alb.referenced_security_group_id == aws_security_group.alb.id &&
      aws_vpc_security_group_ingress_rule.tasks_from_alb.cidr_ipv4 == null &&
      aws_vpc_security_group_ingress_rule.tasks_from_alb.cidr_ipv6 == null
    )
    error_message = "Aspen's HTTP port must admit the ALB's security group only."
  }
  assert {
    condition = alltrue([
      for r in aws_vpc_security_group_ingress_rule.tasks_from_nlb :
      r.referenced_security_group_id == aws_security_group.nlb[0].id && r.cidr_ipv4 == null && r.cidr_ipv6 == null
    ])
    error_message = "With SSH on, the tasks admit the NLB's security group only."
  }
}

run "ecs_exec_sessions_are_not_logged" {
  command = plan

  assert {
    condition     = one(one(aws_ecs_cluster.this.configuration).execute_command_configuration).logging == "NONE"
    error_message = "ECS Exec output holds new tokens (aspen token create); it must not be logged. CloudTrail records the calls."
  }
}

run "cdn_logs_go_to_a_short_lived_private_bucket" {
  command = apply

  assert {
    condition     = one(aws_cloudfront_distribution.packs[0].logging_config).bucket == aws_s3_bucket.cdn_logs[0].bucket_domain_name
    error_message = "The CDN must log to its own bucket: cache hits never reach the origin's logs."
  }
  assert {
    condition     = one(aws_s3_bucket_lifecycle_configuration.cdn_logs[0].rule).expiration[0].days == 7
    error_message = "CDN logs are kept a week by default."
  }
  assert {
    condition     = aws_s3_bucket_public_access_block.cdn_logs[0].block_public_acls && aws_s3_bucket_public_access_block.cdn_logs[0].restrict_public_buckets
    error_message = "The CDN log bucket must block public access."
  }
  assert {
    condition     = one(aws_s3_bucket_ownership_controls.cdn_logs[0].rule).object_ownership == "BucketOwnerPreferred"
    error_message = "CloudFront standard logs need ACLs on their bucket."
  }
}

run "cdn_logs_can_be_turned_off" {
  command = plan

  variables {
    cdn_log_retention_days = 0
  }

  assert {
    condition     = length(aws_s3_bucket.cdn_logs) == 0 && length(aws_cloudfront_distribution.packs[0].logging_config) == 0
    error_message = "cdn_log_retention_days = 0 should turn the CDN's logs off."
  }
}

run "unpinned_images_are_refused" {
  command = plan

  variables {
    container_image = "registry.example.test/aspen:latest"
  }

  expect_failures = [var.container_image]
}

run "untagged_images_are_refused" {
  command = plan

  variables {
    container_image = "registry.example.test/aspen"
  }

  expect_failures = [var.container_image]
}

run "digest_pinned_images_are_accepted" {
  command = plan

  variables {
    container_image = "registry.example.test/aspen@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  }

  assert {
    condition     = local.image == "registry.example.test/aspen@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    error_message = "A digest-pinned image runs as given."
  }
}
