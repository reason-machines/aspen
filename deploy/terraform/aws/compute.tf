# ECS Fargate service running Aspen, its IAM roles and its log group.

resource "aws_cloudwatch_log_group" "aspen" {
  name              = "/aspen/${var.name}"
  retention_in_days = var.log_retention_days
}

resource "aws_ecs_cluster" "this" {
  name = var.name

  setting {
    name  = "containerInsights"
    value = var.container_insights
  }

  # ECS Exec sessions are not logged, for every task on the cluster (the
  # place demo's too). Their output is where `aspen token create` and
  # `aspen-place operator-token` print new secrets, and a log would keep
  # them. CloudTrail records every ExecuteCommand call (who, which task,
  # which command) without the output.
  configuration {
    execute_command_configuration {
      logging = "NONE"
    }
  }
}

resource "aws_ecs_cluster_capacity_providers" "this" {
  cluster_name       = aws_ecs_cluster.this.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]
}

locals {
  public_url = "https://${var.domain_name}"

  # Packfile URIs (docs/EXPRESS.md). Every task mounts express at
  # /-/express on the Git listener and signs URLs with ASPEN_EXPRESS_KEYS.
  # Two tiers: CloudFront (whose origin is that same /-/express, for git
  # 2.56+ only, since CloudFront answers 416 past the end of a cached pack),
  # then the servers' own /-/express, which every client can use.
  express_url = "${local.public_url}/-/express"
  cdn_url     = var.enable_cdn ? "https://${local.cdn_domain_name}/-/express" : ""

  # The configuration contract Aspen reads (deploy/README.md). The compose
  # stack sets the same names. No AWS keys: the SDK picks up the task role.
  default_environment = {
    ASPEN_LISTEN          = "0.0.0.0:${var.http_port}"
    ASPEN_SSH_LISTEN      = var.enable_ssh ? "0.0.0.0:${var.ssh_port}" : ""
    ASPEN_PUBLIC_URL      = local.public_url
    ASPEN_SSH_PUBLIC_HOST = var.enable_ssh ? local.ssh_domain_name : ""
    DATABASE_URL          = "postgres://${aws_db_instance.this.username}@${aws_db_instance.this.address}:${aws_db_instance.this.port}/${aws_db_instance.this.db_name}?sslmode=require"
    ASPEN_BLOB_URL        = "s3://${aws_s3_bucket.packs.id}"
    AWS_REGION            = local.region
    # Git over HTTP reaches the tasks through the ALB, which appends the
    # client's address to X-Forwarded-For; without this, anonymous rate
    # limits would key on the ALB node and every caller would share one
    # bucket (decisions log, D4).
    ASPEN_TRUSTED_PROXY_HOPS = "1"
    # Request rates per caller (issue #125): unlimited unless set.
    ASPEN_LIMITS        = var.aspen_limits_json
    ASPEN_EXPRESS_URLS  = var.enable_express ? local.express_url : ""
    ASPEN_EXPRESS_SERVE = var.enable_express ? "true" : ""
    ASPEN_CDN_URL       = local.cdn_url
    ASPEN_CDN_PAST_END  = var.enable_cdn ? "416" : ""
    ASPEN_LOG           = "info"
    ASPEN_LOG_FORMAT    = "json"
  }
  # Unset optional values are left out rather than passed as empty strings.
  environment = { for k, v in merge(local.default_environment, var.container_environment) : k => v if v != "" }

  secrets = merge(
    { PGPASSWORD = aws_secretsmanager_secret.db_password.arn },
    var.enable_express ? { ASPEN_EXPRESS_KEYS = aws_secretsmanager_secret.express_keys[0].arn } : {},
    var.enable_ssh ? { ASPEN_SSH_HOST_KEY = aws_secretsmanager_secret.ssh_host_key[0].arn } : {},
    var.container_secrets,
  )

  container_definition = {
    name      = "aspen"
    image     = local.image
    essential = true
    command   = local.command
    user      = local.container_user

    portMappings = [
      { name = "http", containerPort = var.http_port, protocol = "tcp" },
      { name = "ssh", containerPort = var.ssh_port, protocol = "tcp" },
    ]

    environment = [for k in sort(keys(local.environment)) : { name = k, value = local.environment[k] }]
    secrets     = [for k in sort(keys(local.secrets)) : { name = k, valueFrom = local.secrets[k] }]

    linuxParameters = { initProcessEnabled = true }
    # Fargate's maximum: lets Aspen finish in-flight pushes on SIGTERM.
    stopTimeout = 120

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.aspen.name
        awslogs-region        = local.region
        awslogs-stream-prefix = "aspen"
        mode                  = "non-blocking"
        max-buffer-size       = "4m"
      }
    }
  }
}

resource "aws_ecs_task_definition" "aspen" {
  family                   = var.name
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.container_cpu
  memory                   = var.container_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.cpu_architecture
  }

  # The push spool lives on the task's ephemeral storage (a real disk, as
  # the spool needs); it must hold about twice the largest push.
  dynamic "ephemeral_storage" {
    for_each = var.ephemeral_storage_gib > 20 ? [1] : []

    content {
      size_in_gib = var.ephemeral_storage_gib
    }
  }

  container_definitions = jsonencode([
    { for k, v in local.container_definition : k => v if v != null }
  ])
}

resource "aws_ecs_service" "aspen" {
  name            = var.name
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.aspen.arn
  desired_count   = var.desired_count

  capacity_provider_strategy {
    capacity_provider = var.fargate_spot ? "FARGATE_SPOT" : "FARGATE"
    weight            = 1
  }

  network_configuration {
    subnets          = var.nat_gateway ? aws_subnet.private[*].id : aws_subnet.public[*].id
    security_groups  = [aws_security_group.tasks.id]
    assign_public_ip = !var.nat_gateway
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.http.arn
    container_name   = "aspen"
    container_port   = var.http_port
  }

  dynamic "load_balancer" {
    for_each = var.enable_ssh ? [1] : []

    content {
      target_group_arn = aws_lb_target_group.ssh[0].arn
      container_name   = "aspen"
      container_port   = var.ssh_port
    }
  }

  health_check_grace_period_seconds  = 60
  deployment_minimum_healthy_percent = 100
  deployment_maximum_percent         = 200
  enable_ecs_managed_tags            = true
  propagate_tags                     = "SERVICE"
  # `aws ecs execute-command` runs `aspen repo create`, `aspen token create`
  # and the other operator commands inside a task (deploy/README.md).
  enable_execute_command = var.enable_ecs_exec

  deployment_circuit_breaker {
    enable   = true
    rollback = true
  }

  # Tasks resolve their secrets at start, so each secret needs its value
  # before the first task runs.
  depends_on = [
    aws_ecs_cluster_capacity_providers.this,
    aws_lb_listener.https,
    aws_lb_listener.ssh,
    aws_secretsmanager_secret_version.db_password,
    aws_secretsmanager_secret_version.express_keys,
    aws_secretsmanager_secret_version.ssh_host_key,
  ]
}

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_tasks_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }

    # Only ECS acting for this account may assume the roles.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:${local.partition}:ecs:${local.region}:${local.account_id}:*"]
    }
  }
}

# Execution role: used by the ECS agent to pull the image, write logs and
# resolve secrets into the container's environment.
resource "aws_iam_role" "execution" {
  name_prefix        = "${var.name}-exec-"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "execution" {
  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.aspen.arn}:*"]
  }

  statement {
    sid       = "ReadSecrets"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = distinct([for arn in values(local.secrets) : arn])
  }

  dynamic "statement" {
    for_each = length(var.ecr_repository_arns) > 0 ? [1] : []

    content {
      sid = "PullImage"
      actions = [
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:GetDownloadUrlForLayer",
      ]
      resources = var.ecr_repository_arns
    }
  }

  dynamic "statement" {
    for_each = length(var.ecr_repository_arns) > 0 ? [1] : []

    content {
      sid       = "EcrAuth"
      actions   = ["ecr:GetAuthorizationToken"]
      resources = ["*"] # this action has no resource-level permissions
    }
  }
}

resource "aws_iam_role_policy" "execution" {
  name   = "aspen-execution"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.execution.json
}

# Task role: what Aspen itself may do. Objects in the pack bucket, nothing
# else.
resource "aws_iam_role" "task" {
  name_prefix        = "${var.name}-task-"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_assume.json
}

data "aws_iam_policy_document" "task" {
  statement {
    sid = "PackBucket"
    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
    ]
    resources = [aws_s3_bucket.packs.arn]
  }

  statement {
    sid = "PackObjects"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${aws_s3_bucket.packs.arn}/*"]
  }
}

resource "aws_iam_role_policy" "task" {
  name   = "aspen-task"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task.json
}

# ECS Exec: the SSM agent inside the task opens its channels with the task
# role. Who may run commands is decided by the operator's own IAM
# permissions (ecs:ExecuteCommand), not by this role.
data "aws_iam_policy_document" "task_exec" {
  statement {
    sid = "EcsExec"
    actions = [
      "ssmmessages:CreateControlChannel",
      "ssmmessages:CreateDataChannel",
      "ssmmessages:OpenControlChannel",
      "ssmmessages:OpenDataChannel",
    ]
    resources = ["*"] # these actions have no resource-level permissions
  }
}

resource "aws_iam_role_policy" "task_exec" {
  count = var.enable_ecs_exec ? 1 : 0

  name   = "aspen-ecs-exec"
  role   = aws_iam_role.task.id
  policy = data.aws_iam_policy_document.task_exec.json
}
