# Aspen on AWS: VPC, RDS PostgreSQL, an S3 pack bucket, ECS Fargate behind an
# ALB (HTTPS) and an NLB (SSH), CloudFront in front of the tasks' /-/express
# for pack downloads, ACM, Route53, Secrets Manager, CloudWatch Logs and an
# AWS Budgets alarm.
#
# Deploy this into a dedicated AWS account (preferred) or at least a dedicated
# VPC that shares nothing with other production systems. See deploy/README.md.

data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  partition  = data.aws_partition.current.partition
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.region

  azs = slice(data.aws_availability_zones.available.names, 0, var.az_count)

  ssh_domain_name = coalesce(var.ssh_domain_name, "ssh.${var.domain_name}")
  cdn_domain_name = coalesce(var.cdn_domain_name, "cdn.${var.domain_name}")

  # Until Aspen serves Git, a BusyBox HTTP server stands in for it. It answers
  # /healthz on the HTTP port so the load balancers and ECS deployments behave
  # exactly as they will with the real binary.
  placeholder = var.container_image == null
  image       = local.placeholder ? "public.ecr.aws/docker/library/busybox:1.37@sha256:a7dbc319b1438115a207e11930dc4bb3e643dcb3ec0bdd694bb36cd8fad289ec" : var.container_image
  command = local.placeholder ? [
    "sh", "-c",
    "mkdir -p /tmp/www && echo ok > /tmp/www/healthz && echo 'Aspen placeholder: the Git server is not deployed yet.' > /tmp/www/index.html && exec httpd -f -v -p 0.0.0.0:${var.http_port} -h /tmp/www",
  ] : var.container_command
  # The real image sets its own unprivileged USER; BusyBox defaults to root.
  container_user = local.placeholder ? "65534:65534" : null
}
