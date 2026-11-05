output "http_url" {
  description = "Base URL for Git over HTTPS."
  value       = "https://${var.domain_name}"
}

output "ssh_host" {
  description = "Hostname for Git over SSH, or null when enable_ssh is false."
  value       = var.enable_ssh ? local.ssh_domain_name : null
}

output "cdn_log_bucket" {
  description = "Bucket holding the CDN's CloudFront standard logs (prefix cloudfront/), or null without them."
  value       = local.cdn_logs ? aws_s3_bucket.cdn_logs[0].id : null
}

output "cdn_url" {
  description = "Base URL Aspen signs CDN pack URLs under (https://<cdn_domain_name>/-/express), or null when enable_cdn is false."
  value       = var.enable_cdn ? local.cdn_url : null
}

output "express_url" {
  description = "Base URL of the tasks' own /-/express, or null when enable_express is false."
  value       = var.enable_express ? local.express_url : null
}

output "express_keys_secret_arn" {
  description = "Secrets Manager secret holding ASPEN_EXPRESS_KEYS, or null when enable_express is false."
  value       = var.enable_express ? aws_secretsmanager_secret.express_keys[0].arn : null
}

output "ssh_host_key_secret_arn" {
  description = "Secrets Manager secret holding the SSH host key, or null when enable_ssh is false."
  value       = var.enable_ssh ? aws_secretsmanager_secret.ssh_host_key[0].arn : null
}

output "ssh_host_public_key_parameter" {
  description = "SSM parameter holding the SSH host's public key (for known_hosts), or null when enable_ssh is false."
  value       = var.enable_ssh ? aws_ssm_parameter.ssh_host_public_key[0].name : null
}

output "pack_bucket" {
  description = "Name of the S3 bucket that stores packs."
  value       = aws_s3_bucket.packs.id
}

output "database_endpoint" {
  description = "RDS endpoint (host:port), reachable only from inside the VPC."
  value       = aws_db_instance.this.endpoint
}

output "database_password_secret_arn" {
  description = "Secrets Manager secret holding the database password."
  value       = aws_secretsmanager_secret.db_password.arn
}

output "ecs_cluster" {
  description = "ECS cluster name."
  value       = aws_ecs_cluster.this.name
}

output "ecs_service" {
  description = "ECS service name."
  value       = aws_ecs_service.aspen.name
}

output "log_group" {
  description = "CloudWatch Logs group for Aspen."
  value       = aws_cloudwatch_log_group.aspen.name
}

output "task_role_arn" {
  description = "IAM role Aspen runs as."
  value       = aws_iam_role.task.arn
}

output "alb_dns_name" {
  description = "DNS name of the HTTPS load balancer."
  value       = aws_lb.http.dns_name
}

output "nlb_dns_name" {
  description = "DNS name of the SSH load balancer, or null when enable_ssh is false."
  value       = var.enable_ssh ? aws_lb.ssh[0].dns_name : null
}

output "vpc_id" {
  description = "ID of the dedicated VPC."
  value       = aws_vpc.this.id
}

output "placeholder" {
  description = "True while the service runs the placeholder instead of a real Aspen image."
  value       = local.placeholder
}

# For resources that share this deployment's network, load balancer and
# database (for example the place demo in deploy/terraform/aws/public-demo).

output "region" {
  description = "Region the deployment runs in."
  value       = local.region
}

output "ecs_cluster_arn" {
  description = "ECS cluster ARN."
  value       = aws_ecs_cluster.this.arn
}

output "task_subnet_ids" {
  description = "Subnets the tasks run in (public without a NAT gateway, private with one)."
  value       = var.nat_gateway ? aws_subnet.private[*].id : aws_subnet.public[*].id
}

output "tasks_have_public_ip" {
  description = "Whether tasks get a public IP (true without a NAT gateway)."
  value       = !var.nat_gateway
}

output "https_listener_arn" {
  description = "ARN of the ALB's HTTPS listener, for extra host rules and certificates."
  value       = aws_lb_listener.https.arn
}

output "alb_security_group_id" {
  description = "Security group of the ALB."
  value       = aws_security_group.alb.id
}

output "alb_zone_id" {
  description = "Route53 zone ID of the ALB, for alias records."
  value       = aws_lb.http.zone_id
}

output "database_security_group_id" {
  description = "Security group of the RDS instance."
  value       = aws_security_group.db.id
}

output "database_address" {
  description = "RDS hostname, reachable only from inside the VPC."
  value       = aws_db_instance.this.address
}

output "database_port" {
  description = "RDS port."
  value       = aws_db_instance.this.port
}

output "database_username" {
  description = "RDS master user name (its password is in database_password_secret_arn)."
  value       = aws_db_instance.this.username
}

output "database_name" {
  description = "Name of Aspen's database on the RDS instance."
  value       = aws_db_instance.this.db_name
}

output "ephemeral_storage_gib" {
  description = "Fargate ephemeral storage per Aspen task in GiB; the push spool lives there."
  value       = var.ephemeral_storage_gib
}

output "budget_names" {
  description = "Names of the AWS Budgets this module created (none when budget_enabled is false)."
  value       = concat(aws_budgets_budget.total[*].name, aws_budgets_budget.cdn[*].name)
}
