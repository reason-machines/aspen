output "git_url" {
  description = "Aspen's base URL for Git over HTTPS."
  value       = module.aspen.http_url
}

output "cdn_url" {
  description = "Base URL CDN pack URLs are signed under, or null without the CDN."
  value       = module.aspen.cdn_url
}

output "ssh_host" {
  description = "Host for Git over SSH, or null when enable_ssh is false."
  value       = module.aspen.ssh_host
}

output "ssh_host_public_key_parameter" {
  description = "SSM parameter with the SSH host's public key, for publishing the known_hosts line."
  value       = module.aspen.ssh_host_public_key_parameter
}

output "place_url" {
  description = "The place page, or null without place_image."
  value       = local.place ? local.place_url : null
}

output "place_clone_url" {
  description = "The canvas repository, or null without place_image."
  value       = local.place ? "${local.place_url}/canvas.git" : null
}

output "ecs_cluster" {
  description = "ECS cluster name, for `aws ecs execute-command`."
  value       = module.aspen.ecs_cluster
}

output "aspen_service" {
  description = "ECS service running Aspen."
  value       = module.aspen.ecs_service
}

output "place_service" {
  description = "ECS service running place, or null without place_image."
  value       = local.place ? aws_ecs_service.place[0].name : null
}

output "log_groups" {
  description = "CloudWatch Logs groups of Aspen and place."
  value       = compact([module.aspen.log_group, local.place ? aws_cloudwatch_log_group.place[0].name : ""])
}

output "placeholder" {
  description = "True while Aspen runs the placeholder instead of a real image."
  value       = module.aspen.placeholder
}

output "account_budget_usd" {
  description = "Monthly limit of the account stack's budget that watches this environment (checked at plan time)."
  value       = module.account_budget.limit_usd
}
