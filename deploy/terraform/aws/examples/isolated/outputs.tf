output "http_url" {
  description = "Base URL for Git over HTTPS."
  value       = module.aspen.http_url
}

output "ssh_host" {
  description = "Hostname for Git over SSH."
  value       = module.aspen.ssh_host
}

output "cdn_url" {
  description = "Base URL of the pack CDN."
  value       = module.aspen.cdn_url
}

output "pack_bucket" {
  description = "S3 bucket that stores packs."
  value       = module.aspen.pack_bucket
}

output "ecs_cluster" {
  description = "ECS cluster name."
  value       = module.aspen.ecs_cluster
}

output "ecs_service" {
  description = "ECS service name."
  value       = module.aspen.ecs_service
}

output "log_group" {
  description = "CloudWatch Logs group for Aspen."
  value       = module.aspen.log_group
}

output "placeholder" {
  description = "True while the placeholder runs instead of Aspen."
  value       = module.aspen.placeholder
}
