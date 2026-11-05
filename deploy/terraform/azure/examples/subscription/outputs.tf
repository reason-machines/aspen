output "url" {
  description = "Base URL for clones."
  value       = module.aspen.url
}

output "admin_command" {
  description = "Run operator commands inside a replica."
  value       = module.aspen.admin_command
}
