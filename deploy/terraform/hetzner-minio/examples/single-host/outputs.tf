output "url" {
  description = "Base URL for clones."
  value       = module.aspen.url
}

output "ipv4" {
  description = "Server IPv4 address."
  value       = module.aspen.ipv4
}

output "ipv6" {
  description = "Server IPv6 address."
  value       = module.aspen.ipv6
}

output "admin_commands" {
  description = "Commands to create a repository and a token."
  value       = module.aspen.admin_commands
}
