output "url" {
  description = "Base URL for clones."
  value       = module.aspen.url
}

output "ip_address" {
  description = "Load balancer address."
  value       = module.aspen.ip_address
}
