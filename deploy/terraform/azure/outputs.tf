output "url" {
  description = "Base URL for clones: <url>/<repo>.git."
  value       = local.public_url
}

output "app_fqdn" {
  description = "The Container App's own host name (CNAME target for domain_name)."
  value       = azurerm_container_app.aspen.ingress[0].fqdn
}

output "admin_command" {
  description = "How to run operator commands inside a replica."
  value       = "az containerapp exec -g ${azurerm_resource_group.aspen.name} -n ${azurerm_container_app.aspen.name} --command \"aspen repo create acme/app\""
}
