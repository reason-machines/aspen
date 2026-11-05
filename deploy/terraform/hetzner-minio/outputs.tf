output "url" {
  description = "Base URL for clones: <url>/<repo>.git."
  value       = "https://${var.domain_name}"
}

output "ipv4" {
  description = "The server's IPv4 address (point domain_name here when dns_zone is null)."
  value       = hcloud_server.aspen.ipv4_address
}

output "ipv6" {
  description = "The server's IPv6 address."
  value       = hcloud_server.aspen.ipv6_address
}

output "admin_commands" {
  description = "How to create a repository and a token on the server (needs SSH open in admin_cidrs)."
  value       = <<-EOT
    ssh root@${hcloud_server.aspen.ipv4_address} docker compose -f /opt/aspen/compose.yaml exec aspen aspen repo create acme/app
    ssh root@${hcloud_server.aspen.ipv4_address} docker compose -f /opt/aspen/compose.yaml exec aspen aspen token create --subject me --grant write:acme/app
  EOT
}
