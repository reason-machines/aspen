output "url" {
  description = "Base URL for clones: <url>/<repo>.git."
  value       = local.url
}

output "ip_address" {
  description = "The load balancer's address; point domain_name here when dns_managed_zone is null."
  value       = google_compute_global_address.lb.address
}

output "service_name" {
  description = "The Cloud Run service (run admin commands with `gcloud run jobs` or a one-off execution of the image)."
  value       = google_cloud_run_v2_service.aspen.name
}

output "bucket" {
  description = "The pack bucket."
  value       = google_storage_bucket.packs.name
}

output "database_connection_name" {
  description = "Cloud SQL connection name."
  value       = google_sql_database_instance.aspen.connection_name
}
