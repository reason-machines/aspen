# A VPC of its own: Cloud Run reaches Cloud SQL's private IP through Direct
# VPC egress; nothing else shares the network.

resource "google_compute_network" "aspen" {
  project                 = var.project_id
  name                    = var.name
  auto_create_subnetworks = false

  depends_on = [google_project_service.this]
}

resource "google_compute_subnetwork" "run" {
  project                  = var.project_id
  name                     = "${var.name}-run"
  region                   = var.region
  network                  = google_compute_network.aspen.id
  ip_cidr_range            = "10.42.0.0/24"
  private_ip_google_access = true
}

# Private services access, for Cloud SQL's private IP.
resource "google_compute_global_address" "private_services" {
  project       = var.project_id
  name          = "${var.name}-private-services"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 20
  network       = google_compute_network.aspen.id
}

resource "google_service_networking_connection" "private_services" {
  network                 = google_compute_network.aspen.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_services.name]
}
