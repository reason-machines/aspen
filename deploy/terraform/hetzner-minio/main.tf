# Aspen on one Hetzner Cloud server: Aspen, PostgreSQL and MinIO under
# docker compose, behind Caddy for TLS, with every byte of state on a
# Hetzner volume. Secrets (database, MinIO) are generated on the server at
# first boot and never pass through Terraform.

locals {
  labels = merge({ "app" = "aspen", "managed-by" = "terraform" }, var.labels)

  # The record name inside dns_zone ("@" for the zone apex).
  dns_name = var.dns_zone == null ? null : (
    var.domain_name == var.dns_zone ? "@" : trimsuffix(var.domain_name, ".${var.dns_zone}")
  )

  # Where the data volume is mounted. Everything Aspen keeps lives here.
  data_dir = "/srv/aspen"

  compose = templatefile("${path.module}/templates/compose.yaml.tftpl", {
    aspen_image    = var.aspen_image
    postgres_image = var.postgres_image
    minio_image    = var.minio_image
    caddy_image    = var.caddy_image
    limits_json    = var.aspen_limits_json
    domain_name    = var.domain_name
    data_dir       = local.data_dir
  })

  caddyfile = templatefile("${path.module}/templates/Caddyfile.tftpl", {
    domain_name = var.domain_name
    acme_email  = var.acme_email
  })

  user_data = templatefile("${path.module}/templates/cloud-init.yaml.tftpl", {
    volume_device = "/dev/disk/by-id/scsi-0HC_Volume_${hcloud_volume.data.id}"
    data_dir      = local.data_dir
    compose       = local.compose
    caddyfile     = local.caddyfile
    policy_json   = var.aspen_policy_json
    # Shared with deploy/compose so both paths set the bucket up the same way.
    minio_init = file("${path.module}/../../compose/minio-init.sh")
    init_env   = file("${path.module}/templates/init-env.sh")
  })
}

resource "hcloud_ssh_key" "admin" {
  count = length(var.admin_ssh_public_keys)

  name       = "${var.name}-admin-${count.index}"
  public_key = var.admin_ssh_public_keys[count.index]
  labels     = local.labels
}

# Only HTTP(S), and SSH from the admin ranges. PostgreSQL, MinIO and Aspen's
# own port are reachable only inside the compose network.
resource "hcloud_firewall" "aspen" {
  name   = var.name
  labels = local.labels

  dynamic "rule" {
    for_each = length(var.admin_cidrs) > 0 ? [1] : []

    content {
      description = "SSH from admin ranges"
      direction   = "in"
      protocol    = "tcp"
      port        = "22"
      source_ips  = var.admin_cidrs
    }
  }

  rule {
    description = "HTTP (redirects to HTTPS; ACME challenges)"
    direction   = "in"
    protocol    = "tcp"
    port        = "80"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }

  rule {
    description = "HTTPS (Git over HTTP)"
    direction   = "in"
    protocol    = "tcp"
    port        = "443"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_volume" "data" {
  name              = "${var.name}-data"
  location          = var.location
  size              = var.data_volume_size_gb
  format            = "ext4"
  delete_protection = var.protect
  labels            = local.labels
}

resource "hcloud_server" "aspen" {
  name               = var.name
  server_type        = var.server_type
  image              = var.server_image
  location           = var.location
  ssh_keys           = hcloud_ssh_key.admin[*].id
  firewall_ids       = [hcloud_firewall.aspen.id]
  user_data          = local.user_data
  delete_protection  = var.protect
  rebuild_protection = var.protect
  labels             = local.labels

  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }

  lifecycle {
    # cloud-init runs once; a changed template must not silently replace
    # the server. Apply template changes by hand, or taint the server.
    ignore_changes = [user_data]
  }
}

resource "hcloud_volume_attachment" "data" {
  volume_id = hcloud_volume.data.id
  server_id = hcloud_server.aspen.id
  automount = false
}

resource "hcloud_zone_rrset" "a" {
  count = var.dns_zone == null ? 0 : 1

  zone    = var.dns_zone
  name    = local.dns_name
  type    = "A"
  ttl     = 300
  records = [{ value = hcloud_server.aspen.ipv4_address }]
}

resource "hcloud_zone_rrset" "aaaa" {
  count = var.dns_zone == null ? 0 : 1

  zone    = var.dns_zone
  name    = local.dns_name
  type    = "AAAA"
  ttl     = 300
  records = [{ value = hcloud_server.aspen.ipv6_address }]
}
