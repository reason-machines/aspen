# Tests with a mocked provider: no Hetzner token, no API calls.
#
#   cd deploy/terraform/hetzner-minio
#   terraform init -backend=false
#   terraform test

# hcloud IDs are numbers; the mocks' random strings would not convert.
mock_provider "hcloud" {
  mock_resource "hcloud_firewall" {
    defaults = { id = "11" }
  }
  mock_resource "hcloud_ssh_key" {
    defaults = { id = "12" }
  }
  mock_resource "hcloud_volume" {
    defaults = { id = "4242" }
  }
  mock_resource "hcloud_server" {
    defaults = { id = "77", ipv4_address = "203.0.113.10", ipv6_address = "2001:db8::1" }
  }
}

variables {
  domain_name           = "git.example.test"
  acme_email            = "ops@example.test"
  aspen_image           = "ghcr.io/reason-machines/aspen:v1.2.3"
  admin_ssh_public_keys = ["ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIExampleKeyOnlyForTests ops@example.test"]
}

run "defaults" {
  command = apply

  # Only 80 and 443 are open; SSH stays closed without admin ranges, and the
  # database, MinIO and Aspen's own port are never exposed.
  assert {
    condition     = length(hcloud_firewall.aspen.rule) == 2
    error_message = "Without admin_cidrs only HTTP and HTTPS are open."
  }
  assert {
    condition     = toset([for r in hcloud_firewall.aspen.rule : r.port]) == toset(["80", "443"])
    error_message = "The firewall opens only ports 80 and 443."
  }
  assert {
    condition     = !can(regex("\"(5432|9000|9001|7070):", local.compose))
    error_message = "PostgreSQL, MinIO and Aspen publish no host ports; only Caddy does."
  }
  # Issue #125: read and push requests are rate limited by default.
  assert {
    condition     = jsondecode(yamldecode(local.compose).services.aspen.environment.ASPEN_LIMITS).read.per_second > 0 && jsondecode(yamldecode(local.compose).services.aspen.environment.ASPEN_LIMITS).push.per_second > 0
    error_message = "ASPEN_LIMITS limits read and push requests per caller by default."
  }

  # State lives on the protected volume; the server cannot be deleted by accident.
  assert {
    condition     = hcloud_volume.data.delete_protection && hcloud_server.aspen.delete_protection && hcloud_server.aspen.rebuild_protection
    error_message = "The server and the data volume are protected by default."
  }
  assert {
    condition     = strcontains(hcloud_server.aspen.user_data, "/dev/disk/by-id/scsi-0HC_Volume_4242")
    error_message = "cloud-init mounts the attached data volume."
  }
  assert {
    condition     = tostring(hcloud_volume_attachment.data.volume_id) == hcloud_volume.data.id && !hcloud_volume_attachment.data.automount
    error_message = "The data volume is attached and mounted by cloud-init at a fixed path."
  }

  # Aspen runs the pinned image as `aspen serve`, behind one proxy, with its
  # spool on the volume.
  assert {
    condition     = strcontains(local.compose, "image: ghcr.io/reason-machines/aspen:v1.2.3") && strcontains(local.compose, "command: [\"serve\"]")
    error_message = "Aspen runs the pinned image as aspen serve."
  }
  assert {
    condition     = strcontains(local.compose, "ASPEN_TRUSTED_PROXY_HOPS: \"1\"") && strcontains(local.compose, "ASPEN_PUBLIC_URL: https://git.example.test")
    error_message = "Aspen trusts exactly one proxy (Caddy) and knows its public URL."
  }
  assert {
    condition     = strcontains(local.compose, "/srv/aspen/spool:/var/lib/aspen/spool")
    error_message = "The push spool is on the data volume, a real disk."
  }

  # Secrets are generated on the server: the user data only references them.
  assert {
    condition     = !can(regex("(PASSWORD|SECRET_KEY): [^$]", local.compose))
    error_message = "Every password and secret key in the compose file comes from the generated .env."
  }
  assert {
    condition     = strcontains(hcloud_server.aspen.user_data, "init-env.sh /srv/aspen/.env")
    error_message = "Secrets are generated at first boot into the data volume."
  }
  assert {
    condition     = length(hcloud_zone_rrset.a) == 0 && length(hcloud_zone_rrset.aaaa) == 0
    error_message = "No DNS records without dns_zone."
  }
  assert {
    condition     = output.url == "https://git.example.test"
    error_message = "The URL is the HTTPS domain."
  }
}

run "ssh_and_dns" {
  command = apply

  variables {
    admin_cidrs = ["198.51.100.0/24"]
    dns_zone    = "example.test"
  }

  assert {
    condition     = one([for r in hcloud_firewall.aspen.rule : r.source_ips if r.port == "22"]) == toset(["198.51.100.0/24"])
    error_message = "SSH opens only to the admin ranges."
  }
  assert {
    condition     = hcloud_zone_rrset.a[0].name == "git" && one(hcloud_zone_rrset.a[0].records).value == "203.0.113.10"
    error_message = "The A record names the server inside the zone."
  }
  assert {
    condition     = one(hcloud_zone_rrset.aaaa[0].records).value == "2001:db8::1"
    error_message = "The AAAA record points at the server's IPv6 address."
  }
}

run "zone_apex" {
  command = plan

  variables {
    domain_name = "example.test"
    dns_zone    = "example.test"
  }

  assert {
    condition     = hcloud_zone_rrset.a[0].name == "@"
    error_message = "The zone apex is written as @."
  }
}

run "unpinned_image_is_refused" {
  command = plan

  variables {
    aspen_image = "ghcr.io/reason-machines/aspen:latest"
  }

  expect_failures = [var.aspen_image]
}

run "no_ssh_key_is_refused" {
  command = plan

  variables {
    admin_ssh_public_keys = []
  }

  expect_failures = [var.admin_ssh_public_keys]
}

run "limits_must_be_json" {
  command = plan

  variables {
    aspen_limits_json = "{read: 10}"
  }

  expect_failures = [var.aspen_limits_json]
}

run "policy_must_be_json" {
  command = plan

  variables {
    aspen_policy_json = "{anonymous: read}"
  }

  expect_failures = [var.aspen_policy_json]
}
