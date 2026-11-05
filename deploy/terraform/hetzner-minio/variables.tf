variable "name" {
  description = "Prefix for every resource name."
  type        = string
  default     = "aspen"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,30}$", var.name))
    error_message = "name must be 2-31 lowercase letters, digits or hyphens, starting with a letter."
  }
}

variable "domain_name" {
  description = "Host name clients use (git.example.com). Caddy gets its TLS certificate from Let's Encrypt, so this name must resolve to the server before the first start."
  type        = string

  validation {
    condition     = can(regex("^([a-z0-9]([a-z0-9-]*[a-z0-9])?\\.)+[a-z]{2,}$", var.domain_name))
    error_message = "domain_name must be a lowercase DNS name such as git.example.com."
  }
}

variable "acme_email" {
  description = "Contact address for the Let's Encrypt account (certificate expiry notices)."
  type        = string

  validation {
    condition     = can(regex("^[^@\\s]+@[^@\\s]+$", var.acme_email))
    error_message = "acme_email must be an email address."
  }
}

variable "aspen_image" {
  description = "Aspen container image, pinned to a release tag (ghcr.io/reason-machines/aspen:v1.2.3) or a digest (...@sha256:...)."
  type        = string

  validation {
    condition     = can(regex("(:v?[0-9]+\\.[0-9]+\\.[0-9]+[^:/]*|@sha256:[0-9a-f]{64})$", var.aspen_image))
    error_message = "aspen_image must be pinned to a version tag (:v1.2.3) or a digest (@sha256:...), never :latest or untagged, so a reboot cannot change what runs."
  }
}

variable "location" {
  description = "Hetzner location (fsn1, nbg1, hel1, ash, hil, sin)."
  type        = string
  default     = "fsn1"
}

variable "server_type" {
  description = "Server type (`hcloud server-type list`). Size memory for about 140 bytes per object of the largest push plus PostgreSQL and MinIO."
  type        = string
  default     = "cpx31"
}

variable "server_image" {
  description = "Operating system image. The cloud-init script expects Ubuntu 24.04 (docker.io and docker-compose-v2 packages)."
  type        = string
  default     = "ubuntu-24.04"
}

variable "data_volume_size_gb" {
  description = "Size of the volume holding PostgreSQL, MinIO, the push spool and the generated secrets. Packs are never deleted in v1, so leave room to grow."
  type        = number
  default     = 100

  validation {
    condition     = var.data_volume_size_gb >= 20
    error_message = "data_volume_size_gb must be at least 20."
  }
}

variable "admin_ssh_public_keys" {
  description = "Public SSH keys allowed to log in as root (at least one: the generated secrets live on the server)."
  type        = list(string)

  validation {
    condition     = length(var.admin_ssh_public_keys) > 0
    error_message = "Give at least one SSH public key."
  }
}

variable "admin_cidrs" {
  description = "Source ranges allowed to reach SSH (port 22). Empty closes SSH in the firewall; open it when you need to log in."
  type        = list(string)
  default     = []
}

variable "dns_zone" {
  description = "Hetzner DNS zone (example.com) to create domain_name's A and AAAA records in. Null if you manage DNS elsewhere: point domain_name at the ipv4/ipv6 outputs yourself."
  type        = string
  default     = null
}

variable "protect" {
  description = "Delete and rebuild protection on the server and the data volume. Turn off before `terraform destroy`."
  type        = bool
  default     = true
}

variable "postgres_image" {
  description = "PostgreSQL image."
  type        = string
  default     = "postgres:17-alpine@sha256:266d3c257594e195b309ae9d1805e54c788f1d7b375f56bf6c367aac6c917e7b"
}

variable "minio_image" {
  description = "MinIO image (the one deploy/compose pins; upstream MinIO no longer publishes images)."
  type        = string
  default     = "cgr.dev/chainguard/minio:latest-dev@sha256:604ce09e2099ab20704a4c3bf25863bbb9bdaa643e7f522dc652098a6284bd6a"
}

variable "caddy_image" {
  description = "Caddy image: TLS termination and HTTP/2 in front of Aspen."
  type        = string
  default     = "caddy:2-alpine@sha256:e70b164da81ef698b3ad19af799ea6086ce6d9e48ed4079eb9fe26f6fbf14ee5"
}

variable "aspen_policy_json" {
  description = "Aspen access policy as JSON (aspen_auth::PolicyConfig). The default lets nobody in without a token from `aspen token create`."
  type        = string
  default     = "{}"

  validation {
    condition     = can(jsondecode(var.aspen_policy_json))
    error_message = "aspen_policy_json must be valid JSON."
  }
}

variable "aspen_limits_json" {
  description = "Aspen rate limits and push quotas (aspen_auth::LimitsConfig) as JSON, set as ASPEN_LIMITS. The default limits read and push requests per caller (deploy/README.md); \"{}\" turns request rates off."
  type        = string
  default     = "{\"read\":{\"burst\":100,\"per_second\":20},\"push\":{\"burst\":20,\"per_second\":5}}"

  validation {
    condition     = can(jsondecode(var.aspen_limits_json))
    error_message = "aspen_limits_json must be valid JSON."
  }
}

variable "labels" {
  description = "Labels added to every resource."
  type        = map(string)
  default     = {}
}
