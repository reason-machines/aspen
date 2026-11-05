variable "domain_name" {
  description = "Host name clients use, e.g. git.example.com."
  type        = string
}

variable "acme_email" {
  description = "Let's Encrypt contact address."
  type        = string
}

variable "aspen_image" {
  description = "Aspen image pinned to a release tag or digest."
  type        = string
}

variable "admin_ssh_public_keys" {
  description = "SSH public keys for root."
  type        = list(string)
}

variable "admin_cidrs" {
  description = "Ranges allowed to reach SSH; empty closes it."
  type        = list(string)
  default     = []
}

variable "dns_zone" {
  description = "Hetzner DNS zone for the records, or null to manage DNS yourself."
  type        = string
  default     = null
}

variable "location" {
  description = "Hetzner location."
  type        = string
  default     = "fsn1"
}

variable "server_type" {
  description = "Hetzner server type."
  type        = string
  default     = "cpx31"
}
