terraform {
  # 1.11+ for write-only attributes (the database password never enters state).
  required_version = ">= 1.11"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0, < 7.0"
      # CloudFront only accepts ACM certificates from us-east-1. Pass the same
      # provider twice if your deployment region is us-east-1.
      configuration_aliases = [aws.us_east_1]
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.7, < 4.0"
    }
    # 4.1+ for the ephemeral tls_private_key (the SSH host key never enters
    # state).
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.1, < 5.0"
    }
  }
}
