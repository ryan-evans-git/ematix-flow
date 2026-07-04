# Refreshed 2026-07-04: validated against Terraform 1.14. The cloudinit
# provider requirement moved here from main.tf (one requirements block —
# duplicate `terraform {}` blocks are legal but easy to fork).
terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.100"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    cloudinit = {
      source  = "hashicorp/cloudinit"
      version = "~> 2.3"
    }
  }
}
