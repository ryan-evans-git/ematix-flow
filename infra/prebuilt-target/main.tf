# Persistent S3 bucket for cached Rust build artifacts (target/).
#
# Why this module is separate from infra/test-validation/:
#
# The campaign terraform under infra/test-validation/ is *ephemeral* —
# every `terraform apply` provisions a fresh suffix-tagged copy of the
# campaign and the matching `terraform destroy` removes it. A prebuilt-
# target bucket must outlive that lifecycle: a GitHub Actions job
# uploads to it on every push to main, and the next campaign launch
# reads from it. Putting the bucket inside the campaign module would
# either churn the cache on every campaign or fight terraform's
# create/destroy semantics with `prevent_destroy` flags.
#
# So this is its own one-shot module with its own state file. You run
# `terraform apply` here once after first setting up the account, then
# never again — campaigns and the GHA workflow assume the bucket exists.
#
# To bootstrap:
#   cd infra/prebuilt-target
#   terraform init
#   terraform apply
#
# After that, the outputs (bucket name + ARN) are what the campaign
# terraform consumes via -var, and what the GHA workflow needs as a
# repository variable.

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "aws_region" {
  description = "Region for the bucket. Match the region the campaign EC2 boxes run in to avoid cross-region transfer."
  type        = string
  default     = "us-east-2"
}

variable "project_tag" {
  description = "Project tag applied to every resource. Used for cost attribution and human grep."
  type        = string
  default     = "ematix-flow"
}

variable "expiry_days" {
  description = "How long to keep cached target tarballs. Each tarball is keyed by git SHA, so a 30-day window covers every active branch's parent commit on main plus a buffer."
  type        = number
  default     = 30
}

provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}

# Bucket name embeds the AWS account ID so the module is safe to apply
# in multiple AWS accounts without collision. S3 bucket names are
# globally unique across all of AWS — without the account suffix, a
# different org running this module would conflict with us.
locals {
  bucket_name = "${var.project_tag}-prebuilt-targets-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket" "prebuilt_targets" {
  bucket = local.bucket_name

  tags = {
    Project = var.project_tag
    Purpose = "cached-rust-target-artifacts"
  }
}

resource "aws_s3_bucket_public_access_block" "prebuilt_targets" {
  bucket                  = aws_s3_bucket.prebuilt_targets.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "prebuilt_targets" {
  bucket = aws_s3_bucket.prebuilt_targets.id
  versioning_configuration {
    # Disabled. Each tarball is keyed by `target-<sha>.tar.gz` plus an
    # overwriting `target-latest.tar.gz` pointer — we don't need version
    # history of the latter, and the SHA-keyed copies already give us
    # rollback-by-checkout.
    status = "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "prebuilt_targets" {
  bucket = aws_s3_bucket.prebuilt_targets.id

  rule {
    id     = "expire-old-tarballs"
    status = "Enabled"

    # `filter {}` with no inner predicate applies the rule to every
    # object in the bucket. Required field on the modern API even when
    # we want the rule to cover everything.
    filter {}

    expiration {
      days = var.expiry_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

output "bucket_name" {
  description = "Pass this to the campaign terraform as -var prebuild_target_bucket=<this> and set as a GitHub Actions repository variable named EMATIX_PREBUILD_BUCKET."
  value       = aws_s3_bucket.prebuilt_targets.bucket
}

output "bucket_arn" {
  description = "Bucket ARN. Used by the campaign terraform's IAM policy to grant Phase A read access."
  value       = aws_s3_bucket.prebuilt_targets.arn
}
