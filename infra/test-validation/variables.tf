# Inputs to the test-validation campaign. All optional with sensible
# defaults — the typical invocation is `terraform apply` with no
# `-var` flags. Override per-phase via `-target` if you want to bring
# up a subset (e.g. Phase A only).

variable "aws_region" {
  description = "Region for all resources. Cheap + central. Avoid us-east-1 for cost reasons."
  type        = string
  default     = "us-east-2"
}

variable "project_tag" {
  description = "Project tag applied to every resource. Used by the teardown script to find resources to clean up."
  type        = string
  default     = "ematix-flow-test"
}

variable "owner_email" {
  description = "Email of the human responsible for this campaign. Surfaces in tags so an AWS admin can route a question."
  type        = string
  default     = "ryanevans23@gmail.com"
}

variable "max_lifetime_hours" {
  description = "Advisory tag — informs the janitor script which resources are expected to be ephemeral."
  type        = number
  default     = 24
}

variable "prebuild_target_bucket" {
  description = "Name of the persistent S3 bucket holding cached cargo target tarballs (created out-of-band via `infra/prebuilt-target/`). When set, the Phase A userdata pulls `target/latest.tar.zst` instead of running `cargo build` and falls through to a full build only on miss. Empty string disables the fast path and the cache-read IAM grant."
  type        = string
  default     = ""
}

# ============================================================
# Phase A — EC2 bench / integration box
# ============================================================

variable "phase_a_enabled" {
  description = "Provision Phase A (EC2 spot box for AVX bench + SF=10 triangulation + cross-arch integration tests)."
  type        = bool
  default     = true
}

variable "phase_a_instance_type" {
  description = "EC2 instance type. c7i.2xlarge gives Intel Sapphire Rapids (AVX-512) at $0.43/hr on-demand, ~$0.13/hr spot."
  type        = string
  default     = "c7i.2xlarge"
}

variable "phase_a_use_spot" {
  description = "Use spot for Phase A. Default false because fresh AWS accounts have a spot quota of 0 by default. Request a quota increase + flip this to true if you want the ~70% savings on subsequent campaigns."
  type        = bool
  default     = false
}

variable "phase_a_max_spot_price" {
  description = "Spot bid cap as decimal USD/hr. Pad above the going rate (~$0.13/hr) so we don't get evicted on a small price spike."
  type        = string
  default     = "0.30"
}

variable "phase_a_ebs_size_gb" {
  description = "Root EBS volume size. Needs to hold SF=10 lineitem (~7.5 GB) + target/ build artifacts + workspace + headroom."
  type        = number
  default     = 100
}

variable "phase_a_max_runtime_hours" {
  description = "Watchdog: SSM-scheduled terminate after this many hours, in case the in-instance self-shutdown fails."
  type        = number
  default     = 5
}

variable "phase_a_ssh_pubkey" {
  description = "Optional SSH public key for emergency console access. Leave empty to disable SSH entirely (rely on SSM Session Manager)."
  type        = string
  default     = ""
}

# ============================================================
# Phase B — S3 bucket
# ============================================================

variable "phase_b_enabled" {
  description = "Provision Phase B (S3 bucket for results + S3RunLog test)."
  type        = bool
  default     = true
}

variable "s3_force_destroy" {
  description = "If false (default), terraform destroy refuses to delete the results bucket while it has objects. Flip to true after running `./scripts/fetch-results.sh`."
  type        = bool
  default     = false
}

# ============================================================
# Phase C — Lambda
# ============================================================

variable "phase_c_enabled" {
  description = "Provision Phase C (Lambda function for LambdaExecutor smoke test)."
  type        = bool
  default     = true
}

# ============================================================
# Phase D — EKS
# ============================================================

variable "phase_d_enabled" {
  description = "Provision Phase D (EKS cluster for K8sJobExecutor smoke test). Locked 2026-05-16: yes — covers IRSA / ECR / autoscaler."
  type        = bool
  default     = true
}

variable "phase_d_node_instance_type" {
  description = "EKS node group instance type. t3.medium is the smallest viable + cheapest while still running real workloads."
  type        = string
  default     = "t3.medium"
}

variable "phase_d_node_count" {
  description = "Fixed node count for the EKS node group. One is enough for a smoke test."
  type        = number
  default     = 1
}
