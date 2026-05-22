# Inputs to the distributed-cluster campaign. Brings up a 4-node EC2
# cluster (1 coordinator + 3 workers) sized for either SF=10 or SF=100.
# Typical invocation:
#
#   terraform apply                                         # SF=10 default
#   terraform apply -var scale_factor=100                   # SF=100 sizing
#   terraform apply -var engine=trino                       # bootstrap Trino
#
# Tear-down via `terraform destroy`.

variable "aws_region" {
  description = "Region for all resources. Match the singleton bucket from infra/test-validation if you're reusing data."
  type        = string
  default     = "us-east-2"
}

variable "project_tag" {
  description = "Project tag applied to every resource."
  type        = string
  default     = "ematix-flow-distributed"
}

variable "owner_email" {
  description = "Email of the human responsible for this campaign."
  type        = string
  default     = "ryanevans23@gmail.com"
}

variable "max_lifetime_hours" {
  description = "Advisory tag — how long the cluster is expected to live."
  type        = number
  default     = 8
}

# ============================================================
# Cluster sizing
# ============================================================

variable "scale_factor" {
  description = "TPC-H scale factor. 10 → c7i.2xlarge / 100 GB EBS. 100 → c7i.4xlarge / 250 GB EBS for Q18/Q21 RAM headroom."
  type        = number
  default     = 10
  validation {
    condition     = contains([10, 100], var.scale_factor)
    error_message = "scale_factor must be 10 or 100."
  }
}

variable "worker_count" {
  description = "Number of worker nodes (excluding coordinator). 3 = 4-node cluster total. The plan's recommended size."
  type        = number
  default     = 3
}

variable "use_spot" {
  description = "Spot pricing on all nodes. Spot capacity for c7i.4xlarge can be flaky; if you get repeated evictions, flip to false."
  type        = bool
  default     = false
}

variable "max_spot_price" {
  description = "Spot bid cap, USD/hr. Default 0.60 covers c7i.4xlarge's ~$0.30/hr spot rate with headroom."
  type        = string
  default     = "0.60"
}

# ============================================================
# Engine selection
# ============================================================

variable "engine" {
  description = "Which engine to bootstrap on the cluster. ematix | trino | pyspark | none. 'none' brings up bare nodes for manual setup / debugging."
  type        = string
  default     = "ematix"
  validation {
    condition     = contains(["ematix", "trino", "pyspark", "none"], var.engine)
    error_message = "engine must be ematix, trino, pyspark, or none."
  }
}

# ============================================================
# Storage / data
# ============================================================

variable "bench_bucket" {
  description = "S3 bucket holding TPC-H parquet at s3://<bucket>/tpch-data/sf{10,100}/<table>.parquet AND receiving results at s3://<bucket>/results/<stamp>/. Required — generate the data first via infra/test-validation/scripts/gen-sf{10,100}-data.sh OR via the single-node Phase A campaign."
  type        = string
}

variable "glue_database_name" {
  description = "Glue Data Catalog database for Trino's hive catalog. Auto-created if missing. Only used when engine=trino."
  type        = string
  default     = "ematix_tpch"
}

# ============================================================
# Networking
# ============================================================

variable "ssh_pubkey" {
  description = "Optional SSH public key for emergency access. Leave empty to rely on SSM Session Manager."
  type        = string
  default     = ""
}
