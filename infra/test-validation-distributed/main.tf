# ematix-flow distributed-cluster campaign.
#
# Brings up 1 coordinator + N workers in the same AZ. All nodes share
# an IAM role with S3 read on the TPC-H data bucket + read/write on
# the results prefix + Glue Get* (for Trino engine). A security group
# allows all intra-cluster TCP (Spark, Trino, Arrow Flight, SSH-via-SSM)
# but only SSM access from outside.
#
# Engine bootstrap lives in `userdata/<engine>-{coordinator,worker}.sh`.
# The userdata script is selected by var.engine. Setting engine=none
# brings up bare AL2023 nodes for manual setup.
#
# Teardown: `terraform destroy`. The data bucket lives outside this
# module — destroy here does NOT delete TPC-H parquet.

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project          = var.project_tag
      Owner            = var.owner_email
      ManagedBy        = "terraform"
      MaxLifetimeHours = tostring(var.max_lifetime_hours)
      Engine           = var.engine
      ScaleFactor      = tostring(var.scale_factor)
    }
  }
}

resource "random_id" "campaign" {
  byte_length = 4
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  suffix     = random_id.campaign.hex
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition
  base_name  = "${var.project_tag}-${local.suffix}"

  # Instance sizing per scale factor. SF=10 lineitem is ~7.5 GB and
  # joins fit in 16 GB; SF=100 lineitem is ~75 GB and the Q18/Q21
  # build sides won't fit on 16 GB workers.
  instance_type = var.scale_factor == 100 ? "c7i.4xlarge" : "c7i.2xlarge"
  # SF100: 1 TB. Spark spills shuffle to local disk aggressively and DNF'd Q5
  # at SF100 on 250 GB with "No space left on device"; 1 TB gives the full
  # 22-query suite (154 warmup+trial iters) headroom. Trino survived on 250 GB
  # but shares this sizing harmlessly.
  ebs_size_gb   = var.scale_factor == 100 ? 1000 : 100

  # Bench bucket name (input) — referenced in IAM policies + userdata.
  # Validate caller provided it.
  bench_bucket_arn = "arn:${local.partition}:s3:::${var.bench_bucket}"
}

# ============================================================
# Networking — default VPC, single AZ
# ============================================================

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# Pick one subnet to anchor the cluster. Distributed perf depends on
# low intra-cluster latency, so we keep every node in the SAME AZ —
# never split a 4-node TPC-H cluster across AZs.
data "aws_subnet" "anchor" {
  id = tolist(data.aws_subnets.default.ids)[0]
}

# ============================================================
# Security group — wide-open intra-cluster, SSM-only ingress
# ============================================================
#
# All cluster ports (Spark 4040/6066/7077/8080, Trino 8080, Arrow
# Flight 50051, SSH 22) opened ONLY to other members of this SG.
# External traffic: nothing. Operator access is via SSM Session
# Manager, which doesn't require any inbound rule.

# NO inline ingress/egress blocks here: mixing inline rules with the
# standalone intra_cluster_all rule resource makes terraform treat the
# inline set as authoritative — the SECOND apply strips the standalone
# rule and partitions the running cluster (workers can't announce).
# All rules live as standalone resources below.
resource "aws_security_group" "cluster" {
  name        = "${local.base_name}-cluster"
  description = "ematix-flow distributed cluster - intra-SG traffic only"
  vpc_id      = data.aws_vpc.default.id
}

# SSH only if pubkey provided (emergency console)
resource "aws_vpc_security_group_ingress_rule" "ssh_operator" {
  count             = var.ssh_pubkey == "" ? 0 : 1
  security_group_id = aws_security_group.cluster.id
  description       = "SSH from operator"
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "all_outbound" {
  security_group_id = aws_security_group.cluster.id
  description       = "All outbound (S3, Glue, dnf, github)"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# Self-referential rule (separate resource to avoid the chicken-and-egg
# of an inline ingress referencing the SG's own id).
resource "aws_vpc_security_group_ingress_rule" "intra_cluster_all" {
  security_group_id            = aws_security_group.cluster.id
  description                  = "All TCP from other cluster members"
  referenced_security_group_id = aws_security_group.cluster.id
  ip_protocol                  = "tcp"
  from_port                    = 1
  to_port                      = 65535
}

# ============================================================
# IAM — instance profile shared by every cluster node
# ============================================================

resource "aws_iam_role" "cluster" {
  name = "${local.base_name}-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "bench_bucket" {
  name = "bench-bucket"
  role = aws_iam_role.cluster.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.bench_bucket_arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = "${local.bench_bucket_arn}/*"
      },
    ]
  })
}

# Glue + Hive metastore reads — Trino only. Cheap to grant always.
resource "aws_iam_role_policy" "glue" {
  name = "glue-read"
  role = aws_iam_role.cluster.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition",
        "glue:CreateDatabase",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
      ]
      Resource = "*"
    }]
  })
}

resource "aws_iam_instance_profile" "cluster" {
  name = "${local.base_name}-node"
  role = aws_iam_role.cluster.name
}

# ============================================================
# Key pair (only if pubkey supplied)
# ============================================================

resource "aws_key_pair" "cluster" {
  count      = var.ssh_pubkey == "" ? 0 : 1
  key_name   = "${local.base_name}-key"
  public_key = var.ssh_pubkey
}

# ============================================================
# AMI — Amazon Linux 2023, x86_64
# ============================================================

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023*-x86_64"]
  }
  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

# ============================================================
# Userdata — engine-specific bootstrap
# ============================================================
#
# Coordinator and workers run different scripts because the engine
# needs to know which role this node plays. The coordinator's
# private IP is known only AFTER the coordinator instance is
# created, so workers depend on it via templatefile() + aws_instance
# coordinator output.

locals {
  # Common base script appended to every userdata. Installs git, awscli
  # v2, python3.12, and clones the repo.
  base_userdata = templatefile("${path.module}/userdata/base.sh", {
    aws_region   = var.aws_region
    bench_bucket = var.bench_bucket
    git_ref      = var.git_ref
  })

  # Engine bootstrap path. "none" → just base.
  engine_script = var.engine == "none" ? "" : "${path.module}/userdata/${var.engine}.sh.tftpl"
}

# Coordinator userdata: base + engine-coordinator script. Engine
# scripts read EMATIX_ROLE=coordinator from the env we export.
data "cloudinit_config" "coordinator" {
  count         = 1
  gzip          = true
  base64_encode = true

  part {
    content_type = "text/x-shellscript"
    content      = local.base_userdata
  }

  dynamic "part" {
    for_each = var.engine == "none" ? [] : [1]
    content {
      content_type = "text/x-shellscript"
      content = templatefile("${path.module}/userdata/${var.engine}.sh.tftpl", {
        role               = "coordinator"
        coordinator_ip     = "" # self
        aws_region         = var.aws_region
        bench_bucket       = var.bench_bucket
        glue_database_name = var.glue_database_name
        scale_factor       = var.scale_factor
        data_prefix        = var.data_prefix
      })
    }
  }
}

# ============================================================
# Coordinator instance
# ============================================================

resource "aws_instance" "coordinator" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = local.instance_type
  subnet_id              = data.aws_subnet.anchor.id
  vpc_security_group_ids = [aws_security_group.cluster.id]
  iam_instance_profile   = aws_iam_instance_profile.cluster.name
  key_name               = var.ssh_pubkey == "" ? null : aws_key_pair.cluster[0].key_name

  associate_public_ip_address = true

  # One-time spot instances can't be stopped for an in-place user_data
  # update — force replacement instead.
  user_data_base64            = data.cloudinit_config.coordinator[0].rendered
  user_data_replace_on_change = true

  root_block_device {
    volume_size = local.ebs_size_gb
    volume_type = "gp3"
    iops        = 3000
    throughput  = 125
    encrypted   = true
  }

  dynamic "instance_market_options" {
    for_each = var.use_spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        max_price          = var.max_spot_price
        spot_instance_type = "one-time"
      }
    }
  }

  tags = {
    Name = "${local.base_name}-coordinator"
    Role = "coordinator"
  }
}

# ============================================================
# Worker instances
# ============================================================

data "cloudinit_config" "worker" {
  count         = var.worker_count
  gzip          = true
  base64_encode = true

  part {
    content_type = "text/x-shellscript"
    content      = local.base_userdata
  }

  dynamic "part" {
    for_each = var.engine == "none" ? [] : [1]
    content {
      content_type = "text/x-shellscript"
      content = templatefile("${path.module}/userdata/${var.engine}.sh.tftpl", {
        role               = "worker"
        coordinator_ip     = aws_instance.coordinator.private_ip
        aws_region         = var.aws_region
        bench_bucket       = var.bench_bucket
        glue_database_name = var.glue_database_name
        scale_factor       = var.scale_factor
        data_prefix        = var.data_prefix
      })
    }
  }
}

resource "aws_instance" "worker" {
  count                  = var.worker_count
  ami                    = data.aws_ami.al2023.id
  instance_type          = local.instance_type
  subnet_id              = data.aws_subnet.anchor.id
  vpc_security_group_ids = [aws_security_group.cluster.id]
  iam_instance_profile   = aws_iam_instance_profile.cluster.name
  key_name               = var.ssh_pubkey == "" ? null : aws_key_pair.cluster[0].key_name

  associate_public_ip_address = true

  # One-time spot instances can't be stopped for an in-place user_data
  # update — force replacement instead.
  user_data_base64            = data.cloudinit_config.worker[count.index].rendered
  user_data_replace_on_change = true

  root_block_device {
    volume_size = local.ebs_size_gb
    volume_type = "gp3"
    iops        = 3000
    throughput  = 125
    encrypted   = true
  }

  dynamic "instance_market_options" {
    for_each = var.use_spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        max_price          = var.max_spot_price
        spot_instance_type = "one-time"
      }
    }
  }

  tags = {
    Name = "${local.base_name}-worker-${count.index}"
    Role = "worker"
  }
}

# ============================================================
# Glue database — Trino metastore. Auto-created.
# ============================================================
#
# Cheap: Glue charges per object stored; ~$1/month per 100K objects.
# At our scale (8 TPC-H tables × 2 scale factors = 16 entries) the
# monthly bill rounds to zero.

resource "aws_glue_catalog_database" "tpch" {
  count = var.engine == "trino" ? 1 : 0
  name  = var.glue_database_name

  description = "TPC-H tables registered for Trino in the distributed bench campaign. Drop after teardown."
}
