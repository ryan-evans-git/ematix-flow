# ematix-flow AWS test-validation campaign.
#
# Phases A/B/C/D are gated by their `*_enabled` variables so you can
# bring up a subset. Defaults bring everything up; total cost per
# campaign run targets < $2 (see docs/AWS_VALIDATION_PLAN.md).
#
# Teardown: `terraform destroy`. Always run `scripts/teardown.sh`
# afterwards to assert zero residue (defence in depth — picks up
# anything Terraform didn't track, e.g. SSM-Run-Command-spawned EBS
# snapshots).

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project          = var.project_tag
      Owner            = var.owner_email
      ManagedBy        = "terraform"
      MaxLifetimeHours = tostring(var.max_lifetime_hours)
      # `CreatedAt` removed: `timestamp()` in default_tags hits a
      # known AWS-provider bug ("Provider produced inconsistent
      # final plan" — the timestamp differs between plan and apply,
      # so the provider sees the tag value flap). The campaign's
      # creation time is recoverable from CloudTrail and the
      # `random_id.campaign` suffix is the canonical campaign ID.
    }
  }
}

# Short suffix appended to globally-namespaced resources (S3 bucket,
# IAM roles, Lambda) so reruns don't collide.
resource "random_id" "campaign" {
  byte_length = 4
}

data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  suffix     = random_id.campaign.hex
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  base_name = "${var.project_tag}-${local.suffix}"
}

# ============================================================
# Networking — use the default VPC + default subnets in the region.
# ============================================================
#
# Spinning up our own VPC just to tear it down would add NAT-gateway
# cost ($0.045/hr per AZ) and IGW setup for no benefit on a test box.
# The default VPC has internet access and is plenty for ephemeral
# workloads.

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

# ============================================================
# Phase A — EC2 spot bench box.
# ============================================================

data "aws_ami" "al2023" {
  count       = var.phase_a_enabled ? 1 : 0
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

# IAM role attached to the EC2 instance profile. Two policies:
#   - AmazonSSMManagedInstanceCore: lets us shell in via SSM Session
#     Manager without opening port 22 to the world.
#   - inline policy: read/write the campaign's own S3 bucket only.
resource "aws_iam_role" "phase_a" {
  count = var.phase_a_enabled ? 1 : 0
  name  = "${local.base_name}-phase-a"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "phase_a_ssm" {
  count      = var.phase_a_enabled ? 1 : 0
  role       = aws_iam_role.phase_a[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy" "phase_a_s3" {
  count = var.phase_a_enabled && var.phase_b_enabled ? 1 : 0
  name  = "s3-results"
  role  = aws_iam_role.phase_a[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
      ]
      Resource = [
        aws_s3_bucket.results[0].arn,
        "${aws_s3_bucket.results[0].arn}/*",
      ]
    }]
  })
}

# Phase A → Phase C: invoke + update the Lambda function.
resource "aws_iam_role_policy" "phase_a_lambda" {
  count = var.phase_a_enabled && var.phase_c_enabled ? 1 : 0
  name  = "lambda-access"
  role  = aws_iam_role.phase_a[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "lambda:InvokeFunction",
        "lambda:UpdateFunctionCode",
        "lambda:UpdateFunctionConfiguration",
        "lambda:GetFunction",
        "lambda:GetFunctionConfiguration",
      ]
      Resource = aws_lambda_function.phase_c[0].arn
    }]
  })
}

# Phase A → Phase D: build + push to ECR, talk to EKS API server.
resource "aws_iam_role_policy" "phase_a_phase_d" {
  count = var.phase_a_enabled && var.phase_d_enabled ? 1 : 0
  name  = "phase-d-access"
  role  = aws_iam_role.phase_a[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ECRPushPull"
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:PutImage",
          "ecr:DescribeRepositories",
          "ecr:DescribeImages",
        ]
        Resource = "*"
      },
      {
        Sid    = "EKSDescribe"
        Effect = "Allow"
        Action = [
          "eks:DescribeCluster",
          "eks:ListClusters",
        ]
        Resource = aws_eks_cluster.phase_d[0].arn
      },
    ]
  })
}

# Phase A → cluster auth. The aws_eks_access_entry below grants the
# EC2 role kubernetes:admin on the cluster so kubectl + the
# kubernetes Python client can talk to the API server without
# additional IAM-aware kubeconfig wiring.
resource "aws_eks_access_entry" "phase_a" {
  count         = var.phase_a_enabled && var.phase_d_enabled ? 1 : 0
  cluster_name  = aws_eks_cluster.phase_d[0].name
  principal_arn = aws_iam_role.phase_a[0].arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "phase_a_admin" {
  count         = var.phase_a_enabled && var.phase_d_enabled ? 1 : 0
  cluster_name  = aws_eks_cluster.phase_d[0].name
  principal_arn = aws_iam_role.phase_a[0].arn
  policy_arn    = "arn:${local.partition}:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  access_scope {
    type = "cluster"
  }
  depends_on = [aws_eks_access_entry.phase_a]
}

resource "aws_iam_instance_profile" "phase_a" {
  count = var.phase_a_enabled ? 1 : 0
  name  = "${local.base_name}-phase-a"
  role  = aws_iam_role.phase_a[0].name
}

resource "aws_key_pair" "phase_a" {
  count      = var.phase_a_enabled && var.phase_a_ssh_pubkey != "" ? 1 : 0
  key_name   = "${local.base_name}-phase-a"
  public_key = var.phase_a_ssh_pubkey
}

# Security group: SSM Session Manager handles inbound, so we only need
# egress. If the user supplied an SSH key, open 22 to /0 — convenience
# for emergency console access; the spot lifetime is hours.
resource "aws_security_group" "phase_a" {
  count       = var.phase_a_enabled ? 1 : 0
  name        = "${local.base_name}-phase-a"
  description = "Phase A bench box - egress + optional SSH."
  vpc_id      = data.aws_vpc.default.id

  egress {
    description = "Allow all outbound (S3, package mirrors, GitHub clone)."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  dynamic "ingress" {
    for_each = var.phase_a_ssh_pubkey != "" ? [1] : []
    content {
      description = "SSH for emergency console — only opens if a pubkey was supplied."
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = ["0.0.0.0/0"]
    }
  }
}

# Userdata script. Templated so we can inject the campaign's S3 bucket
# name; it then pulls the bench harness from that bucket (uploaded by
# the local `make aws-up-phase-a` target before launch) and runs it.
locals {
  phase_a_userdata = var.phase_a_enabled ? templatefile("${path.module}/userdata.sh", {
    region               = var.aws_region
    results_bucket       = var.phase_b_enabled ? aws_s3_bucket.results[0].bucket : ""
    project_tag          = var.project_tag
    max_runtime_hrs      = var.phase_a_max_runtime_hours
    lambda_function_name = var.phase_c_enabled ? aws_lambda_function.phase_c[0].function_name : ""
    eks_cluster_name     = var.phase_d_enabled ? aws_eks_cluster.phase_d[0].name : ""
    ecr_repo_url         = var.phase_d_enabled ? aws_ecr_repository.phase_d_worker[0].repository_url : ""
  }) : ""
}

# EC2 spot launch. Spot saves ~70%; on-demand fallback isn't worth the
# Terraform complexity for a test box — if spot is unavailable the
# request just doesn't fulfil and we rerun.
resource "aws_instance" "phase_a" {
  count = var.phase_a_enabled ? 1 : 0

  ami                         = data.aws_ami.al2023[0].id
  instance_type               = var.phase_a_instance_type
  iam_instance_profile        = aws_iam_instance_profile.phase_a[0].name
  vpc_security_group_ids      = [aws_security_group.phase_a[0].id]
  subnet_id                   = data.aws_subnets.default.ids[0]
  associate_public_ip_address = true
  user_data                   = local.phase_a_userdata
  key_name                    = var.phase_a_ssh_pubkey != "" ? aws_key_pair.phase_a[0].key_name : null

  # Terminate (not stop) on shutdown. The userdata's last line is
  # `shutdown -h now`; with this attribute it deletes the instance
  # + EBS in one step.
  instance_initiated_shutdown_behavior = "terminate"

  root_block_device {
    volume_type           = "gp3"
    volume_size           = var.phase_a_ebs_size_gb
    delete_on_termination = true
    encrypted             = true
  }

  # Spot configuration. Hibernation off (gp3 root is non-EBS-encrypted-
  # capable for hibernate anyway). `one-time` request type means we
  # don't refresh on interruption — caller reruns the campaign.
  dynamic "instance_market_options" {
    for_each = var.phase_a_use_spot ? [1] : []
    content {
      market_type = "spot"
      spot_options {
        max_price                      = var.phase_a_max_spot_price
        spot_instance_type             = "one-time"
        instance_interruption_behavior = "terminate"
      }
    }
  }

  metadata_options {
    # IMDSv2 required — closes the SSRF blast radius. AL2023 supports
    # it out of the box.
    http_tokens   = "required"
    http_endpoint = "enabled"
  }

  tags = {
    Name = "${local.base_name}-phase-a"
    Role = "bench-and-integration"
  }
}

# ============================================================
# Phase B — S3 results bucket.
# ============================================================

resource "aws_s3_bucket" "results" {
  count  = var.phase_b_enabled ? 1 : 0
  bucket = "${local.base_name}-results"
  # `force_destroy = var.s3_force_destroy` — defaults to FALSE so
  # `terraform destroy` refuses if the bucket still has objects.
  # That's the safety net for "I forgot to fetch results first".
  #
  # Flip to true via `-var s3_force_destroy=true` once you've run
  # `./scripts/fetch-results.sh` (or you don't care about the
  # results). The 24-hour lifecycle policy below also self-expires
  # objects, so a true=false bucket eventually becomes destroyable
  # without intervention.
  force_destroy = var.s3_force_destroy
}

resource "aws_s3_bucket_versioning" "results" {
  count  = var.phase_b_enabled ? 1 : 0
  bucket = aws_s3_bucket.results[0].id
  versioning_configuration {
    # No versioning — the campaign overwrites results in place and we
    # don't need history. Saves storage cost.
    status = "Disabled"
  }
}

resource "aws_s3_bucket_public_access_block" "results" {
  count                   = var.phase_b_enabled ? 1 : 0
  bucket                  = aws_s3_bucket.results[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# 24-hour auto-delete lifecycle. Belt-and-braces against forgotten
# objects — `terraform destroy` is the primary cleanup path.
resource "aws_s3_bucket_lifecycle_configuration" "results" {
  count  = var.phase_b_enabled ? 1 : 0
  bucket = aws_s3_bucket.results[0].id
  rule {
    id     = "auto-expire-24h"
    status = "Enabled"
    filter {} # apply to all objects in bucket
    expiration {
      days = 1
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# ============================================================
# Phase C — Lambda smoke test.
# ============================================================

resource "aws_iam_role" "phase_c" {
  count = var.phase_c_enabled ? 1 : 0
  name  = "${local.base_name}-phase-c"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "phase_c_basic" {
  count      = var.phase_c_enabled ? 1 : 0
  role       = aws_iam_role.phase_c[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Stub Lambda — a tiny inline handler that pretends to be the flow
# worker. The real campaign replaces this with the proper wheel-built
# package before invoking. Terraform's job is to provision the
# function shell; populating the code is a follow-up step in the
# bench harness (uses `aws lambda update-function-code`).
data "archive_file" "phase_c_stub" {
  count       = var.phase_c_enabled ? 1 : 0
  type        = "zip"
  output_path = "${path.module}/.terraform-build/phase-c-stub.zip"
  source {
    content  = "def handler(event, context):\n    return {'ok': True, 'event': event}\n"
    filename = "index.py"
  }
}

resource "aws_lambda_function" "phase_c" {
  count            = var.phase_c_enabled ? 1 : 0
  function_name    = "${local.base_name}-lambda"
  role             = aws_iam_role.phase_c[0].arn
  handler          = "index.handler"
  runtime          = "python3.12"
  architectures    = ["x86_64"]
  memory_size      = 256
  timeout          = 60
  filename         = data.archive_file.phase_c_stub[0].output_path
  source_code_hash = data.archive_file.phase_c_stub[0].output_base64sha256
}

# ============================================================
# Phase D — EKS smoke test.
# ============================================================
#
# We use a minimal EKS cluster with a single managed node group. Real
# IRSA-style IAM-for-pods is wired up so the K8sJobExecutor can be
# tested against the actual production-shape security model.

resource "aws_iam_role" "phase_d_cluster" {
  count = var.phase_d_enabled ? 1 : 0
  name  = "${local.base_name}-phase-d-cluster"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "phase_d_cluster_policy" {
  count      = var.phase_d_enabled ? 1 : 0
  role       = aws_iam_role.phase_d_cluster[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonEKSClusterPolicy"
}

resource "aws_iam_role" "phase_d_node" {
  count = var.phase_d_enabled ? 1 : 0
  name  = "${local.base_name}-phase-d-node"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "phase_d_node_worker" {
  count      = var.phase_d_enabled ? 1 : 0
  role       = aws_iam_role.phase_d_node[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonEKSWorkerNodePolicy"
}

resource "aws_iam_role_policy_attachment" "phase_d_node_cni" {
  count      = var.phase_d_enabled ? 1 : 0
  role       = aws_iam_role.phase_d_node[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonEKS_CNI_Policy"
}

resource "aws_iam_role_policy_attachment" "phase_d_node_ecr" {
  count      = var.phase_d_enabled ? 1 : 0
  role       = aws_iam_role.phase_d_node[0].name
  policy_arn = "arn:${local.partition}:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
}

resource "aws_eks_cluster" "phase_d" {
  count    = var.phase_d_enabled ? 1 : 0
  name     = "${local.base_name}-phase-d"
  role_arn = aws_iam_role.phase_d_cluster[0].arn
  version  = "1.30"

  vpc_config {
    subnet_ids              = data.aws_subnets.default.ids
    endpoint_public_access  = true
    endpoint_private_access = false
  }

  # API-mode access config so the Phase A EC2 role can be granted
  # cluster admin via `aws_eks_access_entry` (vs the legacy
  # aws-auth ConfigMap dance).
  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = true
  }

  # Default logs OFF. CloudWatch logging costs add up surprisingly fast;
  # for a 2-hour smoke we don't need them.

  depends_on = [aws_iam_role_policy_attachment.phase_d_cluster_policy]
}

resource "aws_eks_node_group" "phase_d" {
  count           = var.phase_d_enabled ? 1 : 0
  cluster_name    = aws_eks_cluster.phase_d[0].name
  node_group_name = "default"
  node_role_arn   = aws_iam_role.phase_d_node[0].arn
  subnet_ids      = data.aws_subnets.default.ids
  instance_types  = [var.phase_d_node_instance_type]

  scaling_config {
    desired_size = var.phase_d_node_count
    min_size     = var.phase_d_node_count
    max_size     = var.phase_d_node_count
  }

  depends_on = [
    aws_iam_role_policy_attachment.phase_d_node_worker,
    aws_iam_role_policy_attachment.phase_d_node_cni,
    aws_iam_role_policy_attachment.phase_d_node_ecr,
  ]
}

resource "aws_ecr_repository" "phase_d_worker" {
  count                = var.phase_d_enabled ? 1 : 0
  name                 = "${local.base_name}/flow-worker"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}
