# Outputs the bench orchestrator pulls to drive the cluster from a
# laptop: `terraform output -json | jq` or `terraform output -raw <key>`.

output "region" {
  value = var.aws_region
}

output "scale_factor" {
  value = var.scale_factor
}

output "engine" {
  value = var.engine
}

output "instance_type" {
  value = local.instance_type
}

output "bench_bucket" {
  value = var.bench_bucket
}

output "coordinator_instance_id" {
  description = "EC2 instance ID for SSM session-manager."
  value       = aws_instance.coordinator.id
}

output "coordinator_private_ip" {
  description = "Private IP — what workers use to register, what bench scripts target."
  value       = aws_instance.coordinator.private_ip
}

output "coordinator_public_ip" {
  description = "Public IP — for direct SSH if pubkey was supplied."
  value       = aws_instance.coordinator.public_ip
}

output "worker_instance_ids" {
  value = aws_instance.worker[*].id
}

output "worker_private_ips" {
  description = "Workers' private IPs in count.index order. Both PySpark and Trino bench scripts iterate over this list."
  value       = aws_instance.worker[*].private_ip
}

output "worker_public_ips" {
  value = aws_instance.worker[*].public_ip
}

output "glue_database_name" {
  description = "Glue database used by the Trino hive catalog. Empty unless engine=trino."
  value       = var.engine == "trino" ? aws_glue_catalog_database.tpch[0].name : ""
}

output "ssm_session_command" {
  description = "Copy-paste to SSM into the coordinator."
  value       = "aws ssm start-session --target ${aws_instance.coordinator.id} --region ${var.aws_region}"
}
