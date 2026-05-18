# Outputs surfaced for the bench harness + teardown script. Everything
# here is post-apply data the next phase of the campaign needs to
# operate.

output "phase_a_instance_id" {
  description = "Phase A EC2 instance ID. Use with `aws ssm start-session --target ...` to shell in."
  value       = var.phase_a_enabled ? aws_instance.phase_a[0].id : null
}

output "phase_a_public_ip" {
  description = "Phase A public IP. Convenience only; prefer SSM Session Manager."
  value       = var.phase_a_enabled ? aws_instance.phase_a[0].public_ip : null
}

output "phase_b_bucket" {
  description = "Phase B S3 bucket name. Bench harness uploads results here; S3RunLog test points at this bucket."
  value       = var.phase_b_enabled ? aws_s3_bucket.results[0].bucket : null
}

output "phase_c_lambda_name" {
  description = "Phase C Lambda function name. Invoke via `aws lambda invoke --function-name ...`."
  value       = var.phase_c_enabled ? aws_lambda_function.phase_c[0].function_name : null
}

output "phase_d_cluster_name" {
  description = "Phase D EKS cluster name. Configure kubectl: `aws eks update-kubeconfig --name ...`."
  value       = var.phase_d_enabled ? aws_eks_cluster.phase_d[0].name : null
}

output "phase_d_ecr_repo_url" {
  description = "Phase D ECR repo URL. Push the flow worker image here, then reference from the K8s Job manifest."
  value       = var.phase_d_enabled ? aws_ecr_repository.phase_d_worker[0].repository_url : null
}

output "campaign_suffix" {
  description = "Random suffix appended to all globally-namespaced resources this campaign. Use to disambiguate parallel campaigns."
  value       = random_id.campaign.hex
}

output "region" {
  description = "Region everything lives in. Pass to AWS CLI commands."
  value       = var.aws_region
}
