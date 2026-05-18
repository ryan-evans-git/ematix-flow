# `infra/prebuilt-target/`

Persistent S3 bucket for cached Rust build artifacts. One-shot module —
apply once per AWS account, then leave alone.

## Why this exists

Every campaign run under `infra/test-validation/` previously spent
~10 minutes on `cargo build --release --workspace` before the first
bench stage could start. With this module + the matching GitHub Actions
workflow (`.github/workflows/prebuild-target.yml`) and the userdata.sh
fast-path, the campaign EC2 box instead pulls a tarball that GitHub
Actions has already built from the latest `main` commit, untars it
into `target/`, and skips straight to bench execution.

That ~10 min × N campaigns per week × c7i spot price (~$0.20/hr) is
not huge in absolute dollars but compounds painfully when iterating on
the campaign harness itself.

## One-time setup

```sh
cd infra/prebuilt-target
terraform init
terraform apply
```

Outputs:

- `bucket_name`: pass to the campaign module as `-var prebuild_target_bucket=<this>`
- `bucket_arn`: used internally by the campaign module to scope the IAM read policy

After `terraform apply`:

1. **Set the GitHub Actions repository variable** `EMATIX_PREBUILD_BUCKET` to the `bucket_name` output value. Repo Settings → Secrets and variables → Actions → Variables tab → New repository variable.
2. **Set the GitHub Actions repository secrets** `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for the IAM user that uploads to this bucket. Recommend creating a dedicated `github-prebuild-uploader` IAM user with only `s3:PutObject` + `s3:GetObject` on this bucket's ARN. (TODO: convert to OIDC in a follow-up.)
3. **Pass `-var prebuild_target_bucket=<bucket_name>`** when running `terraform apply` in `infra/test-validation/` so the campaign EC2 role gets read access.

## Teardown

Don't. Or: only if you're shutting the project down. The bucket
self-expires objects after 30 days so storage cost is bounded; an empty
bucket is essentially free.

If you do need to remove it, run `terraform destroy` from this dir —
the bucket has no `force_destroy` flag, so you must empty it first:

```sh
aws s3 rm "s3://$(terraform output -raw bucket_name)" --recursive
terraform destroy
```
