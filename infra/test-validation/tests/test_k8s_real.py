"""Real-AWS K8sJobExecutor smoke + submit-to-completion latency.

Submits a `batch/v1.Job` to the EKS cluster created by Terraform,
backed by the flow worker image pushed to ECR. Two things it proves:

  1. End-to-end K8s integration: kubectl auth via aws-iam-authenticator
     / EKS IRSA, image pull from ECR, pod scheduling, container exit
     code surfacing.
  2. Submit-to-completion latency for one Job — the cold-start cost
     on a 1-node t3.medium cluster. This is the latency K8sJobExecutor
     adds vs SubprocessExecutor in real EKS.

Usage:

    EKS_CLUSTER_NAME=ematix-flow-test-xxxx-phase-d \\
    ECR_IMAGE=123456789.dkr.ecr.us-east-2.amazonaws.com/ematix-flow-test-xxxx/flow-worker:validation \\
    AWS_REGION=us-east-2 \\
    python test_k8s_real.py

Exits non-zero if any assertion fails.

Pre-reqs:
  - aws CLI configured with credentials that can read the cluster.
  - `aws eks update-kubeconfig --name $EKS_CLUSTER_NAME` was run
    (this script does that automatically if kubeconfig is missing).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config


def _ensure_kubeconfig(cluster: str, region: str) -> None:
    """Ensure kubeconfig points at the EKS cluster."""
    try:
        k8s_config.load_kube_config()
        # Confirm current-context is the right cluster.
        ctx = subprocess.check_output(
            ["kubectl", "config", "current-context"], text=True
        ).strip()
        if cluster not in ctx:
            raise RuntimeError(f"current context {ctx!r} doesn't match cluster {cluster}")
    except Exception:
        print(f"==> Setting up kubeconfig for {cluster}")
        subprocess.check_call([
            "aws", "eks", "update-kubeconfig",
            "--name", cluster,
            "--region", region,
        ])
        k8s_config.load_kube_config()


def _render_manifest(template: str, name: str, image: str, region: str,
                     campaign: str) -> dict:
    """Stamp placeholders in the YAML template, parse to a dict."""
    import yaml

    body = template
    body = body.replace("PLACEHOLDER_JOB_NAME", name)
    body = body.replace("PLACEHOLDER_IMAGE", image)
    body = body.replace("PLACEHOLDER_REGION", region)
    body = body.replace("PLACEHOLDER_CAMPAIGN", campaign)
    return yaml.safe_load(body)


def main() -> int:
    cluster = os.environ.get("EKS_CLUSTER_NAME")
    image = os.environ.get("ECR_IMAGE")
    region = os.environ.get("AWS_REGION", "us-east-2")
    if not cluster or not image:
        print("ERROR: EKS_CLUSTER_NAME + ECR_IMAGE must be set", file=sys.stderr)
        return 2

    print(f"==> K8s smoke: cluster={cluster} image={image}")
    _ensure_kubeconfig(cluster, region)

    here = os.path.dirname(__file__)
    template_path = os.path.normpath(os.path.join(here, "..", "k8s", "job-template.yaml"))
    with open(template_path) as f:
        template = f.read()

    job_name = f"flow-validation-{uuid.uuid4().hex[:8]}"
    campaign = uuid.uuid4().hex[:8]
    manifest = _render_manifest(template, job_name, image, region, campaign)

    batch = k8s_client.BatchV1Api()
    core = k8s_client.CoreV1Api()

    print(f"==> Creating Job {job_name}")
    submit_t0 = time.monotonic()
    batch.create_namespaced_job(namespace="default", body=manifest)

    # Poll for completion (succeeded or failed). Cap at 5 min.
    deadline = time.monotonic() + 300
    final_status = None
    while time.monotonic() < deadline:
        job = batch.read_namespaced_job_status(name=job_name, namespace="default")
        s = job.status
        if s.succeeded:
            final_status = "succeeded"
            break
        if s.failed:
            final_status = "failed"
            break
        time.sleep(2)
    submit_to_done_s = time.monotonic() - submit_t0

    if final_status is None:
        print(f"FAIL: Job did not complete in 5 minutes", file=sys.stderr)
        return 1

    print(f"    Job {final_status} in {submit_to_done_s:.1f}s")

    # Grab the pod's logs for the campaign record.
    pods = core.list_namespaced_pod(
        namespace="default",
        label_selector=f"job-name={job_name}",
    )
    logs = ""
    if pods.items:
        pod_name = pods.items[0].metadata.name
        try:
            logs = core.read_namespaced_pod_log(name=pod_name, namespace="default")
        except Exception as e:
            logs = f"(log fetch failed: {e})"
    print("--- pod logs ---")
    print(logs)
    print("--- end pod logs ---")

    # Cleanup the Job (TTL would handle it but cleanup keeps the
    # namespace tidy mid-campaign).
    batch.delete_namespaced_job(
        name=job_name,
        namespace="default",
        body=k8s_client.V1DeleteOptions(propagation_policy="Background"),
    )

    if final_status != "succeeded":
        print(f"FAIL: Job did not succeed (status={final_status})", file=sys.stderr)
        return 1

    summary = {
        "ok": True,
        "submit_to_done_s": round(submit_to_done_s, 2),
        "cluster": cluster,
        "image": image,
        "region": region,
        "pod_log_excerpt": logs[:200] if logs else "",
    }
    print("=== K8S SUMMARY ===")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
