"""Prepare isolated owned-fixture configuration or a read-only evidence inspector."""

import argparse
import json
import re
import subprocess

import yaml
from create_credentials import ROOT
from safety import validate_live_campaign
from submit import apply, pod_spec


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--campaign-directory", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--search", action="store_true")
    parser.add_argument("--inspector", action="store_true")
    args = parser.parse_args()
    validate_live_campaign(args.campaign_directory)
    if not re.fullmatch(r"closure-[a-z0-9-]+", args.prefix):
        raise ValueError("Only explicitly isolated owned closure prefixes allowed")
    if not re.fullmatch(r"[a-z0-9-]+", args.environment):
        raise ValueError("Invalid environment name")
    config = yaml.safe_load((ROOT / "infra/k8s/overlays/closure/configuration.yaml").read_text())
    config["metadata"]["name"] = args.environment
    config["data"].update(
        CONFIGSTREAM_LAKE_PREFIX=args.prefix,
        CONFIGSTREAM_DEMO_EVIDENCE_DIR=f"/evidence/{args.prefix}",
        SPARK_CHECKPOINT_DIR=f"/evidence/{args.prefix}/checkpoint",
    )
    if args.search:
        config["data"].update(
            CONFIGSTREAM_SEARCH_ENABLED="true",
            ELASTICSEARCH_URL="http://elasticsearch.cs-app.svc.cluster.local:9200",
            CONFIGSTREAM_SEARCH_INDEX_PREFIX="cs20k1",
        )
    executable = str(ROOT / ".tools/k8s/kubectl")
    existing = subprocess.run(
        [executable, "get", "configmap", args.environment, "-n", "cs-processing", "-o", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if existing.returncode == 0:
        if json.loads(existing.stdout)["data"] != config["data"]:
            raise RuntimeError("Existing source/checkpoint configuration cannot be changed")
    elif "NotFound" not in existing.stderr:
        raise RuntimeError("Cannot safely establish configuration availability")
    apply(config)
    if args.inspector:
        versions = json.loads((ROOT / "infra/k8s/versions.json").read_text())
        spec = pod_spec(
            "configstream-k8s:" + versions["accepted_application_source_sha"],
            environment=args.environment,
        )
        spec["containers"][0]["command"] = ["python", "-c", "import time; time.sleep(3600)"]
        subprocess.run(
            [executable, "create", "-f", "-"],
            input=yaml.safe_dump(
                dict(
                    apiVersion="v1",
                    kind="Pod",
                    metadata=dict(name="closure-inspector", namespace="cs-processing"),
                    spec=spec,
                )
            ),
            check=True,
            text=True,
        )


if __name__ == "__main__":
    main()
