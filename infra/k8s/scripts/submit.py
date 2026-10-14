"""Submit only owned-fixture native Kubernetes Spark jobs inside the registered budget."""

import argparse
import json
import subprocess
from pathlib import Path
from time import time

import yaml

ROOT = Path(__file__).resolve().parents[3]


def pod_spec(image, *, spark=False):
    spec = {
        "serviceAccountName": "spark" if spark else "default",
        "automountServiceAccountToken": spark,
        "restartPolicy": "Never",
        "securityContext": {
            "runAsUser": 185,
            "runAsGroup": 185,
            "runAsNonRoot": True,
            "fsGroup": 185,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "containers": [
            {
                "name": "spark-kubernetes-driver" if spark else "fixture",
                "image": image,
                "imagePullPolicy": "Never",
                "envFrom": [
                    {"configMapRef": {"name": "configstream-env"}},
                    {"secretRef": {"name": "minio-credentials"}},
                ],
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "capabilities": {"drop": ["ALL"]},
                },
                "resources": {
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                    "limits": {"cpu": "1", "memory": "1536Mi"},
                },
                "volumeMounts": [{"name": "evidence", "mountPath": "/evidence"}],
            }
        ],
        "volumes": [
            {"name": "evidence", "persistentVolumeClaim": {"claimName": "fixture-evidence"}}
        ],
    }
    return spec


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("seed", "stream", "batch"))
    parser.add_argument("--image", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    private = ROOT / ".cache/phase20-k8s"
    receipt = json.loads((private / "execution-receipt.json").read_text())
    if time() - receipt["started_epoch"] >= receipt["cap_seconds"]:
        raise RuntimeError("Registered campaign cap exhausted")
    if ":latest" in args.image or not args.image.startswith("configstream-k8s:"):
        raise ValueError("Only versioned locally imported fixture application image allowed")
    guard = json.loads((private / "resource-watch.json").read_text())
    if guard["failure"] or not guard["samples"]:
        raise RuntimeError("Fail-closed resource sampling unavailable")
    spec = pod_spec(args.image, spark=args.mode != "seed")
    container = spec["containers"][0]
    if args.mode == "seed":
        container["command"] = ["python", "/opt/configstream/application.py", "seed"]
    else:
        template = dict(apiVersion="v1", kind="Pod", spec=pod_spec(args.image, spark=True))
        template["spec"]["containers"][0]["name"] = "spark-kubernetes-executor"
        template["spec"]["volumes"].append(
            {"name": "template", "configMap": {"name": "spark-template"}}
        )
        template["spec"]["containers"][0]["volumeMounts"].append(
            {"name": "template", "mountPath": "/template"}
        )
        template_yaml = yaml.safe_dump(template)
        template_path = private / "spark-pod-template.yaml"
        template_path.write_text(template_yaml)
        subprocess.run(
            [
                str(ROOT / ".tools/k8s/kubectl"),
                "create",
                "configmap",
                "spark-template",
                "-n",
                "cs-processing",
                "--from-file=template.yaml=" + str(template_path),
                "--dry-run=client",
                "-o",
                "yaml",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        manifest = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "spark-template", "namespace": "cs-processing"},
            "data": {"template.yaml": template_yaml},
        }
        apply(manifest)
        spec["volumes"].append({"name": "template", "configMap": {"name": "spark-template"}})
        container["volumeMounts"].append({"name": "template", "mountPath": "/template"})
        configuration = {
            "spark.kubernetes.namespace": "cs-processing",
            "spark.kubernetes.container.image": args.image,
            "spark.kubernetes.container.image.pullPolicy": "Never",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.kubernetes.driver.podTemplateFile": "/template/template.yaml",
            "spark.kubernetes.executor.podTemplateFile": "/template/template.yaml",
            "spark.kubernetes.driver.podTemplateContainerName": "spark-kubernetes-executor",
            "spark.kubernetes.executor.podTemplateContainerName": "spark-kubernetes-executor",
            "spark.kubernetes.driver.label.configstream-owned": "phase20k",
            "spark.kubernetes.executor.label.configstream-owned": "phase20k",
            "spark.kubernetes.driver.request.cores": "250m",
            "spark.kubernetes.executor.request.cores": "500m",
            "spark.driver.memory": "768m",
            "spark.executor.memory": "768m",
            "spark.driver.memoryOverhead": "384m",
            "spark.executor.memoryOverhead": "384m",
            "spark.executor.instances": "1",
            "spark.executor.cores": "1",
            "spark.pyspark.python": "/opt/configstream/.venv/bin/python",
            "spark.kubernetes.submission.waitAppCompletion": "true",
        }
        container["command"] = [
            "/opt/spark/bin/spark-submit",
            "--master",
            "k8s://https://kubernetes.default.svc:443",
            "--deploy-mode",
            "cluster",
            "--name",
            f"cs-{args.mode}-{args.run_id}",
        ]
        for name, value in configuration.items():
            container["command"].extend(["--conf", f"{name}={value}"])
        container["command"].extend(["local:///opt/configstream/application.py", args.mode])
    document = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"cs-{args.mode}-{args.run_id}",
            "namespace": "cs-processing",
            "labels": {"configstream-owned": "phase20k"},
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": min(
                600, int(receipt["cap_seconds"] - (time() - receipt["started_epoch"]))
            ),
            "template": {"metadata": {"labels": {"configstream-owned": "phase20k"}}, "spec": spec},
        },
    }
    (private / f"{args.mode}-{args.run_id}.yaml").write_text(yaml.safe_dump(document))
    apply(document)


def apply(document):
    subprocess.run(
        [str(ROOT / ".tools/k8s/kubectl"), "apply", "-f", "-"],
        input=json.dumps(document),
        text=True,
        check=True,
    )


if __name__ == "__main__":
    main()
