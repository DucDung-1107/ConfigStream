"""Generate private local-demo credentials and submit them without logging values."""

import json
import os
import secrets
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def main():
    directory = ROOT / ".cache/phase20-k8s"
    path = directory / "demo-credentials.json"
    if path.exists():
        values = json.loads(path.read_text())
    else:
        values = {
            "MINIO_ROOT_USER": "cs-owned-demo",
            "MINIO_ROOT_PASSWORD": secrets.token_urlsafe(36),
            "ELASTICSEARCH_PASSWORD": secrets.token_urlsafe(36),
        }
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump(values, output)
    for namespace in ("cs-infra", "cs-processing", "cs-app"):
        data = {name: value for name, value in values.items() if name.startswith("MINIO")}
        if namespace == "cs-app":
            data.update(
                ELASTICSEARCH_USERNAME="elastic",
                ELASTICSEARCH_PASSWORD=values["ELASTICSEARCH_PASSWORD"],
            )
        document = dict(
            apiVersion="v1",
            kind="Secret",
            metadata=dict(name="minio-credentials", namespace=namespace),
            type="Opaque",
            stringData=data,
        )
        subprocess.run(
            [str(ROOT / ".tools/k8s/kubectl"), "apply", "-f", "-"],
            input=json.dumps(document),
            text=True,
            check=True,
            stdout=subprocess.DEVNULL,
        )


if __name__ == "__main__":
    main()
