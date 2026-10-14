"""Bootstrap owned Elasticsearch and an API account with read-only index privileges."""

import json
import os
import secrets

from create_credentials import ROOT
from submit import apply


def main():
    private = ROOT / ".cache/phase20-k8s"
    existing = json.loads((private / "demo-credentials.json").read_text())
    path = private / "closure-serving-credentials.json"
    if path.exists():
        values = json.loads(path.read_text())
    else:
        values = dict(
            ELASTICSEARCH_USERNAME="cs-api", ELASTICSEARCH_PASSWORD=secrets.token_urlsafe(36)
        )
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump(values, output)
    for namespace, name, data in (
        (
            "cs-app",
            "closure-es-bootstrap",
            {"ELASTIC_PASSWORD": existing["ELASTICSEARCH_PASSWORD"]},
        ),
        (
            "cs-processing",
            "closure-search-writer",
            {
                "ELASTICSEARCH_USERNAME": "elastic",
                "ELASTICSEARCH_PASSWORD": existing["ELASTICSEARCH_PASSWORD"],
            },
        ),
        ("cs-app", "closure-search-reader", values),
    ):
        apply(
            dict(
                apiVersion="v1",
                kind="Secret",
                metadata=dict(name=name, namespace=namespace),
                type="Opaque",
                stringData=data,
            )
        )


if __name__ == "__main__":
    main()
