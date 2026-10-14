"""Bootstrap owned Elasticsearch and an API account with read-only index privileges."""

import argparse
import json
import os
import secrets
from urllib.parse import urlsplit

from create_credentials import ROOT
from submit import apply


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--initialize-url")
    args = parser.parse_args()
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
    if args.initialize_url:
        import httpx

        from search.client import initialize_indices
        from search.config import SearchConfig

        endpoint = urlsplit(args.initialize_url)
        if endpoint.hostname != "127.0.0.1" or endpoint.scheme != "http":
            raise ValueError("Only explicit local port-forward bootstrap endpoint permitted")
        with httpx.Client(
            base_url=args.initialize_url,
            auth=("elastic", existing["ELASTICSEARCH_PASSWORD"]),
            timeout=15,
        ) as client:
            client.put(
                "/_security/role/cs-api-reader",
                json={
                    "cluster": ["monitor"],
                    "indices": [
                        {"names": ["cs20k1-*"], "privileges": ["read", "view_index_metadata"]}
                    ],
                },
            ).raise_for_status()
            client.put(
                "/_security/user/cs-api",
                json={"password": values["ELASTICSEARCH_PASSWORD"], "roles": ["cs-api-reader"]},
            ).raise_for_status()
        initialize_indices(
            SearchConfig(
                url=args.initialize_url,
                index_prefix="cs20k1",
                username="elastic",
                password=existing["ELASTICSEARCH_PASSWORD"],
            )
        )


if __name__ == "__main__":
    main()
