"""Isolated immutable DNS metadata and physical projections; existing formats untouched."""

from lake.ris import EventStore, metadata_digest, safe_key

TABLES = {
    "raw": "temporal/dns/measurements/physical-v1.1",
    "groups": "temporal/dns/observed-groups/physical-v1",
    "contributors": "temporal/dns/contributors/physical-v1",
    "values": "temporal/dns/value-sets/physical-v1",
    "reuse": "temporal/dns/reused-groups/physical-v1",
    "differences": "temporal/dns/observed-differences/physical-v1",
    "qualifications": "temporal/dns/comparison-qualifications/physical-v1",
}


class DnsStore(EventStore):
    def inventory(self, query_id):
        safe_key(query_id)
        prefix = f"_dns_commits/v1/batches/{query_id}/"
        if self.root:
            return [
                self.get(f"batches/{query_id}/{path.stem}")
                for path in sorted((self.root / prefix).glob("*.json"))
            ]
        from lake.client import s3_client

        result = []
        for page in (
            s3_client(self.config)
            .get_paginator("list_objects_v2")
            .paginate(Bucket=self.config.bucket, Prefix=self.config.object_key(prefix))
        ):
            for row in page.get("Contents", []):
                if len(result) >= 10000:
                    raise ValueError("DNS commit inventory bound")
                name = row["Key"].removeprefix(self.config.object_key("_dns_commits/v1/"))
                result.append(self.get(name.removesuffix(".json")))
        return result

    def path(self, identity, table="raw"):
        safe_key(identity)
        if table not in TABLES:
            raise ValueError("Unregistered DNS physical table")
        suffix = f"{TABLES[table]}/{identity}"
        return str(self.root / suffix) if self.root else self.config.uri(suffix)

    def key(self, key):
        safe_key(key)
        return f"_dns_commits/v1/{key}.json"

    def committed_path(self, commit, query_id):
        if (
            commit.get("format_version") != "dns-source-1.1"
            or commit.get("query_id") != query_id
            or type(commit.get("batch_id")) is not int
            or commit["batch_id"] < 0
        ):
            raise ValueError("DNS committed schema/identity mismatch")
        path = self.path(f"{query_id}/{commit['batch_id']:020d}")
        if commit.get("path") != path:
            raise ValueError("DNS committed path mismatch")
        return path


def read_source_completion(store, query_id, artifact):
    from schemas.dns import DnsReference

    descriptor = store.get(f"completion/{query_id}/{artifact}")
    if (
        not descriptor
        or descriptor.get("format_version") != "dns-complete-source-1.1"
        or descriptor.get("query_id") != query_id
        or descriptor.get("artifact_sha256") != artifact
        or descriptor.get("entity_coverage") != "unknown"
        or descriptor.get("answer_coverage") != "unknown"
        or set(descriptor.get("table_commits", {})) != {"groups", "contributors", "values", "reuse"}
    ):
        raise ValueError("DNS source completion missing/invalid")
    source = DnsReference.model_validate(
        store.get(f"sources/{descriptor['provenance_manifest_id']}")
    )
    if source.manifest.artifacts[0].sha256 != artifact or descriptor["source_rows"] != sum(
        source.row_groups
    ):
        raise ValueError("DNS selected source coverage mismatch")
    if descriptor["inventory_sha256"] != metadata_digest(
        sorted(
            [
                row
                for row in store.inventory(query_id)
                if artifact in row["source_artifact_sha256s"]
            ],
            key=lambda row: row["batch_id"],
        )
    ):
        raise ValueError("DNS source committed inventory mismatch")
    for table, checksum in descriptor["table_commits"].items():
        commit = store.get(f"tables/{query_id}/{table}/{artifact}")
        if (
            table not in {"groups", "contributors", "values", "reuse"}
            or commit is None
            or metadata_digest(commit) != checksum
        ):
            raise ValueError("DNS completed group/value evidence missing/corrupt")
    return descriptor


def read_window(store, query_id, identity):
    window = store.get(f"windows/{query_id}/{identity}")
    if (
        not window
        or window.get("format_version") != "dns-selected-window-1.0"
        or window.get("query_id") != query_id
        or window.get("comparison_id") != identity
        or len(window.get("artifact_sha256s", [])) != 2
        or len(set(window["artifact_sha256s"])) != 2
        or window.get("entity_coverage") != "unknown"
    ):
        raise ValueError("DNS accepted selected-window metadata missing/invalid")
    comparison = store.get(f"comparison/{query_id}/{identity}")
    if not comparison or metadata_digest(comparison) != window["comparison_sha256"]:
        raise ValueError("DNS comparison binding missing/corrupt")
    if window["inventory_sha256"] != metadata_digest(
        sorted(store.inventory(query_id), key=lambda row: row["batch_id"])
    ):
        raise ValueError("DNS selected-window inventory changed")
    for artifact in window["artifact_sha256s"]:
        descriptor = read_source_completion(store, query_id, artifact)
        source = store.get(f"sources/{descriptor['provenance_manifest_id']}")
        if (
            metadata_digest(descriptor) != window["source_completion_sha256s"][artifact]
            or metadata_digest(source) != window["source_reference_sha256s"][artifact]
        ):
            raise ValueError("DNS source wrapper/completion binding corrupt")
    for table, field in [
        ("differences", "difference_commit_sha256"),
        ("qualifications", "qualification_commit_sha256"),
    ]:
        commit = store.get(f"tables/{query_id}/{table}/{identity}")
        if not commit or metadata_digest(commit) != comparison[field]:
            raise ValueError("DNS selected-window analytical commit missing/corrupt")
    return window


def publish_window(store, query_id, comparison):
    if (
        comparison.get("format_version") != "dns-comparison-complete-1.0"
        or comparison.get("query_id") != query_id
        or comparison.get("exact_independent_projection") is not True
    ):
        raise ValueError("DNS exact accepted comparison required")
    artifacts = comparison["artifact_sha256s"]
    if len(artifacts) != 2 or len(set(artifacts)) != 2:
        raise ValueError("DNS selected-window source scope invalid")
    inventory = sorted(store.inventory(query_id), key=lambda row: row["batch_id"])
    if any(not set(row["source_artifact_sha256s"]) <= set(artifacts) for row in inventory):
        raise ValueError("DNS unexpected artifact in selected-window inventory")
    descriptors = [read_source_completion(store, query_id, artifact) for artifact in artifacts]
    if [metadata_digest(row) for row in descriptors] != comparison["source_completion_sha256s"]:
        raise ValueError("DNS comparison source completion changed")
    identity = comparison["comparison_id"]
    window = dict(
        format_version="dns-selected-window-1.0",
        query_id=query_id,
        comparison_id=identity,
        artifact_sha256s=artifacts,
        comparison_sha256=metadata_digest(comparison),
        inventory_sha256=metadata_digest(inventory),
        source_completion_sha256s={
            artifact: metadata_digest(row)
            for artifact, row in zip(artifacts, descriptors, strict=True)
        },
        source_reference_sha256s={
            artifact: metadata_digest(store.get(f"sources/{row['provenance_manifest_id']}"))
            for artifact, row in zip(artifacts, descriptors, strict=True)
        },
        logical_source_rows=sum(row["source_rows"] for row in descriptors),
        entity_coverage="unknown",
        answer_coverage="unknown",
        causal_interpretation="unknown",
    )
    store.put(f"windows/{query_id}/{identity}", window)
    return read_window(store, query_id, identity)
