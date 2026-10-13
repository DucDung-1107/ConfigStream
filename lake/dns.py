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
        or descriptor.get("artifact_sha256") != artifact
        or descriptor.get("entity_coverage") != "unknown"
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
