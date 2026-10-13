"""Per-artifact analytical barriers and exact reference/value reconstruction."""

import json
from contextlib import ExitStack

from lake.ris import metadata_digest
from spark.dns import committed_raw, digest, observed_groups, schema, verify_projection
from workloads.dns_oracle import group_schema, raw_schema


def link_schema():
    from pyspark.sql.types import StructType

    return StructType().add("group_id", "string").add("logical_record_id", "string")


def read_table(session, store, query, artifact, table):
    from pyspark.sql.types import StructType

    commit = store.get(f"tables/{query}/{table}/{artifact}")
    if (
        not commit
        or commit.get("format_version") != "dns-analytical-1.0"
        or commit.get("table") != table
        or commit.get("artifact_sha256") != artifact
    ):
        raise ValueError("DNS analytical table not committed")
    expected = store.path(f"{query}/{artifact}", table)
    if commit["path"] != expected:
        raise ValueError("DNS analytical path conflict")
    frame = session.read.schema(StructType.fromJson(commit["schema"])).parquet(expected)
    if digest(frame, commit["sort_columns"]) != (commit["rows"], commit["projection_sha256"]):
        raise ValueError("DNS analytical projection corruption")
    return frame


def publish_table(session, store, query, artifact, table, frame, sort_columns):
    count, checksum = digest(frame, sort_columns)
    commit = dict(
        format_version="dns-analytical-1.0",
        query_id=query,
        artifact_sha256=artifact,
        table=table,
        path=store.path(f"{query}/{artifact}", table),
        rows=count,
        projection_sha256=checksum,
        sort_columns=sort_columns,
        schema=frame.schema.jsonValue(),
    )
    key = f"tables/{query}/{table}/{artifact}"
    previous = store.get(key)
    if previous is not None:
        if previous != commit:
            raise ValueError("DNS analytical immutable replay conflict")
    else:
        frame.repartition(2).write.mode("overwrite").parquet(commit["path"])
        store.put(key, commit)
    read_table(session, store, query, artifact, table)
    return commit


def finalize_comparison(
    session, store, query, selected, oracle, differences, *, check=lambda: None
):
    from hashlib import sha256

    from lake.dns import read_source_completion
    from workloads.dns_oracle import comparison_ledger

    artifacts = [day["artifact"]["sha256"] for day in selected]
    if len(artifacts) != 2 or len(set(artifacts)) != 2:
        raise ValueError("DNS comparison requires two distinct selected sources")
    sources = [read_source_completion(store, query, artifact) for artifact in artifacts]
    identity = sha256(json.dumps(artifacts, separators=(",", ":")).encode()).hexdigest()
    path = oracle / "comparison-ledger.parquet"
    if not path.exists():
        comparison_ledger(oracle / selected[0]["date"], oracle / selected[1]["date"], path)
    expected = session.read.parquet(str(path)).select(*differences.columns)
    with verify_projection(differences, expected, complete=True) as checked:
        check()
        commit = publish_table(
            session, store, query, identity, "differences", checked, ["group_key"]
        )
        qualifications = checked.select(
            "group_key",
            "previous_id",
            "current_id",
            "strict_qualification",
            "same_recorded_workers",
        )
        qualification = publish_table(
            session, store, query, identity, "qualifications", qualifications, ["group_key"]
        )
        check()
        descriptor = dict(
            format_version="dns-comparison-complete-1.0",
            query_id=query,
            artifact_sha256s=artifacts,
            comparison_id=identity,
            source_completion_sha256s=[metadata_digest(source) for source in sources],
            difference_commit_sha256=metadata_digest(commit),
            qualification_commit_sha256=metadata_digest(qualification),
            exact_independent_projection=True,
            causal_interpretation="unknown",
            entity_coverage="unknown",
            strict_matched_vantage_eligible=0,
        )
        store.put(f"comparison/{query}/{identity}", descriptor)
        return descriptor


def finalize_source(
    session, store, query, reference, oracle, *, check=lambda: None, fault=lambda point: None
):
    from time import monotonic

    from pyspark.sql import functions as functions

    began = monotonic()
    artifact = reference.manifest.artifacts[0].sha256
    expected = session.read.schema(schema(raw_schema())).parquet(str(oracle / "raw.parquet"))
    summary = json.loads((oracle / "summary.json").read_text())
    all_expected = expected
    with ExitStack() as contexts:
        all_raw = contexts.enter_context(
            committed_raw(session, store, query, all_expected, artifacts=[artifact])
        )
        source = all_raw.where(functions.col("artifact_sha256") == artifact)
        valid = contexts.enter_context(verify_projection(source, expected, complete=True))
        raw_count, raw_digest = digest(valid)
        if (raw_count, raw_digest) != (
            summary["source_rows"],
            summary["raw_projection_sha256"],
        ) or digest(valid, ["row_group", "row"])[1] != summary["source_order_sha256"]:
            raise ValueError("DNS source identity/projection oracle mismatch")
        generated, selected = observed_groups(valid, reference)
        expected_groups = session.read.schema(schema(group_schema())).parquet(
            str(oracle / "groups.parquet")
        )
        groups = contexts.enter_context(
            verify_projection(generated, expected_groups, complete=True)
        )
        group_count, group_digest = digest(groups)
        if (group_count, group_digest) != (
            summary["address_groups"],
            summary["group_projection_sha256"],
        ):
            raise ValueError("DNS group oracle mismatch")
        links = selected.join(
            groups.select("group_id", "query_name", "query_type", "response_name", "response_type"),
            ["query_name", "query_type", "response_name", "response_type"],
        ).select("group_id", "logical_record_id")
        expected_links = session.read.schema(link_schema()).parquet(
            str(oracle / "contributors.parquet")
        )
        if (
            links.exceptAll(expected_links).limit(1).count()
            or expected_links.exceptAll(links).limit(1).count()
        ):
            raise ValueError("DNS contributor references lost/added")
        values = groups.select("value_set_id", "addresses_json").distinct()
        reused = groups.drop("addresses_json")
        rebuilt = reused.join(values, "value_set_id").select(*groups.columns)
        with verify_projection(rebuilt, expected_groups, complete=True) as reconstructed:
            if digest(reconstructed) != (group_count, group_digest):
                raise ValueError("DNS value reuse reconstruction mismatch")
        check()
        fault("before_groups")
        commits = {}
        for table, frame, columns in [
            ("groups", groups, ["group_id"]),
            ("contributors", links, ["group_id", "logical_record_id"]),
            ("values", values, ["value_set_id"]),
            ("reuse", reused, ["group_id"]),
        ]:
            commits[table] = publish_table(session, store, query, artifact, table, frame, columns)
            check()
            fault("after_group_table")
        inventory = sorted(
            [row for row in store.inventory(query) if artifact in row["source_artifact_sha256s"]],
            key=lambda row: row["batch_id"],
        )
        descriptor = dict(
            format_version="dns-complete-source-1.1",
            query_id=query,
            artifact_sha256=artifact,
            provenance_manifest_id=reference.manifest.manifest_id,
            source_rows=raw_count,
            source_order_sha256=summary["source_order_sha256"],
            raw_projection_sha256=raw_digest,
            address_rows=summary["selected_address_rows"],
            address_groups=group_count,
            group_projection_sha256=group_digest,
            entity_coverage="unknown",
            answer_coverage="unknown",
            table_commits={key: metadata_digest(value) for key, value in commits.items()},
            inventory_sha256=metadata_digest(inventory),
            real_proven_empty_rows=0,
        )
        check()
        fault("before_completion")
        store.put(f"completion/{query}/{artifact}", descriptor)
        return descriptor | dict(
            finalization_seconds=monotonic() - began,
            value_reuse_reconstruction="exact",
            contributor_reconstruction="exact",
        )
