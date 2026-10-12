"""Dedicated RPKI DataFrame path; no configuration version buffers or router rules."""

import json
from datetime import timedelta
from hashlib import sha256
from uuid import UUID

from pyspark.sql import functions as functions

from lake.rpki import validated_paths
from schemas.rpki import DayReference, member_encoding, parse_chunk
from schemas.temporal.identity import canonical_bytes, logical_identity

ROW_SCHEMA = (
    "observation_id STRING, provenance_id STRING, artifact_sha256 STRING, "
    "observation_date STRING, scope STRING, row_index LONG, row_id STRING, "
    "membership_id STRING, member_encoding STRING, asn LONG, prefix STRING, "
    "max_length INT, payload_digest STRING"
)
MEMBER_COLUMNS = ["membership_id", "member_encoding", "scope", "asn", "prefix", "max_length"]
MEMBER_SCHEMA = (
    "membership_id STRING, member_encoding STRING, scope STRING, "
    "asn LONG, prefix STRING, max_length INT"
)
RAW_DIGEST_COLUMNS = [field.strip().split()[0] for field in ROW_SCHEMA.split(",")]


def decode_chunks(frame, references):
    serialized = {key: value.model_dump_json() for key, value in references.items()}

    def expand(partition):
        catalog = {
            key: DayReference.model_validate_json(value) for key, value in serialized.items()
        }
        for message in partition:
            try:
                chunk = parse_chunk(message.value, catalog)
            except ValueError:
                raise ValueError("Invalid RPKI chunk; failed batch remains unpublished") from None
            for row in chunk.rows:
                encoded = member_encoding(row.member).decode("utf-8")
                yield (
                    chunk.observation_id,
                    chunk.provenance_id,
                    chunk.artifact_sha256,
                    chunk.observation_date.isoformat(),
                    row.member.scope,
                    row.row_index,
                    row.row_id,
                    row.member.membership_id,
                    encoded,
                    row.member.asn,
                    row.member.prefix,
                    row.member.max_length,
                    sha256(canonical_bytes(row.model_dump(mode="json"))).hexdigest(),
                )

    return frame.sparkSession.createDataFrame(
        frame.select("value").rdd.mapPartitions(expand), ROW_SCHEMA
    )


def reconcile_rows(frame):
    if (
        frame.filter(
            functions.greatest(
                *[functions.col(name).isNull().cast("int") for name in frame.columns]
            )
            == 1
        )
        .limit(1)
        .count()
    ):
        raise ValueError("Null field in committed source-row projection")
    member = functions.struct(
        functions.col("asn"),
        functions.col("max_length"),
        functions.col("prefix"),
        functions.col("scope"),
    )
    payload = functions.to_json(
        functions.struct(
            member.alias("member"), functions.col("row_id"), functions.col("row_index")
        )
    )
    if (
        frame.filter(functions.sha2(payload, 256) != functions.col("payload_digest"))
        .limit(1)
        .count()
    ):
        raise ValueError("Raw source payload checksum mismatch")
    if (
        frame.groupBy("row_id")
        .agg(functions.countDistinct(functions.struct(*frame.columns)).alias("variants"))
        .filter("variants != 1")
        .limit(1)
        .count()
    ):
        raise ValueError("Conflicting source-row retry")
    return frame.dropDuplicates(["row_id"])


def projection_digest(frame, *, columns, order, seed=b"rpki-rows-v1\n"):
    digest = sha256(seed)
    for row in frame.select(*columns).orderBy(*order).toLocalIterator(prefetchPartitions=False):
        encoded = canonical_bytes(list(row))
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def state_digest(members):
    digest = sha256(b"rpki-set-v1\n")
    for row in (
        members.select("member_encoding")
        .orderBy("member_encoding")
        .toLocalIterator(prefetchPartitions=False)
    ):
        encoded = row.member_encoding.encode("utf-8")
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def write_parquet(frame, path):
    frame.write.mode("overwrite").option("compression", "snappy").option(
        "maxRecordsPerFile", 100000
    ).parquet(path)


def publish_raw(frame, store, query_id, batch_id):
    UUID(query_id)
    if type(batch_id) is not int or batch_id < 0:
        raise ValueError("Invalid RPKI batch identity")
    rows = reconcile_rows(frame).persist()
    try:
        count = rows.count()
        if not count:
            return None
        digest = projection_digest(rows, columns=RAW_DIGEST_COLUMNS, order=["row_id"])
        identity = f"{query_id}/{batch_id:020d}"
        commit = dict(
            format_version="rpki-1.0",
            dataset="source_rows",
            identity=identity,
            path=store.path("source_rows", identity),
            rows=count,
            digest=digest,
            digest_policy="full-source-row-v1",
            observation_ids=sorted(
                row.observation_id
                for row in rows.select("observation_id").distinct().limit(33).collect()
            ),
        )
        previous = store.get(f"raw/{identity}")
        if previous is not None:
            if previous != commit:
                raise ValueError("RPKI raw batch replay conflict")
            return previous
        write_parquet(rows, commit["path"])
        store.put(f"raw/{identity}", commit)
        return commit
    finally:
        rows.unpersist()


def committed_rows(session, store, query_id, *, observation_ids=None):
    UUID(query_id)
    paths = []
    for commit in store.list(f"raw/{query_id}"):
        if (
            observation_ids is not None
            and "observation_ids" in commit
            and not set(commit["observation_ids"]).intersection(observation_ids)
        ):
            continue
        path = validated_paths(store, commit, "source_rows")
        if commit.get("digest_policy") != "full-source-row-v1":
            raise ValueError("Unsupported raw ledger digest policy; use a new validated query")
        frame = session.read.schema(ROW_SCHEMA).parquet(path)
        if (
            frame.count() != commit["rows"]
            or projection_digest(frame, columns=RAW_DIGEST_COLUMNS, order=["row_id"])
            != commit["digest"]
        ):
            raise ValueError("Committed raw ledger checksum mismatch")
        paths.append(path)
    frame = (
        session.read.schema(ROW_SCHEMA).parquet(*paths)
        if paths
        else session.createDataFrame([], ROW_SCHEMA)
    )
    if observation_ids is not None:
        frame = frame.filter(functions.col("observation_id").isin(*observation_ids))
    return reconcile_rows(frame)


def exact_members(actual, expected):
    keys = MEMBER_COLUMNS
    if actual.select(*keys).join(expected.select(*keys), keys, "left_anti").limit(1).count() or (
        expected.select(*keys).join(actual.select(*keys), keys, "left_anti").limit(1).count()
    ):
        raise ValueError("Canonical membership disagrees with independent oracle")


def validate_member_projection(frame):
    encoding = functions.concat(
        functions.lit('["'),
        functions.col("scope"),
        functions.lit('",'),
        functions.col("asn").cast("string"),
        functions.lit(',"'),
        functions.col("prefix"),
        functions.lit('",'),
        functions.col("max_length").cast("string"),
        functions.lit("]"),
    )
    if frame.filter(encoding != functions.col("member_encoding")).limit(1).count():
        raise ValueError("Member tuple/encoding conflict")
    preimage = functions.concat(
        functions.lit('["configstream-temporal-v1","rpki_membership","membership-v1",[["string","'),
        functions.col("scope"),
        functions.lit('"],["integer",'),
        functions.col("asn").cast("string"),
        functions.lit('],["string","'),
        functions.col("prefix"),
        functions.lit('"],["integer",'),
        functions.col("max_length").cast("string"),
        functions.lit("]]]"),
    )
    if (
        frame.filter(functions.sha2(preimage, 256) != functions.col("membership_id"))
        .limit(1)
        .count()
    ):
        raise ValueError("Member identity checksum mismatch")


def finalize_day(rows, reference, expected, store, *, expected_rows=None):
    observation_id = reference.observation_id
    selected = reconcile_rows(
        rows.filter(functions.col("observation_id") == observation_id)
    ).persist()
    try:
        stats = selected.agg(
            functions.count("*").alias("count"),
            functions.countDistinct("row_index").alias("indices"),
            functions.min("row_index").alias("first"),
            functions.max("row_index").alias("last"),
        ).first()
        wanted = reference.manifest.expected_records
        if stats["count"] < wanted:
            return None
        if (
            stats["count"] != wanted
            or stats.indices != wanted
            or stats.first != 1
            or stats.last != wanted
        ):
            raise ValueError("Daily source coordinate inventory mismatch")
        if (
            selected.filter(
                (functions.col("provenance_id") != reference.manifest.manifest_id)
                | (functions.col("artifact_sha256") != reference.manifest.artifacts[0].sha256)
                | (functions.col("scope") != reference.manifest.source_scope)
                | (
                    functions.col("observation_date")
                    != reference.observation.payload.observation_date.isoformat()
                )
            )
            .limit(1)
            .count()
        ):
            raise ValueError("Daily source provenance mismatch")
        if expected_rows is None:
            raise ValueError("Independent source-coordinate oracle required for completion")
        source_keys = ["row_index", "member_encoding"]
        if selected.select(*source_keys).join(
            expected_rows.select(*source_keys), source_keys, "left_anti"
        ).limit(1).count() or (
            expected_rows.select(*source_keys)
            .join(selected.select(*source_keys), source_keys, "left_anti")
            .limit(1)
            .count()
        ):
            raise ValueError("Source coordinate/payload map disagrees with oracle")
        members = selected.select(*MEMBER_COLUMNS).dropDuplicates().persist()
        try:
            exact_members(members, expected)
            descriptor = reference.observation.payload
            if (
                members.count() != descriptor.unique_members
                or state_digest(members) != descriptor.set_digest
            ):
                raise ValueError("Complete state digest/accounting mismatch")
            identity = observation_id
            key = f"state/{reference.manifest.source_scope}/{descriptor.observation_date}"
            commit = dict(
                format_version="rpki-1.0",
                dataset="members",
                identity=identity,
                path=store.path("members", identity),
                reference=reference.model_dump(mode="json"),
                rows=descriptor.unique_members,
                digest=descriptor.set_digest,
                source_rows=wanted,
                source_map_digest=projection_digest(
                    selected, columns=source_keys, order=["row_index"], seed=b"rpki-source-map-v1\n"
                ),
            )
            previous = store.get(key)
            if previous is not None:
                if previous != commit:
                    raise ValueError("Conflicting published same-day artifact/state")
                read_state(rows.sparkSession, store, reference)
                return previous
            write_parquet(members, commit["path"])
            store.put(key, commit)
            return commit
        finally:
            members.unpersist()
    finally:
        selected.unpersist()


def read_state(session, store, reference):
    day = reference.observation.payload.observation_date
    commit = store.get(f"state/{reference.manifest.source_scope}/{day}")
    if commit is None:
        raise ValueError("Historical state incomplete or unpublished")
    if commit["reference"] != reference.model_dump(mode="json"):
        raise ValueError("Historical state provenance conflict")
    frame = session.read.schema(MEMBER_SCHEMA).parquet(validated_paths(store, commit, "members"))
    validate_member_projection(frame)
    if (
        frame.count() != commit["rows"]
        or state_digest(frame) != reference.observation.payload.set_digest
    ):
        raise ValueError("Historical state checksum mismatch")
    return frame


def comparable(previous, current):
    old, new = previous.observation.payload, current.observation.payload
    if (
        old.observation_date + timedelta(days=1) != new.observation_date
        or old.comparison_policy != new.comparison_policy
        or old.validation_regime != new.validation_regime
        or (
            previous.manifest.source_id,
            previous.manifest.dataset_id,
            previous.manifest.source_scope,
            previous.manifest.parser_version,
            previous.manifest.canonicalization_version,
            previous.manifest.counting_policy_version,
        )
        != (
            current.manifest.source_id,
            current.manifest.dataset_id,
            current.manifest.source_scope,
            current.manifest.parser_version,
            current.manifest.canonicalization_version,
            current.manifest.counting_policy_version,
        )
    ):
        raise ValueError("RPKI observations are not comparable consecutive days")


def transition_frame(old, new):
    return (
        new.join(old.select("membership_id"), "membership_id", "left_anti")
        .withColumn("action", functions.lit("add"))
        .unionByName(
            old.join(new.select("membership_id"), "membership_id", "left_anti").withColumn(
                "action", functions.lit("remove")
            )
        )
    )


def transition_member_digest(frame):
    digest = sha256(b"rpki-transition-members-v1\n")
    for row in (
        frame.select("action", "member_encoding")
        .orderBy("action", "member_encoding")
        .toLocalIterator(prefetchPartitions=False)
    ):
        encoded = row.action.encode() + b":" + row.member_encoding.encode()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def transition_ids(frame, previous, current):
    policy = logical_identity(
        "policy_bindings",
        "v1",
        (
            current.manifest.parser_version,
            current.manifest.canonicalization_version,
            current.manifest.counting_policy_version,
        ),
    )
    constants = [
        current.manifest.source_id,
        current.manifest.source_scope,
        previous.observation_id,
        current.observation_id,
    ]
    prefix = '["configstream-temporal-v1","rpki_transition",' + json.dumps(policy) + ",["
    tagged = ",".join('["string",' + json.dumps(value) + "]" for value in constants) + ","
    preimage = functions.concat(
        functions.lit(prefix + tagged + '["string","'),
        functions.col("membership_id"),
        functions.lit('"],["string","'),
        functions.col("action"),
        functions.lit(
            '"],["string",' + json.dumps(current.observation.payload.comparison_policy) + "]]]"
        ),
    )
    return (
        frame.withColumn("logical_record_id", functions.sha2(preimage, 256))
        .withColumn("previous_id", functions.lit(previous.observation_id))
        .withColumn("current_id", functions.lit(current.observation_id))
    )


def publish_boundary(session, store, previous, current, oracle):
    comparable(previous, current)
    old, new = read_state(session, store, previous), read_state(session, store, current)
    transition = transition_ids(transition_frame(old, new), previous, current).persist()
    try:
        digest = transition_member_digest(transition)
        counts = {
            row.action: row["count"] for row in transition.groupBy("action").count().collect()
        }
        if (
            counts.get("add", 0) != oracle["additions"]
            or counts.get("remove", 0) != oracle["removals"]
            or digest != oracle["transition_member_digest"]
        ):
            raise ValueError("Exact transition membership digest disagrees with oracle")
        identity = previous.observation_id + "/" + current.observation_id
        commit = dict(
            format_version="rpki-1.0",
            dataset="transitions",
            identity=identity,
            path=store.path("transitions", identity),
            previous_id=previous.observation_id,
            current_id=current.observation_id,
            rows=sum(counts.values()),
            digest=digest,
            additions=counts.get("add", 0),
            removals=counts.get("remove", 0),
            provenance_ids=[previous.manifest.manifest_id, current.manifest.manifest_id],
            identity_digest=projection_digest(
                transition,
                columns=["logical_record_id", "action", "member_encoding"],
                order=["logical_record_id"],
            ),
        )
        key = f"boundary/{identity}"
        saved = store.get(key)
        if saved is not None:
            if saved != commit:
                raise ValueError("Conflicting transition replay")
            return saved
        write_parquet(transition, commit["path"])
        store.put(key, commit)
        return commit
    finally:
        transition.unpersist()


def reconstruct(session, store, references):
    if not references:
        raise ValueError("Reconstruction requires an accepted baseline")
    state = read_state(session, store, references[0])
    for previous, current in zip(references, references[1:], strict=False):
        comparable(previous, current)
        commit = store.get(f"boundary/{previous.observation_id}/{current.observation_id}")
        if (
            commit is None
            or commit.get("previous_id") != previous.observation_id
            or commit.get("current_id") != current.observation_id
        ):
            raise ValueError("Missing or incorrect reconstruction boundary")
        transitions = session.read.parquet(validated_paths(store, commit, "transitions"))
        validate_member_projection(transitions)
        if (
            transitions.count() != commit["rows"]
            or transition_member_digest(transitions) != commit["digest"]
            or projection_digest(
                transitions,
                columns=["logical_record_id", "action", "member_encoding"],
                order=["logical_record_id"],
            )
            != commit["identity_digest"]
        ):
            raise ValueError("Reconstruction transition checksum mismatch")
        removed = transitions.filter("action = 'remove'")
        added = transitions.filter("action = 'add'")
        if removed.join(state.select("membership_id"), "membership_id", "left_anti").limit(
            1
        ).count() or (
            added.join(state.select("membership_id"), "membership_id", "inner").limit(1).count()
        ):
            raise ValueError("Invalid reconstruction transition membership")
        state = state.join(
            removed.select("membership_id"), "membership_id", "left_anti"
        ).unionByName(added.select(*MEMBER_COLUMNS))
        exact_members(state, read_state(session, store, current))
        if state_digest(state) != current.observation.payload.set_digest:
            raise ValueError("Reconstructed state digest mismatch")
    return state
