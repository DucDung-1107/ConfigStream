"""Bounded committed-only RPKI research queries, not a serving API."""

from lake.rpki import validated_paths
from spark.rpki import comparable, projection_digest, read_state, transition_member_digest


def authorization_at(session, store, reference, member):
    if member.scope != reference.manifest.source_scope:
        raise ValueError("Authorization lookup scope mismatch")
    from pyspark.sql import functions as functions

    return (
        read_state(session, store, reference)
        .filter(functions.col("membership_id") == member.membership_id)
        .limit(1)
        .count()
        == 1
    )


def transition_history(session, store, previous, current):
    comparable(previous, current)
    commit = store.get(f"boundary/{previous.observation_id}/{current.observation_id}")
    if commit is None:
        raise ValueError("Transition boundary missing or unpublished")
    frame = session.read.parquet(validated_paths(store, commit, "transitions"))
    if (
        frame.count() != commit["rows"]
        or transition_member_digest(frame) != commit["digest"]
        or (
            projection_digest(
                frame,
                columns=["logical_record_id", "action", "member_encoding"],
                order=["logical_record_id"],
            )
            != commit["identity_digest"]
        )
    ):
        raise ValueError("Historical transition checksum mismatch")
    return frame


def asn_activity(session, store, previous, current):
    return transition_history(session, store, previous, current).groupBy("asn", "action").count()


def distinct_memberships(session, store, references):
    if not 1 <= len(references) <= 14:
        raise ValueError("RPKI query window must contain 1 to 14 accepted states")
    projection = None
    for reference in references:
        selected = read_state(session, store, reference).select("membership_id")
        projection = selected if projection is None else projection.unionByName(selected)
    return projection.distinct().count()
