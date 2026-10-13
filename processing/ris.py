"""Bounded pinned-parser projection. Fingerprints are descriptive, never identities."""

import gzip
import io
import json
from hashlib import sha256
from struct import pack

from processing.mrt_framing import frame_mrt
from schemas.ris import EventRow
from schemas.temporal.provenance import verify_artifact_file


def decode_frame(frame, parser):
    if parser.__version__ != "2.2.0":
        raise ValueError("RIS requires pinned mrtparse 2.2.0")
    wire = (
        pack("!IHHI", frame.timestamp_seconds, frame.record_type, frame.subtype, frame.body_length)
        + frame.body
    )
    try:
        decoded = next(parser.Reader(io.BytesIO(wire)))
    except (KeyError, ValueError, IndexError) as error:
        return None, "parser_exception_" + type(error).__name__
    if decoded.err:
        return None, "parser_body_error:" + str(decoded.err_msg)
    return decoded.data, None


def project_record(record):
    message = record.get("bgp_message", {})
    if 2 not in message.get("type", {}):
        return []
    events = []
    for action, values in (
        ("withdrawal", message.get("withdrawn_routes", [])),
        ("announcement", message.get("nlri", [])),
    ):
        events.extend((action, 1, value) for value in values)
    for attribute in message.get("path_attributes", []):
        number = next(iter(attribute["type"]))
        if number not in {14, 15}:
            continue
        value = attribute["value"]
        family = next(iter(value["afi"]))
        if 1 not in value["safi"] or family not in {1, 2}:
            raise ValueError("Unsupported MP family under frozen unicast policy")
        action = "announcement" if number == 14 else "withdrawal"
        events.extend(
            (action, family, prefix)
            for prefix in value.get("nlri", value.get("withdrawn_routes", []))
        )
    if len(events) > 4096:
        raise ValueError("RIS expansion bound exceeded")
    return events


def event_rows(frame, record, reference):
    from ipaddress import ip_network

    attributes = sha256(
        json.dumps(
            record.get("bgp_message", {}).get("path_attributes", []), sort_keys=True
        ).encode()
    ).digest()
    timestamp = next(iter(record["timestamp"]))
    if timestamp != frame.timestamp_seconds:
        raise ValueError("Parser/framer source timestamp disagreement")
    peer = (str(record.get("peer_as")), record.get("peer_ip"))
    for ordinal, (action, family, prefix) in enumerate(project_record(record)):
        network = str(ip_network(f"{prefix['prefix']}/{prefix['length']}", strict=True))
        fingerprint = sha256(
            repr(
                (
                    timestamp,
                    action,
                    peer,
                    prefix["prefix"],
                    prefix["length"],
                    prefix.get("path_id"),
                    attributes,
                )
            ).encode()
        )
        yield EventRow(
            logical_record_id=reference.occurrence_id(frame.byte_offset, ordinal),
            artifact_sha256=reference.manifest.artifacts[0].sha256,
            provenance_manifest_id=reference.manifest.manifest_id,
            byte_offset=frame.byte_offset,
            all_record_index=frame.all_record_index,
            expansion_ordinal=ordinal,
            source_seconds=timestamp,
            source_microseconds=frame.microseconds,
            timestamp_precision="second" if frame.microseconds is None else "microsecond",
            peer_asn=int(peer[0]),
            peer_ip=peer[1],
            action=action,
            afi=family,
            safi=1,
            prefix=network,
            path_id=prefix.get("path_id"),
            attribute_digest=attributes.hex(),
            content_fingerprint=fingerprint.hexdigest(),
        )


def read_archive(path, reference, parser):
    verify_artifact_file(path, reference.manifest.artifacts[0])
    with gzip.open(path, "rb") as stream:
        for frame in frame_mrt(stream):
            record, error = decode_frame(frame, parser)
            yield frame, error, (() if error else tuple(event_rows(frame, record, reference)))
    verify_artifact_file(path, reference.manifest.artifacts[0])
