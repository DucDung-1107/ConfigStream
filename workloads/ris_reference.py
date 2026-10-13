"""Independent wire extractor for BGP4MP unicast UPDATEs. No production parser imports."""

import gzip
import json
from hashlib import sha256
from ipaddress import ip_address, ip_network
from struct import unpack


def frames(path):
    with gzip.open(path, "rb") as stream:
        offset = index = 0
        while header := stream.read(12):
            if len(header) != 12:
                raise ValueError("Oracle truncated MRT header")
            stamp, kind, subtype, size = unpack("!IHHI", header)
            if size > 1024**2:
                raise ValueError("Oracle frame size bound")
            body = stream.read(size)
            if len(body) != size:
                raise ValueError("Oracle truncated MRT body")
            yield offset, index, stamp, kind, subtype, body
            offset += size + 12
            index += 1


class Cursor:
    def __init__(self, data):
        self.data = data
        self.position = 0

    def take(self, size):
        end = self.position + size
        if size < 0 or end > len(self.data):
            raise ValueError("Oracle truncated field")
        value = self.data[self.position : end]
        self.position = end
        return value

    def number(self, size):
        return int.from_bytes(self.take(size), "big")

    def remaining(self):
        return len(self.data) - self.position


def networks(data, family, addpath):
    cursor = Cursor(data)
    result = []
    width = {1: 4, 2: 16}[family]
    while cursor.remaining():
        if len(result) >= 4096:
            raise ValueError("Independent NLRI expansion bound")
        path = cursor.number(4) if addpath else None
        bits = cursor.number(1)
        if bits > width * 8:
            raise ValueError("Oracle invalid prefix length")
        raw = cursor.take((bits + 7) // 8)
        padded = raw + bytes(width - len(raw))
        address = ip_address(padded)
        result.append((str(ip_network(f"{address}/{bits}", strict=False)), path))
    return result


def extract(kind, subtype, body, *, with_attributes=False):
    if kind not in {16, 17} or subtype not in {1, 4, 6, 7, 8, 9, 10, 11}:
        return None
    cursor = Cursor(body)
    micros = cursor.number(4) if kind == 17 else None
    if micros is not None and micros > 999999:
        raise ValueError("Oracle invalid microseconds")
    as_width = 4 if subtype in {4, 7, 9, 11} else 2
    peer_asn = cursor.number(as_width)
    cursor.take(as_width)
    cursor.take(2)
    family = cursor.number(2)
    if family not in {1, 2}:
        raise ValueError("Oracle unsupported peer AFI")
    width = 4 if family == 1 else 16
    peer_ip = str(ip_address(cursor.take(width)))
    cursor.take(width)
    if cursor.take(16) != b"\xff" * 16:
        raise ValueError("Oracle invalid BGP marker")
    length = cursor.number(2)
    message_type = cursor.number(1)
    if length < 19 or length - 19 != cursor.remaining():
        raise ValueError("Oracle BGP length mismatch")
    if message_type != 2:
        result = (peer_asn, peer_ip, micros, [])
        return (*result, []) if with_attributes else result
    addpath = subtype in {8, 9, 10, 11}
    withdrawals = networks(cursor.take(cursor.number(2)), 1, addpath)
    attributes = Cursor(cursor.take(cursor.number(2)))
    announcements = networks(cursor.take(cursor.remaining()), 1, addpath)
    events = [("withdrawal", 1, prefix, path) for prefix, path in withdrawals]
    events.extend(("announcement", 1, prefix, path) for prefix, path in announcements)
    decoded_attributes = []
    while attributes.remaining():
        flags, number = attributes.number(1), attributes.number(1)
        size = attributes.number(2 if flags & 16 else 1)
        raw = attributes.take(size)
        if with_attributes:
            decoded_attributes.append(decode_attribute(flags, number, raw, as_width, addpath))
        value = Cursor(raw)
        if number not in {14, 15}:
            continue
        afi, safi = value.number(2), value.number(1)
        if afi not in {1, 2} or safi != 1:
            raise ValueError("Oracle unsupported MP family")
        if number == 14:
            value.take(value.number(1))
            value.take(1)
        events.extend(
            ("announcement" if number == 14 else "withdrawal", afi, prefix, path)
            for prefix, path in networks(value.take(value.remaining()), afi, addpath)
        )
    result = (peer_asn, peer_ip, micros, events)
    return (*result, decoded_attributes) if with_attributes else result


ATTRIBUTE_LABELS = {
    1: "ORIGIN",
    2: "AS_PATH",
    3: "NEXT_HOP",
    4: "MULTI_EXIT_DISC",
    5: "LOCAL_PREF",
    6: "ATOMIC_AGGREGATE",
    7: "AGGREGATOR",
    8: "COMMUNITY",
    9: "ORIGINATOR_ID",
    10: "CLUSTER_LIST",
    14: "MP_REACH_NLRI",
    15: "MP_UNREACH_NLRI",
    16: "EXTENDED COMMUNITIES",
    17: "AS4_PATH",
    18: "AS4_AGGREGATOR",
    21: "AS_PATHLIMIT",
    32: "LARGE_COMMUNITY",
    35: "Only to Customer",
}


def prefix_objects(raw, family, addpath):
    result = []
    for prefix, path in networks(raw, family, addpath):
        address, length = prefix.split("/")
        item = {"length": int(length), "prefix": address}
        if path is not None:
            item["path_id"] = path
        result.append(item)
    return result


def decode_attribute(flags, number, raw, as_width, addpath):
    """Wire values independently decoded; labels/JSON shape implement frozen digest format."""
    value = Cursor(raw)
    if number == 1:
        origin = value.number(1)
        decoded = {origin: {0: "IGP", 1: "EGP", 2: "INCOMPLETE"}.get(origin, "Unknown")}
    elif number in {2, 17}:
        decoded = []
        labels = {1: "AS_SET", 2: "AS_SEQUENCE", 3: "AS_CONFED_SEQUENCE", 4: "AS_CONFED_SET"}
        while value.remaining():
            segment, count = value.number(1), value.number(1)
            decoded.append(
                {
                    "type": {segment: labels.get(segment, "Unknown")},
                    "length": count,
                    "value": [
                        str(value.number(4 if number == 17 else as_width)) for _ in range(count)
                    ],
                }
            )
    elif number == 3:
        decoded = str(ip_address(value.take(len(raw)))) if len(raw) in {4, 16} else None
        if decoded is None:
            value.take(value.remaining())
    elif number in {4, 5}:
        decoded = value.number(4)
    elif number in {7, 18}:
        decoded = {
            "as": str(value.number(4 if number == 18 or len(raw) >= 8 else 2)),
            "id": str(ip_address(value.take(4))),
        }
    elif number in {8, 32}:
        width = 4 if number == 8 else 12
        decoded = []
        while value.remaining():
            if number == 8:
                community = value.number(width)
                decoded.append(f"{community >> 16}:{community & 65535}")
            else:
                decoded.append(":".join(str(value.number(4)) for _ in range(3)))
    elif number == 9:
        decoded = str(ip_address(value.take(4)))
    elif number in {10, 16}:
        decoded = []
        while value.remaining():
            decoded.append(str(ip_address(value.take(4))) if number == 10 else value.number(8))
    elif number in {14, 15}:
        family, safi = value.number(2), value.number(1)
        if family not in {1, 2} or safi != 1:
            raise ValueError("Independent attribute unsupported MP scope")
        decoded = {"afi": {family: "IPv4" if family == 1 else "IPv6"}, "safi": {1: "UNICAST"}}
        if number == 14:
            length = value.number(1)
            width = 4 if family == 1 else 16
            if length not in ({4} if family == 1 else {16, 32}):
                raise ValueError("Independent next-hop length unsupported")
            decoded["next_hop_length"] = length
            decoded["next_hop"] = [
                str(ip_address(value.take(width))) for _ in range(length // width)
            ]
            decoded["reserved"] = value.number(1)
        decoded["nlri" if number == 14 else "withdrawn_routes"] = prefix_objects(
            value.take(value.remaining()), family, addpath
        )
    elif number in {6, 21, 35, 255}:
        decoded = value.take(value.remaining()).hex(" ")
    else:
        raise ValueError(f"Independent attribute family unsupported: {number}")
    if value.remaining():
        raise ValueError("Independent attribute length mismatch")
    return {
        "flag": flags,
        "type": {number: ATTRIBUTE_LABELS.get(number, "Unknown")},
        "length": len(raw),
        "value": decoded,
    }


def occurrence_projection(reference, coordinate, decoded):
    """Independent values and hash preimages; no production projection/identity calls."""
    offset, index, stamp, kind, subtype, body = coordinate
    peer_asn, peer_ip, micros, prefixes, attributes = decoded
    attribute_hash = sha256(json.dumps(attributes, sort_keys=True).encode()).digest()
    for ordinal, (action, family, prefix, path) in enumerate(prefixes):
        address, length = prefix.split("/")
        fingerprint = sha256(
            repr(
                (
                    stamp,
                    action,
                    (str(peer_asn), peer_ip),
                    address,
                    int(length),
                    path,
                    attribute_hash,
                )
            ).encode()
        ).hexdigest()
        yield {
            "logical_record_id": identity(reference, offset, ordinal),
            "artifact_sha256": reference.manifest.artifacts[0].sha256,
            "provenance_manifest_id": reference.manifest.manifest_id,
            "byte_offset": offset,
            "all_record_index": index,
            "expansion_ordinal": ordinal,
            "source_seconds": stamp,
            "source_microseconds": micros,
            "timestamp_precision": "second" if micros is None else "microsecond",
            "peer_asn": peer_asn,
            "peer_ip": peer_ip,
            "action": action,
            "afi": family,
            "safi": 1,
            "prefix": prefix,
            "path_id": path,
            "attribute_digest": attribute_hash.hex(),
            "content_fingerprint": fingerprint,
        }


def identity(reference, offset, ordinal):
    source = reference.manifest

    def encode(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()

    def digest(kind, policy, values):
        tags = [["string" if isinstance(value, str) else "integer", value] for value in values]
        return sha256(encode(["configstream-temporal-v1", kind, policy, tags])).hexdigest()

    policy = digest(
        "policy_bindings",
        "v1",
        (
            source.parser_version,
            source.canonicalization_version,
            source.counting_policy_version,
        ),
    )
    return digest(
        "ris_occurrence",
        policy,
        (
            source.source_id,
            source.source_scope,
            source.artifacts[0].sha256,
            offset,
            ordinal,
        ),
    )
