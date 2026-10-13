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
        path = cursor.number(4) if addpath else None
        bits = cursor.number(1)
        if bits > width * 8:
            raise ValueError("Oracle invalid prefix length")
        raw = cursor.take((bits + 7) // 8)
        padded = raw + bytes(width - len(raw))
        address = ip_address(padded)
        result.append((str(ip_network(f"{address}/{bits}", strict=False)), path))
    return result


def extract(kind, subtype, body):
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
        return (peer_asn, peer_ip, micros, [])
    addpath = subtype in {8, 9, 10, 11}
    withdrawals = networks(cursor.take(cursor.number(2)), 1, addpath)
    attributes = Cursor(cursor.take(cursor.number(2)))
    announcements = networks(cursor.take(cursor.remaining()), 1, addpath)
    events = [("withdrawal", 1, prefix, path) for prefix, path in withdrawals]
    events.extend(("announcement", 1, prefix, path) for prefix, path in announcements)
    while attributes.remaining():
        flags, number = attributes.number(1), attributes.number(1)
        value = Cursor(attributes.take(attributes.number(2 if flags & 16 else 1)))
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
    return peer_asn, peer_ip, micros, events


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
