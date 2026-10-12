import json
import unicodedata
from hashlib import sha256


def canonical_bytes(value) -> bytes:
    """Bounded JSON, NFC strings, sorted keys; floats and non-JSON objects fail closed."""
    used_bytes = 0

    def reserve(size):
        nonlocal used_bytes
        used_bytes += size
        if used_bytes > 1024**2:
            raise ValueError("Canonical normalization byte budget exceeded")

    def normalize(item, depth=0):
        if depth > 16:
            raise ValueError("Canonical value nesting limit exceeded")
        if item is None or type(item) in {bool, int}:
            if type(item) is int and not -(2**63) <= item < 2**63:
                raise ValueError("Canonical integer outside signed 64-bit range")
            reserve(len(json.dumps(item)) + 1)
            return item
        if type(item) is str:
            if len(item) > 65536:
                raise ValueError("Canonical string limit exceeded")
            normalized = unicodedata.normalize("NFC", item)
            reserve(len(json.dumps(normalized, ensure_ascii=False).encode("utf-8")) + 1)
            return normalized
        if type(item) in {tuple, list}:
            if len(item) > 10000:
                raise ValueError("Canonical sequence limit exceeded")
            reserve(2)
            return [normalize(part, depth + 1) for part in item]
        if type(item) is dict:
            if len(item) > 1000 or any(type(key) is not str for key in item):
                raise ValueError("Invalid canonical object")
            reserve(2)
            result = {}
            for key, part in item.items():
                normalized = normalize(key, depth + 1)
                if normalized in result:
                    raise ValueError("Unicode key collision")
                result[normalized] = normalize(part, depth + 1)
            return result
        raise ValueError("Unsupported canonical value type")

    encoded = json.dumps(
        normalize(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    if len(encoded) > 1024**2:
        raise ValueError("Canonical value byte limit exceeded")
    return encoded


def logical_identity(kind: str, policy: str, components: tuple) -> str:
    from pydantic import TypeAdapter

    from schemas.temporal.base import Token

    TypeAdapter(Token).validate_python(kind)
    TypeAdapter(Token).validate_python(policy)
    if type(components) is not tuple or len(components) > 64:
        raise ValueError("Identity requires a bounded tuple of at most 64 components")
    tagged = []
    for component in components:
        tag = {str: "string", int: "integer", bool: "boolean", type(None): "unknown"}.get(
            type(component)
        )
        if tag is None:
            raise ValueError("Identity components must be scalar typed values")
        tagged.append([tag, component])
    return sha256(canonical_bytes(["configstream-temporal-v1", kind, policy, tagged])).hexdigest()
