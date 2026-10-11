import base64
import binascii
import json
import re
from datetime import datetime
from hashlib import sha256

from api.errors import ApiError

MAX_CURSOR_LENGTH = 16384


def cursor_context(kind: str, filters: dict) -> str:
    canonical = json.dumps(["api-v1", kind, filters], sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode()).hexdigest()


def valid_sort(values) -> bool:
    if isinstance(values, list) and len(values) == 2 and isinstance(values[0], str):
        try:
            if datetime.fromisoformat(values[0]).tzinfo is None:
                return False
        except ValueError:
            return False
    return (
        isinstance(values, list)
        and len(values) == 2
        and (
            type(values[0]) is int
            and -(2**63) <= values[0] < 2**63
            or isinstance(values[0], str)
            and 0 < len(values[0]) <= 64
        )
        and isinstance(values[1], str)
        and 0 < len(values[1]) <= 1024
    )


def encode_cursor(values: list, context: str) -> str:
    if not valid_sort(values):
        raise ApiError(502, "invalid_search_response", "Invalid search pagination metadata")
    data = json.dumps(
        {"context": context, "sort": values}, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def decode_cursor(cursor: str | None, context: str) -> list | None:
    if cursor is None:
        return None
    try:
        if len(cursor) > MAX_CURSOR_LENGTH or not re.fullmatch(r"[A-Za-z0-9_-]+", cursor):
            raise ValueError
        data = json.loads(base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_"))
        if (
            not isinstance(data, dict)
            or set(data) != {"context", "sort"}
            or data["context"] != context
            or not valid_sort(data["sort"])
        ):
            raise ValueError
        return data["sort"]
    except (ValueError, TypeError, binascii.Error, UnicodeError, RecursionError):
        raise ApiError(400, "invalid_cursor", "Cursor is invalid for this query") from None
