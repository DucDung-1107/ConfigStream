from collections.abc import Iterator
from dataclasses import dataclass, field
from struct import unpack
from typing import BinaryIO


class FramingError(ValueError):
    def __init__(self, category: str, offset: int, index: int):
        self.category, self.offset, self.index = category, offset, index
        super().__init__(f"MRT framing {category}; offset={offset}; all_record_index={index}")


@dataclass(frozen=True, slots=True)
class MrtFrame:
    byte_offset: int
    all_record_index: int
    timestamp_seconds: int
    record_type: int
    subtype: int
    body_length: int
    microseconds: int | None
    body: bytes = field(repr=False)

    @property
    def next_offset(self) -> int:
        return self.byte_offset + 12 + self.body_length


def _read(stream: BinaryIO, size: int) -> bytes:
    output = bytearray()
    while len(output) < size:
        chunk = stream.read(size - len(output))
        if not chunk:
            break
        output.extend(chunk)
    return bytes(output)


def frame_mrt(stream: BinaryIO, *, max_body_bytes: int = 1024**2) -> Iterator[MrtFrame]:
    """RFC 6396 framing from stream start; no body parser and no resynchronization."""
    if type(max_body_bytes) is not int or not 4 <= max_body_bytes <= 16 * 1024**2:
        raise ValueError("MRT body bound must be 4 bytes through 16 MiB")
    if stream.tell() != 0:
        raise ValueError("Framing requires decompressed artifact origin, not a sliced stream")
    offset = index = 0
    while True:
        header = _read(stream, 12)
        if not header:
            return
        if len(header) != 12:
            raise FramingError("truncated_header", offset, index)
        timestamp, record_type, subtype, length = unpack("!IHHI", header)
        if length > max_body_bytes:
            raise FramingError("declared_length_limit", offset, index)
        body = _read(stream, length)
        if len(body) != length:
            raise FramingError("truncated_body", offset, index)
        microseconds = None
        if record_type in {17, 33, 49}:
            if length < 4:
                raise FramingError("truncated_extended_timestamp", offset, index)
            microseconds = unpack("!I", body[:4])[0]
        yield MrtFrame(offset, index, timestamp, record_type, subtype, length, microseconds, body)
        offset += 12 + length
        index += 1
