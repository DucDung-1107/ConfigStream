from typing import Literal

from pydantic import Field

from schemas.temporal.base import Contract
from schemas.temporal.records import RisOccurrence


class MpPrefixes(Contract):
    attribute: Literal[14, 15]
    prefixes: tuple[RisOccurrence, ...] = Field(max_length=4096)


def expansion_order(
    withdrawals: tuple[RisOccurrence, ...],
    announcements: tuple[RisOccurrence, ...],
    mp_attributes: tuple[MpPrefixes, ...],
) -> tuple[tuple[int, RisOccurrence], ...]:
    """Owned/pinned-parser projection boundary, not an MRT decoder or publisher."""
    groups = [("withdrawal", withdrawals), ("announcement", announcements)]
    groups.extend(
        ("announcement" if attribute.attribute == 14 else "withdrawal", attribute.prefixes)
        for attribute in mp_attributes
    )
    events = []
    for action, prefixes in groups:
        for prefix in prefixes:
            if prefix.action != action:
                raise ValueError("Prefix action and wire section disagree")
            if len(events) >= 4096:
                raise ValueError("Expansion fixture bound exceeded")
            events.append((len(events), prefix))
    return tuple(events)
