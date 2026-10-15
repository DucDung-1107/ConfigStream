from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

from processing.errors import AssetMismatchError
from processing.models import NormalizedConfig


@dataclass(frozen=True, slots=True)
class BufferedSnapshot:
    config: NormalizedConfig = field(repr=False)
    event_time_us: int

    @property
    def event_time_ms(self) -> int:
        return self.event_time_us // 1000

    @property
    def event_time(self) -> datetime:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=self.event_time_us)

    @property
    def version(self) -> int:
        return self.config.version


@dataclass(frozen=True, slots=True)
class AssetVersionState:
    asset_id: str
    snapshots: tuple[BufferedSnapshot, ...] = field(default=(), repr=False)
    emitted: frozenset[int] = frozenset()
    floor: int = 0

    def expires_at(self, timeout_ms: int) -> int | None:
        if not self.snapshots:
            return None
        return max(snapshot.event_time_ms for snapshot in self.snapshots) + timeout_ms


@dataclass(frozen=True, slots=True)
class StateDiagnostic:
    reason: str
    versions: tuple[int, ...]
    snapshot_id: str | None = None
    missing_start: int | None = None
    missing_end: int | None = None


@dataclass(frozen=True, slots=True)
class StateTransition:
    state: AssetVersionState
    pairs: tuple[tuple[BufferedSnapshot, BufferedSnapshot], ...] = field(default=(), repr=False)
    diagnostics: tuple[StateDiagnostic, ...] = ()
    accepted: bool = False


def _gaps(snapshots: tuple[BufferedSnapshot, ...]) -> tuple[StateDiagnostic, ...]:
    versions = tuple(snapshot.version for snapshot in snapshots)
    return tuple(
        StateDiagnostic(
            "unresolved_version_gap", versions, missing_start=old + 1, missing_end=new - 1
        )
        for old, new in zip(versions, versions[1:], strict=False)
        if new > old + 1
    )


def expire_state(state: AssetVersionState, watermark_ms: int, timeout_ms: int) -> StateTransition:
    expiry = state.expires_at(timeout_ms)
    if expiry is None or watermark_ms < expiry:
        return StateTransition(state)
    diagnostics = _gaps(state.snapshots) + (
        StateDiagnostic("state_timeout", tuple(snapshot.version for snapshot in state.snapshots)),
    )
    return StateTransition(AssetVersionState(state.asset_id), diagnostics=diagnostics)


def inspect_snapshot(
    state: AssetVersionState,
    snapshot_id: str,
    version: int,
    snapshot: BufferedSnapshot | None = None,
) -> StateTransition | None:
    """Reject known identities/conflicts before normalizing configuration again."""
    for existing in state.snapshots:
        if existing.config.snapshot_id == snapshot_id:
            if snapshot is None:
                return None
            if existing != snapshot:
                return StateTransition(
                    state,
                    diagnostics=(
                        StateDiagnostic("snapshot_identity_conflict", (version,), snapshot_id),
                    ),
                )
            return StateTransition(state)
    for existing in state.snapshots:
        if existing.version == version:
            return StateTransition(
                state,
                diagnostics=(StateDiagnostic("version_conflict", (version,), snapshot_id),),
            )
    if version <= state.floor:
        return StateTransition(
            state,
            diagnostics=(StateDiagnostic("evicted_version", (version,), snapshot_id),),
        )
    return None


def accept_snapshot(
    state: AssetVersionState, snapshot: BufferedSnapshot, max_versions: int
) -> StateTransition:
    """Pure bounded transition; Spark owns persistence, grouping, and event-time timers."""
    if type(max_versions) is not int or max_versions < 2:
        raise ValueError("max_versions must be an integer >= 2")
    if snapshot.config.asset_id != state.asset_id:
        raise AssetMismatchError("State and snapshot must belong to the same asset")
    rejected = inspect_snapshot(state, snapshot.config.snapshot_id, snapshot.version, snapshot)
    if rejected is not None:
        return rejected
    snapshots = tuple(sorted((*state.snapshots, snapshot), key=lambda item: item.version))
    emitted = set(state.emitted)
    pairs = []
    for previous, current in zip(snapshots, snapshots[1:], strict=False):
        if current.version == previous.version + 1 and previous.version not in emitted:
            pairs.append((previous, current))
            emitted.add(previous.version)
    diagnostics = ()
    floor = state.floor
    if len(snapshots) > max_versions:
        evicted = snapshots[0]
        diagnostics = tuple(
            gap for gap in _gaps(snapshots) if gap.missing_start == evicted.version + 1
        ) + (StateDiagnostic("state_eviction", (evicted.version,), evicted.config.snapshot_id),)
        floor = evicted.version
        snapshots = snapshots[1:]
    retained = {item.version for item in snapshots}
    updated = replace(
        state,
        snapshots=snapshots,
        emitted=frozenset(
            version for version in emitted if version in retained and version + 1 in retained
        ),
        floor=floor,
    )
    return StateTransition(updated, tuple(pairs), diagnostics, accepted=True)
