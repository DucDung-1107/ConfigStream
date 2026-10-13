"""Sorted full-projection comparison; callers own typed schemas and source validation."""


def reconcile_sorted(rows):
    """One full projection and constant flags per identity; never buffer a duplicate group."""
    previous = None
    exemplar = None
    actual_seen = expected_seen = conflict = False

    def outcome():
        status = (
            "conflict"
            if conflict
            else "accepted"
            if actual_seen and expected_seen
            else "unexpected"
            if actual_seen
            else "missing"
        )
        return (*exemplar, status)

    for row in rows:
        projection, origin = tuple(row[:-1]), row[-1]
        identity = projection[0]
        if origin not in (0, 1) or not isinstance(identity, str):
            raise ValueError("RIS invalid reconciliation input")
        if previous is not None and identity < previous:
            raise ValueError("RIS reconciliation requires sorted identities")
        if identity != previous:
            if exemplar is not None:
                yield outcome()
            previous, exemplar = identity, projection
            actual_seen = expected_seen = conflict = False
        elif projection != exemplar:
            conflict = True
        actual_seen = actual_seen or origin == 0
        expected_seen = expected_seen or origin == 1
    if exemplar is not None:
        yield outcome()
