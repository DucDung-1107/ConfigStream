"""Small service-free Phase 20 accounting and exactness primitives."""

import hashlib
import json
import random
from itertools import zip_longest
from statistics import median


def randomized_order(configurations, repeats=3, seed=42):
    if repeats < 3 or len(set(configurations)) != len(configurations):
        raise ValueError("Three repeats and distinct configurations required")
    order = [
        (configuration, repetition)
        for repetition in range(1, repeats + 1)
        for configuration in configurations
    ]
    random.Random(seed).shuffle(order)
    return order


def statistics(values):
    if not values or any(value < 0 for value in values):
        raise ValueError("Missing or negative measured duration")
    return dict(individual=values, median=median(values), minimum=min(values), maximum=max(values))


def preference(full_bytes, candidate_bytes, full_queries, candidate_queries, *, correct):
    if full_bytes <= 0 or candidate_bytes < 0:
        raise ValueError("Invalid analytical byte denominator")
    saving = 1 - candidate_bytes / full_bytes
    if not correct:
        return dict(status="REJECTED_CORRECTNESS", analytical_saving=saving)
    if not full_queries or full_queries.keys() != candidate_queries.keys():
        return dict(
            status="INCONCLUSIVE", reason="missing required query", analytical_saving=saving
        )
    if any(len(values) < 3 for values in (*full_queries.values(), *candidate_queries.values())):
        return dict(status="INSUFFICIENT_REPETITIONS", analytical_saving=saving)
    if any(value <= 0 for values in full_queries.values() for value in values):
        raise ValueError("Zero reconstruction/query denominator")
    ratios = {
        query: median(candidate_queries[query]) / median(values)
        for query, values in full_queries.items()
    }
    return dict(
        status="PREFERRED"
        if candidate_bytes * 5 <= full_bytes * 4 and all(value <= 2 for value in ratios.values())
        else "NOT_PREFERRED",
        analytical_saving=saving,
        query_median_ratios=ratios,
    )


def exact_encoding_digest(actual, expected, check=lambda: None):
    checksum = hashlib.sha256(b"rpki-set-v1\n")
    count = 0
    for count, (observed, reference) in enumerate(zip_longest(actual, expected), 1):
        if observed is None or reference is None or observed != reference:
            raise ValueError("Exact canonical membership mismatch")
        encoded = observed[0]
        if isinstance(encoded, str):
            encoded = encoded.encode()
        checksum.update(len(encoded).to_bytes(4, "big"))
        checksum.update(encoded)
        if count % 8192 == 0:
            check()
    return dict(rows=count, set_digest=checksum.hexdigest())


def summary_digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
