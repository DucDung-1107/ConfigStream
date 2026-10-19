import json

import pytest
from pydantic import ValidationError

from api.errors import ApiError
from api.models import RESPONSE_MODELS, SnapshotResponse
from api.pagination import cursor_context, decode_cursor, encode_cursor
from tests.api.conftest import document


@pytest.mark.parametrize("kind", list(RESPONSE_MODELS))
def test_models_validate_safe_projection_and_utc(kind):
    payload = document(kind)
    field = "created_at" if kind == "alerts" else "event_time"
    payload[field] = "2026-10-11T07:00:00+07:00"
    result = RESPONSE_MODELS[kind].model_validate(payload).model_dump(mode="json")
    assert result[field] == "2026-10-11T00:00:00Z"
    assert not {"content", "normalized_content", "details", "value"} & result.keys()


@pytest.mark.parametrize(
    "field,value",
    [("version", 0), ("version", "1"), ("asset_id", " "), ("event_time", "2026-10-11T00:00:00")],
)
def test_malformed_snapshot_contract(field, value):
    with pytest.raises(ValidationError):
        SnapshotResponse.model_validate({**document("snapshots"), field: value})


def test_severity_uses_domain_enum():
    with pytest.raises(ValidationError):
        RESPONSE_MODELS["alerts"].model_validate({**document("alerts"), "severity": "urgent"})


@pytest.mark.parametrize(
    "values",
    [
        [True, "id"],
        [[], "id"],
        [123, None],
        [123, ""],
        [123],
        [2**64, "id"],
        [123, "a" * 1025],
        ["not-a-date", "id"],
        ["2026-10-11T00:00:00", "id"],
    ],
)
def test_server_sort_metadata_must_be_valid(values):
    with pytest.raises(ApiError) as caught:
        encode_cursor(values, cursor_context("snapshots", {}))
    assert caught.value.status == 502


@pytest.mark.parametrize(
    "payload",
    [
        {"sort": [1, "id"]},
        {"context": "bad", "sort": [1, "id"]},
        {"context": "bad", "sort": [[], {}]},
        [],
    ],
)
def test_client_cursor_structure_rejected(payload):
    import base64

    cursor = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    with pytest.raises(ApiError) as caught:
        decode_cursor(cursor, cursor_context("snapshots", {}))
    assert caught.value.status == 400


def test_maximum_unicode_identifier_fits_bounded_cursor():
    context = cursor_context("snapshots", {})
    values = [123, "🌐" * 1024]
    cursor = encode_cursor(values, context)
    assert len(cursor) <= 16384
    assert decode_cursor(cursor, context) == values


def test_deeply_nested_cursor_is_a_client_error():
    import base64

    cursor = base64.urlsafe_b64encode(("[" * 1500 + "0" + "]" * 1500).encode()).decode()
    with pytest.raises(ApiError) as caught:
        decode_cursor(cursor.rstrip("="), cursor_context("snapshots", {}))
    assert caught.value.status == 400
