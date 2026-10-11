from unittest.mock import Mock

import pytest

from search.bulk import write_actions
from search.config import SearchConfig


def actions(count):
    for index in range(count):
        yield {
            "_index": "configstream-alerts",
            "_id": f"alert-{index}",
            "_source": {"message": "SENSITIVE_TEST_MARKER"},
        }


def response(statuses):
    return {
        "items": [
            {"index": {"status": status, "error": "SENSITIVE_TEST_MARKER"}} for status in statuses
        ]
    }


def test_bounded_bulk_and_metrics(caplog):
    client = Mock()
    client.bulk.side_effect = [response([201, 200]), response([201])]
    metrics = write_actions(actions(3), SearchConfig(bulk_size=2), client)
    assert metrics == {"attempted": 3, "indexed": 3, "requests": 2, "retries": 0, "failures": 0}
    assert client.bulk.call_args_list[0].kwargs["require_alias"] is True
    assert len(client.bulk.call_args_list[0].kwargs["operations"]) == 4
    assert "SENSITIVE_TEST_MARKER" not in caplog.text


def test_retry_only_transient_items(monkeypatch):
    monkeypatch.setattr("search.bulk.time.sleep", lambda seconds: None)
    client = Mock()
    client.bulk.side_effect = [response([201, 429]), response([503]), response([201])]
    metrics = write_actions(actions(2), SearchConfig(), client)
    assert metrics["indexed"] == 2
    assert metrics["retries"] == 2
    assert len(client.bulk.call_args_list[1].kwargs["operations"]) == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404])
def test_nonretryable_partial_failure_propagates(status, caplog):
    client = Mock()
    client.bulk.return_value = response([201, status])
    with pytest.raises(RuntimeError, match=f"status={status}") as failure:
        write_actions(actions(2), SearchConfig(), client)
    assert client.bulk.call_count == 1
    assert "SENSITIVE_TEST_MARKER" not in str(failure.value) + caplog.text


def test_exhausted_retry_and_transport_failure(monkeypatch):
    monkeypatch.setattr("search.bulk.time.sleep", lambda seconds: None)
    client = Mock()
    client.bulk.return_value = response([429])
    with pytest.raises(RuntimeError):
        write_actions(actions(1), SearchConfig(), client)
    assert client.bulk.call_count == 4
    client = Mock()
    client.bulk.side_effect = ConnectionError("SENSITIVE_TEST_MARKER")
    with pytest.raises(RuntimeError, match="request failed") as failure:
        write_actions(actions(1), SearchConfig(), client)
    assert client.bulk.call_count == 4
    assert "SENSITIVE_TEST_MARKER" not in str(failure.value)


def test_response_count_and_byte_bound(monkeypatch):
    client = Mock()
    client.bulk.return_value = response([])
    with pytest.raises(RuntimeError, match="item count"):
        write_actions(actions(1), SearchConfig(), client)
    monkeypatch.setattr("search.bulk.MAX_BULK_BYTES", 4)
    with pytest.raises(ValueError, match="byte limit"):
        write_actions(actions(1), SearchConfig(), client)


def test_partition_skips_empty_and_closes_on_failure(monkeypatch):
    from types import SimpleNamespace

    from search.sink import write_partition

    factory = Mock()
    monkeypatch.setattr("search.sink.create_client", factory)
    write_partition(iter(()), "alerts", SearchConfig())
    factory.assert_not_called()
    client = factory.return_value
    client.bulk.return_value = response([400])
    row = SimpleNamespace(document_json='{"alert_id":"alert-1"}')
    with pytest.raises(RuntimeError):
        write_partition(iter([row]), "alerts", SearchConfig())
    client.close.assert_called_once()
