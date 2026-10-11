from unittest.mock import Mock

import pytest

from search.client import initialize_indices
from search.config import KINDS, SERVER_VERSION, SearchConfig
from search.mappings import mapping
from search.queries import aggregate, query_documents


def fake_client():
    config = SearchConfig()
    client = Mock()
    client.info.return_value = {"version": {"number": SERVER_VERSION}}
    client.indices.exists.return_value = True
    client.indices.get_mapping.side_effect = lambda index: {
        index: {"mappings": mapping(index.removeprefix("configstream-").removesuffix("-v1"))}
    }
    client.indices.get_settings.side_effect = lambda index: {
        index: {"settings": {"index": {"number_of_shards": "1", "number_of_replicas": "0"}}}
    }
    client.indices.exists_alias.return_value = True
    client.indices.get_alias.side_effect = lambda name: {
        name + "-v1": {"aliases": {name: {"is_write_index": True}}}
    }
    return config, client


def test_init_idempotent_and_creation():
    config, client = fake_client()
    initialize_indices(config, client)
    initialize_indices(config, client)
    client.indices.create.assert_not_called()
    client.indices.exists.return_value = False
    client.indices.exists_alias.return_value = False
    initialize_indices(config, client)
    assert client.indices.create.call_count == len(KINDS)
    assert client.indices.put_alias.call_count == len(KINDS)


@pytest.mark.parametrize("broken", ["version", "mapping", "alias", "settings"])
def test_incompatible_existing_resources_rejected(broken):
    config, client = fake_client()
    if broken == "version":
        client.info.return_value = {"version": {"number": "8.19.0"}}
    elif broken == "mapping":
        client.indices.get_mapping.side_effect = lambda index: {index: {"mappings": {}}}
    elif broken == "settings":
        client.indices.get_settings.side_effect = lambda index: {
            index: {"settings": {"index": {"number_of_shards": "2", "number_of_replicas": "0"}}}
        }
    else:
        client.indices.get_alias.side_effect = lambda name: {"other-index": {"aliases": {name: {}}}}
    with pytest.raises(RuntimeError):
        initialize_indices(config, client)
    client.indices.delete.assert_not_called()


def test_query_filters_sort_and_aggregations():
    config, client = fake_client()
    client.search.return_value = {"aggregations": {"counts": {"buckets": []}}}
    query_documents(client, config, "snapshots", asset_id="router", latest=True)
    options = client.search.call_args.kwargs
    assert options["index"] == "configstream-snapshots"
    assert options["size"] == 1
    assert options["sort"][0] == {"version": "desc"}
    query_documents(
        client,
        config,
        "alerts",
        severity="high",
        rule_id="logging-disabled",
        start="2026-10-11T00:00:00Z",
        end="2026-10-12T00:00:00Z",
    )
    assert len(client.search.call_args.kwargs["query"]["bool"]["filter"]) == 3
    for name in ("severity", "rule_id", "asset_id", "diffs_per_hour"):
        aggregate(client, config, name)
    with pytest.raises(ValueError):
        query_documents(client, config, "diffs", latest=True)
    with pytest.raises(ValueError):
        query_documents(client, config, "diffs", severity="high")
    with pytest.raises(ValueError):
        query_documents(client, config, "alerts", size=1001)


def test_live_config_requires_lake():
    from spark.common.config import SparkConfig

    with pytest.raises(ValueError, match="authoritative"):
        SparkConfig(search=SearchConfig())
