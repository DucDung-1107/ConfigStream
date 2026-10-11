from search.config import KINDS, SERVER_VERSION, SearchConfig
from search.mappings import mapping


def create_client(config: SearchConfig):
    from elasticsearch import Elasticsearch

    return Elasticsearch(
        config.url,
        basic_auth=(config.username, config.password) if config.username else None,
        request_timeout=config.timeout_seconds,
        max_retries=0,
    )


def initialize_indices(config: SearchConfig, client=None) -> None:
    owned = client is None
    client = create_client(config) if owned else client
    try:
        if client.info()["version"]["number"] != SERVER_VERSION:
            raise RuntimeError(f"Elasticsearch server must be the pinned {SERVER_VERSION}")
        for kind in KINDS:
            index, alias = config.index(kind), config.alias(kind)
            expected = mapping(kind)
            if not client.indices.exists(index=index):
                client.indices.create(
                    index=index,
                    mappings=expected,
                    settings={"number_of_shards": 1, "number_of_replicas": 0},
                )
            actual = client.indices.get_mapping(index=index)[index]["mappings"]
            if actual != expected:
                raise RuntimeError(
                    f"Incompatible search mapping: {index}; create a new schema version"
                )
            settings = client.indices.get_settings(index=index)[index]["settings"]["index"]
            if settings["number_of_shards"] != "1" or settings["number_of_replicas"] != "0":
                raise RuntimeError(f"Incompatible local search settings: {index}")
            if client.indices.exists_alias(name=alias):
                targets = client.indices.get_alias(name=alias)
                if set(targets) != {index} or targets[index]["aliases"][alias] != {
                    "is_write_index": True
                }:
                    raise RuntimeError(f"Incompatible search alias: {alias}")
            else:
                client.indices.put_alias(index=index, name=alias, is_write_index=True)
    finally:
        if owned:
            client.close()
