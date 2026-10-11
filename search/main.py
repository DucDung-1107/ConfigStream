import argparse
import json
import logging

from search.client import create_client, initialize_indices
from search.config import KINDS, SearchConfig
from search.queries import aggregate, query_documents


def main() -> None:
    parser = argparse.ArgumentParser(description="Safe derived search index operations")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    commands.add_parser("inspect")
    query = commands.add_parser("query")
    query.add_argument("kind", choices=KINDS)
    query.add_argument("--asset-id")
    query.add_argument("--severity", choices=("info", "low", "medium", "high", "critical"))
    query.add_argument("--rule-id")
    query.add_argument("--type", dest="diagnostic_type")
    query.add_argument("--start")
    query.add_argument("--end")
    query.add_argument("--latest", action="store_true")
    query.add_argument("--size", type=int, default=20)
    query.add_argument("--group-by", choices=("severity", "rule_id", "asset_id", "diffs_per_hour"))
    args = parser.parse_args()
    if (
        args.command == "query"
        and args.group_by
        and any(
            getattr(args, name)
            for name in (
                "asset_id",
                "severity",
                "rule_id",
                "diagnostic_type",
                "start",
                "end",
                "latest",
            )
        )
    ):
        parser.error("Group-by queries are unfiltered; do not combine them with document filters")
    config = SearchConfig.from_env()
    with create_client(config) as client:
        if args.command == "init":
            initialize_indices(config, client)
            print(json.dumps({"indices": [config.index(kind) for kind in KINDS]}))
        elif args.command == "inspect":
            client.indices.refresh(index=",".join(config.alias(kind) for kind in KINDS))
            print(
                json.dumps(
                    {kind: client.count(index=config.alias(kind))["count"] for kind in KINDS},
                    sort_keys=True,
                )
            )
        elif args.group_by:
            print(json.dumps(aggregate(client, config, args.group_by)))
        else:
            options = {
                name: getattr(args, name)
                for name in (
                    "asset_id",
                    "severity",
                    "rule_id",
                    "diagnostic_type",
                    "start",
                    "end",
                    "latest",
                    "size",
                )
            }
            result = query_documents(client, config, args.kind, **options)
            print(
                json.dumps(
                    {
                        "total": result["hits"]["total"]["value"],
                        "documents": [hit["_source"] for hit in result["hits"]["hits"]],
                    }
                )
            )


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    main()
