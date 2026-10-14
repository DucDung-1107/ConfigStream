"""Owned-fixture Kafka/MinIO persistence inventory, safe to retain without payload logs."""

import json
from hashlib import sha256


def inventory():
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import AdminClient, OffsetSpec

    from lake.client import s3_client
    from lake.config import LakeConfig
    from messaging.config import KafkaConfig

    lake = LakeConfig.from_env()
    client = s3_client(lake)
    objects = []
    for prefix in ("closure-20k1/", "closure-20k1-recovery/"):
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=lake.bucket, Prefix=prefix
        ):
            for entry in page.get("Contents", []):
                response = client.get_object(Bucket=lake.bucket, Key=entry["Key"])
                checksum = sha256()
                for block in iter(lambda body=response["Body"]: body.read(1024**2), b""):
                    checksum.update(block)
                objects.append(
                    {"key": entry["Key"], "bytes": entry["Size"], "sha256": checksum.hexdigest()}
                )
    admin = AdminClient(KafkaConfig.from_env().client_options())
    offsets = admin.list_offsets(
        {TopicPartition("config.snapshots", 0): OffsetSpec.latest()}, request_timeout=15
    )
    end = next(iter(offsets.values())).result(15).offset
    return {
        "objects": sorted(objects, key=lambda entry: entry["key"]),
        "kafka_config_snapshots_end_offset": end,
    }


if __name__ == "__main__":
    print(json.dumps(inventory(), sort_keys=True))
