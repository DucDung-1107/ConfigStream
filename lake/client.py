import json
from uuid import UUID

from lake.config import LakeConfig


def s3_client(config: LakeConfig):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=config.endpoint,
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=5,
            read_timeout=10,
            retries={"mode": "standard", "total_max_attempts": 3},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def ensure_bucket(config: LakeConfig) -> None:
    from botocore.exceptions import ClientError

    client = s3_client(config)
    try:
        client.head_bucket(Bucket=config.bucket)
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        options = (
            {}
            if config.region == "us-east-1"
            else {"CreateBucketConfiguration": {"LocationConstraint": config.region}}
        )
        client.create_bucket(Bucket=config.bucket, **options)


def commit_key(config: LakeConfig, query_id: str, batch_id: int) -> str:
    UUID(query_id)
    if type(batch_id) is not int or batch_id < 0:
        raise ValueError("batch_id must be a nonnegative integer")
    return config.object_key(f"_commits/{query_id}/{batch_id:020d}.json")


def load_commit(config: LakeConfig, query_id: str, batch_id: int) -> dict | None:
    from botocore.exceptions import ClientError

    client = s3_client(config)
    try:
        response = client.get_object(
            Bucket=config.bucket, Key=commit_key(config, query_id, batch_id)
        )
    except ClientError as error:
        if error.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    with response["Body"] as body:
        return json.loads(body.read())


def save_commit(config: LakeConfig, manifest: dict) -> None:
    s3_client(config).put_object(
        Bucket=config.bucket,
        Key=commit_key(config, manifest["query_id"], manifest["batch_id"]),
        Body=json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        IfNoneMatch="*",
    )


def list_commits(config: LakeConfig, query_id: str | None = None):
    client = s3_client(config)
    suffix = "_commits/"
    if query_id is not None:
        UUID(query_id)
        suffix += f"{query_id}/"
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=config.bucket, Prefix=config.object_key(suffix)
    ):
        for entry in page.get("Contents", []):
            if entry["Key"].endswith(".json"):
                response = client.get_object(Bucket=config.bucket, Key=entry["Key"])
                with response["Body"] as body:
                    yield json.loads(body.read())
