import importlib.util
import sys
from pathlib import Path

import pytest

from lake.config import LakeConfig, lake_enabled
from spark.common.config import SparkConfig


def test_credentials_required_and_not_in_representation() -> None:
    with pytest.raises(ValueError, match="Export MINIO_ROOT_USER"):
        LakeConfig.from_env({})
    config = LakeConfig("test-user", "synthetic-private-value")
    assert "test-user" not in repr(config)
    assert "synthetic-private-value" not in repr(config)


def test_loading_and_safe_paths() -> None:
    config = LakeConfig.from_env(
        {
            "MINIO_ROOT_USER": "test-user",
            "MINIO_ROOT_PASSWORD": "test-value",
            "MINIO_ENDPOINT": "http://127.0.0.1:9000",
            "CONFIGSTREAM_LAKE_BUCKET": "test-lake",
            "CONFIGSTREAM_LAKE_PREFIX": "acceptance/run-1",
            "MINIO_REGION": "us-east-1",
            "CONFIGSTREAM_LAKE_MAX_RECORDS_PER_FILE": "10",
        }
    )
    assert config.max_records_per_file == 10
    assert (
        config.uri("processed/snapshots") == "s3a://test-lake/acceptance/run-1/processed/snapshots"
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("endpoint", "ftp://localhost"),
        ("endpoint", "http://user:private@host"),
        ("endpoint", "http://host/private"),
        ("endpoint", "http://host?token=private"),
        ("endpoint", "http://host#fragment"),
        ("bucket", "Uppercase"),
        ("bucket", "ab"),
        ("bucket", "one..two"),
        ("bucket", "127.0.0.1"),
        ("prefix", "../private"),
        ("prefix", "/absolute"),
        ("prefix", "one//two"),
        ("region", ""),
        ("max_records_per_file", 0),
        ("max_records_per_file", True),
    ],
)
def test_invalid_config_rejected(field, value) -> None:
    with pytest.raises(ValueError):
        LakeConfig(**{"access_key": "test-user", "secret_key": "test-value", field: value})


def test_enabled_mode_has_separate_default_checkpoint() -> None:
    config = SparkConfig.from_env(
        {
            "CONFIGSTREAM_LAKE_ENABLED": "true",
            "MINIO_ROOT_USER": "test-user",
            "MINIO_ROOT_PASSWORD": "test-value",
        }
    )
    assert config.lake is not None
    assert config.checkpoint_dir == Path(".cache/spark-checkpoints/config-snapshots-v9")
    assert SparkConfig.from_env({}).lake is None
    assert not lake_enabled({"CONFIGSTREAM_LAKE_ENABLED": "FALSE"})
    with pytest.raises(ValueError):
        lake_enabled({"CONFIGSTREAM_LAKE_ENABLED": "yes"})


def test_local_credential_file_is_private_and_never_overwritten(tmp_path, monkeypatch) -> None:
    from scripts.lake_dev_credentials import main

    monkeypatch.chdir(tmp_path)
    main()
    path = Path(".cache/minio.env")
    first = path.read_bytes()
    assert path.stat().st_mode & 0o777 == 0o600
    main()
    assert path.read_bytes() == first


def test_core_imports_do_not_load_optional_clients() -> None:
    import subprocess

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import processing; import lake.config; import sys; "
            "assert 'boto3' not in sys.modules; assert 'pyspark' not in sys.modules",
        ],
        check=False,
    )
    assert result.returncode == 0
    assert importlib.util.find_spec("lake") is not None
