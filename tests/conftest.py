import json
import os
import subprocess
from hashlib import sha256
from pathlib import Path

import pytest

from workloads.observed import GitSource, extract


@pytest.fixture
def observed_fixture(tmp_path):
    repository = tmp_path / "repository"
    repository.mkdir()
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    commits = []
    license_bytes = b"Original project fixture; no external configuration content.\n"
    for state in json.loads(Path("tests/fixtures/phase11/history.json").read_text()):
        for file in (repository / "conf").glob("*"):
            file.unlink()
        for name, content in {**state["files"], "LICENSE": license_bytes.decode()}.items():
            file = repository / name
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(content)
        environment = {
            **os.environ,
            "GIT_AUTHOR_NAME": "Fixture",
            "GIT_COMMITTER_NAME": "Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
            "GIT_AUTHOR_DATE": state["time"],
            "GIT_COMMITTER_DATE": state["time"],
        }
        subprocess.run(["git", "-C", str(repository), "add", "-A"], check=True)
        subprocess.run(
            ["git", "-C", str(repository), "commit", "-qm", "fixture"], env=environment, check=True
        )
        commits.append(
            subprocess.check_output(
                ["git", "-C", str(repository), "rev-parse", "HEAD"], text=True
            ).strip()
        )
    source = GitSource(
        name="fixture",
        repository="nginx/nginx",
        default_branch="master",
        start_revision=commits[0],
        end_revision=commits[-1],
        shallow_since="2023-12-31T00:00:00Z",
        license="Original test fixture",
        license_path="LICENSE",
        license_sha256=sha256(license_bytes).hexdigest(),
        scope="conf",
        allowed_patterns=["conf/*.conf"],
        asset_type="nginx_server",
    )
    destination = tmp_path / "observed/fixture"
    manifest = extract(source, repository / ".git", destination)
    return destination.parent, source, manifest


@pytest.fixture
def terraform_fixture(tmp_path):
    from workloads.terrads_history import TerraformSource, terraform_rejection

    repository = tmp_path / "terraform"
    repository.mkdir()
    subprocess.run(["git", "init", "-q", str(repository)], check=True)
    license_bytes = b"Permission is hereby granted, free of charge, to any person.\n"
    commits = []
    for ordinal, state in enumerate(
        json.loads(Path("tests/fixtures/phase12/history.json").read_text())
    ):
        for file in repository.glob("*.tf"):
            file.unlink()
        for name, content in state["files"].items():
            (repository / name).write_text(content)
        (repository / "LICENSE").write_bytes(license_bytes)
        (repository / "README.md").write_text(f"Fixture ordinal {ordinal}\n")
        environment = {
            **os.environ,
            "GIT_AUTHOR_NAME": "Fixture",
            "GIT_COMMITTER_NAME": "Fixture",
            "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
            "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
            "GIT_AUTHOR_DATE": state["time"],
            "GIT_COMMITTER_DATE": state["time"],
        }
        subprocess.run(["git", "-C", str(repository), "add", "-A"], check=True)
        subprocess.run(
            ["git", "-C", str(repository), "commit", "-qm", "fixture"], env=environment, check=True
        )
        commits.append(
            subprocess.check_output(
                ["git", "-C", str(repository), "rev-parse", "HEAD"], text=True
            ).strip()
        )
    subprocess.run(
        ["git", "-C", str(repository), "remote", "add", "origin", str(repository)], check=True
    )
    source = TerraformSource(
        name="terrads-1",
        repository="fixture/owned",
        start_revision=commits[0],
        end_revision=commits[-1],
        license="MIT",
        license_path="LICENSE",
        license_sha256=sha256(license_bytes).hexdigest(),
        repository_id=1,
        sample_seed=42,
        stratum="size-0",
    )
    destination = tmp_path / "terrads/history/terrads-1"
    manifest = extract(
        source,
        repository / ".git",
        destination,
        content_filter=terraform_rejection,
        incarnation_rejection=True,
        max_file_bytes=1024**2,
    )
    return destination.parent, source, manifest
