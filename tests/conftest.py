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
