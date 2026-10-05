import json
import re
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

import yaml
from pydantic import Field, ValidationError, field_validator

from schemas import Alert, ConfigDiff, Severity
from schemas.base import NonBlankString, SchemaModel


class RuleConfigurationError(ValueError):
    """Rules must be valid before query startup."""


class Rule(SchemaModel):
    id: NonBlankString
    enabled: bool = Field(default=True, strict=True)
    severity: Severity
    operation: Literal["added", "removed", "any"]
    pattern: str = Field(strict=True, min_length=1)
    message: NonBlankString

    @field_validator("pattern")
    @classmethod
    def valid_regex(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Pattern must not be blank")
        try:
            re.compile(value)
        except re.error:
            raise ValueError("Invalid regular expression") from None
        return value


class RuleFile(SchemaModel):
    rules: list[Rule]


def load_rules(path: str | Path) -> tuple[Rule, ...]:
    try:
        document = RuleFile.model_validate(yaml.safe_load(Path(path).read_text(encoding="utf-8")))
    except (OSError, yaml.YAMLError, ValidationError):
        raise RuleConfigurationError(
            "Cannot load rules: check file, YAML, and rule fields"
        ) from None
    identifiers = [rule.id for rule in document.rules]
    if len(set(identifiers)) != len(identifiers):
        raise RuleConfigurationError("Duplicate rule IDs")
    return tuple(document.rules)


def evaluate_rules(diff: ConfigDiff, event_time: datetime, rules: tuple[Rule, ...]) -> list[Alert]:
    alerts = []
    for rule in rules:
        if not rule.enabled:
            continue
        if any(
            (rule.operation == "any" or rule.operation == detail.operation)
            and re.fullmatch(rule.pattern, detail.line) is not None
            for detail in diff.details
        ):
            identity = json.dumps(
                ["configstream:alert:v1", diff.diff_id, rule.id], separators=(",", ":")
            )
            alerts.append(
                Alert(
                    alert_id=f"alert-{uuid5(NAMESPACE_URL, identity)}",
                    asset_id=diff.asset_id,
                    snapshot_id=diff.new_snapshot_id,
                    rule_id=rule.id,
                    severity=rule.severity,
                    message=rule.message,
                    created_at=event_time,
                )
            )
    return alerts
