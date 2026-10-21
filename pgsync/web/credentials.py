"""Credential placeholder helpers for PGSync Studio."""

from __future__ import annotations

import os
import re
import typing as t


ENV_PLACEHOLDER_PATTERN = re.compile(
    r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}"
)


def resolve_env_placeholders(value: t.Any) -> t.Any:
    """Resolve ${ENV_NAME} placeholders in strings without mutating payloads."""

    if isinstance(value, dict):
        return {
            key: resolve_env_placeholders(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [resolve_env_placeholders(item) for item in value]
    if isinstance(value, tuple):
        return tuple(resolve_env_placeholders(item) for item in value)
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        env_name = match.group(1)
        default = match.group(2)
        if env_name in os.environ:
            return os.environ.get(env_name, "")
        return "" if default is None else default

    return ENV_PLACEHOLDER_PATTERN.sub(replace, value)
