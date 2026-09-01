"""Shared dependencies — YAML helpers, path validation, DB connections."""

from __future__ import annotations

import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

import yaml
from fastapi import HTTPException

from .config import CONFIG_DIR


_SAFE_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def validate_id(value: str, label: str = "identifier") -> str:
    """Validate that *value* is a safe alphanumeric/underscore identifier."""
    if not _SAFE_ID.match(value):
        raise HTTPException(
            400, f"Invalid {label}: must be alphanumeric/underscore, max 64 chars"
        )
    return value


def load_yaml(path: Path | str) -> dict:
    """Load a YAML file. Accepts a Path or a str.  Returns empty dict on missing or empty files."""
    p = path if isinstance(path, Path) else Path(path)
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_yaml(path: Path, data: dict) -> None:
    """Atomic write: write to .tmp then rename."""
    tmp = path.with_suffix(".yaml.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        yaml.dump(
            data, f, default_flow_style=False, sort_keys=False, allow_unicode=True
        )
    shutil.move(str(tmp), str(path))


def backup(path: Path) -> Path:
    """Create a timestamped backup before mutating any config."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_dir = CONFIG_DIR / ".backups"
    backup_dir.mkdir(exist_ok=True)
    dst = backup_dir / f"{path.stem}_{ts}{path.suffix}"
    shutil.copy2(str(path), str(dst))
    return dst


def deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into *base* (new dict — inputs untouched)."""
    result = {**base}
    for key, val in override.items():
        if isinstance(val, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], val)
        else:
            result[key] = val
    return result
