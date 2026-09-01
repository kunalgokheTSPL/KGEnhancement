"""
Base connector class — all source connectors inherit from this.
"""

import abc
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
import pandas as pd

from connectors.destination import DestinationWriter

logger = logging.getLogger(__name__)


def _resolve_env(val):
    """Replace ${VAR:-default} patterns with environment values."""

    def _repl(m):
        var = m.group(1)
        default = m.group(3) if m.group(3) is not None else ""
        return os.environ.get(var, default)

    if not isinstance(val, str):
        return val
    return re.sub(r"\$\{([A-Z_][A-Z0-9_]*)(?:(:-)(.*?))?\}", _repl, val)


def _resolve_config(cfg):
    if isinstance(cfg, dict):
        return {k: _resolve_config(v) for k, v in cfg.items()}
    if isinstance(cfg, list):
        return [_resolve_config(v) for v in cfg]
    if isinstance(cfg, str):
        return _resolve_env(cfg)
    return cfg


def load_connector_config(config_path: str) -> Dict[str, Any]:
    """Load and env-resolve a connector YAML config."""
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)
    return _resolve_config(raw)


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base (override wins on conflicts)."""
    merged = base.copy()
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


class BaseConnector(abc.ABC):
    """Every connector must implement:"""

    DATA_TYPE: str = ""

    def __init__(
        self,
        config_path: str,
        destination_config_path: str,
        source_type: str,
        destination_type: str = None,
    ):
        self.config = load_connector_config(config_path)
        common = self.config.get("common", {})
        sources = self.config.get("sources", {})
        if sources and source_type in sources:
            self.source_cfg = _deep_merge(common, sources[source_type])
        elif "source" in self.config:
            self.source_cfg = self.config["source"]
        else:
            self.source_cfg = common
        self.source_cfg["type"] = source_type
        self.destination = DestinationWriter(destination_config_path, destination_type)

    @abc.abstractmethod
    def validate_config(self):
        """Raise ValueError if required config fields are missing."""
        ...

    @abc.abstractmethod
    def connect(self):
        """Establish or verify connectivity. Raise on failure."""
        ...

    @abc.abstractmethod
    def extract(self) -> list:
        """Pull data from the source."""
        ...

    def run(self):
        """Full lifecycle: validate → connect → extract → write."""
        logger.info("[%s] Validating config...", self.__class__.__name__)
        self.validate_config()
        logger.info("[%s] Connecting to source...", self.__class__.__name__)
        self.connect()
        logger.info("[%s] Extracting data...", self.__class__.__name__)
        results = self.extract()
        logger.info(
            "[%s] Writing %d result(s) to destination...",
            self.__class__.__name__,
            len(results),
        )
        for item in results:
            df = item.pop("df")
            filename = item.pop("filename")
            self.destination.write_dataframe(
                df,
                self.DATA_TYPE,
                filename,
                **item,
            )
        logger.info("[%s] Done.", self.__class__.__name__)
