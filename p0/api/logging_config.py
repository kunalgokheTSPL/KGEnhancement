"""
Centralized logging configuration for the CDM API.

Call ``setup_logging(log_dir)`` once at startup (in main.py).

Log files written to DATA_DIR/logs/:
  api.log         — all CDM activity (api + connectors + pipeline)
  connectors.log  — all connector activity (upload → RustFS staging), all sources
  pnid.log        — P&ID end-to-end: upload → RustFS staging → pipeline → DB
  docs.log        — Documents end-to-end: upload → RustFS staging → pipeline → DB
  sap.log         — SAP end-to-end: upload → RustFS staging → pipeline → DB
  timeseries.log  — Timeseries end-to-end: upload → RustFS/IoTDB → DB

Logger hierarchy (all under "cdm" root):
  cdm                        → api.log  (root CDM logger)
  cdm.connector              → connectors.log  (propagates to cdm → api.log)
  cdm.connector.pnid         → pnid.log  (propagates to cdm.connector → connectors.log → api.log)
  cdm.connector.docs         → docs.log
  cdm.connector.sap          → sap.log
  cdm.connector.timeseries   → timeseries.log
  cdm.pipeline.pnid          → pnid.log  (propagates to cdm → api.log)
  cdm.pipeline.docs          → docs.log
  cdm.pipeline.sap           → sap.log
  cdm.pipeline.timeseries    → timeseries.log
  cdm.api.*                  → api.log  (propagates to cdm)
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

_FMT = "%(asctime)s  %(levelname)-8s  %(name)s:%(lineno)d  %(message)s"
_DATE = "%Y-%m-%dT%H:%M:%S"
_MAX_BYTES = 10 * 1024 * 1024
_BACKUP_COUNT = 5


def _rotating_handler(path: Path) -> logging.handlers.RotatingFileHandler:
    h = logging.handlers.RotatingFileHandler(
        path,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    h.setLevel(logging.DEBUG)
    h.setFormatter(logging.Formatter(_FMT, datefmt=_DATE))
    return h


def setup_logging(log_dir: Path) -> None:
    """Wire all CDM log handlers.  Call once at application startup."""
    log_dir.mkdir(parents=True, exist_ok=True)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(
        logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt=_DATE,
        )
    )

    api_log_h = _rotating_handler(log_dir / "api.log")
    for root_name in ("cdm", "p0"):
        root = logging.getLogger(root_name)
        root.setLevel(logging.DEBUG)
        root.propagate = False
        root.addHandler(api_log_h)
        root.addHandler(console)

    logging.getLogger("cdm.connector").addHandler(
        _rotating_handler(log_dir / "connectors.log")
    )

    for source in ("pnid", "docs", "sap", "timeseries"):
        h = _rotating_handler(log_dir / f"{source}.log")
        logging.getLogger(f"cdm.connector.{source}").addHandler(h)
        logging.getLogger(f"cdm.pipeline.{source}").addHandler(h)
