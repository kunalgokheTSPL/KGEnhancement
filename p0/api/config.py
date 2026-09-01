"""
Application-wide configuration — paths, feature flags, constants.

All path resolution is relative to p0_DIR so the API works regardless
of whether it runs from the repo checkout or inside a container image.
"""

from __future__ import annotations

import os
from pathlib import Path
import re as _re
import yaml as _yaml

BASE_DIR = Path(__file__).resolve().parent.parent.parent
p0_DIR = BASE_DIR / "p0"
p0_ROOT = p0_DIR
CONFIG_DIR = p0_DIR / "config"
DATA_DIR = Path(
    os.environ.get("P0_DATA_DIR") or os.environ.get("p0_DATA_DIR", str(p0_DIR / "data"))
)

_ENV_YAML = BASE_DIR / "env.yaml"
_env: dict = {}
if _ENV_YAML.is_file():
    with open(_ENV_YAML) as _f:
        _env = _yaml.safe_load(_f) or {}

STAGING_DIR = DATA_DIR / "staging"
LOG_DIR = Path(os.environ.get("P0_LOG_DIR") or DATA_DIR / "logs")

WORK_DIR = DATA_DIR / "work"

CONNECTORS_DIR = p0_ROOT / "connectors"

TEMPLATES_DIR = CONFIG_DIR / "templates"
COMMON_DIR = TEMPLATES_DIR / "_common"

SAP_EXTRACTION_FILE = COMMON_DIR / "sap_extraction.yaml"
SAP_JOIN_COLUMNS_FILE = COMMON_DIR / "sap_join_columns.yaml"
SAP_RENAME_FILE = COMMON_DIR / "column_rename" / "sap_column_rename.yaml"
CDM_CONFIG_FILE = COMMON_DIR / "cdm_config.yaml"
ENTITIES_FROZEN_FILE = COMMON_DIR / "entities.yaml"
SCHEMA_FROZEN_FILE = COMMON_DIR / "schema.yaml"
RELATIONSHIPS_FROZEN_FILE = COMMON_DIR / "relationships.yaml"
VALIDATION_CONTRACTS_FILE = COMMON_DIR / "validation_contracts.yaml"

USER_CONFIG_FILE = CONFIG_DIR / "user_config.yaml"

PNID_CONFIG = CONNECTORS_DIR / "pnid" / "config.yaml"
DOCS_CONFIG = CONNECTORS_DIR / "documents" / "config.yaml"
SAP_CONFIG = CONNECTORS_DIR / "sap" / "config.yaml"
TS_CONFIG = CONNECTORS_DIR / "timeseries" / "config.yaml"
DEST_CONFIG = CONNECTORS_DIR / "destination" / "config.yaml"

UPLOAD_STAGING_PNID = DATA_DIR / "staging" / "pnid"
UPLOAD_STAGING_DOCS = DATA_DIR / "staging" / "documents"
UPLOAD_STAGING_SAP = DATA_DIR / "staging" / "sap"
UPLOAD_STAGING_TS = DATA_DIR / "staging" / "ts"
UPLOAD_STAGING_AIF = DATA_DIR / "staging" / "aif"
UPLOAD_STAGING_GLOC = DATA_DIR / "staging" / "gloc"
UPLOAD_STAGING_LOPC = DATA_DIR / "staging" / "lopc"
UPLOAD_STAGING_ALERTS = DATA_DIR / "staging" / "alerts"

FINDINGS_SOURCES_FILE = COMMON_DIR / "findings_sources.yaml"
ALERTS_SOURCES_FILE = COMMON_DIR / "alerts_sources.yaml"

PROCESSED_DIR = DATA_DIR / "processed"

from p0.driver import (
    PostgresDriver as _PostgresDriver,
    IoTDBDriver as _IoTDBDriver,
    deployment_mode as _deployment_mode,
    get_rustfs_settings as _get_rustfs_settings,
)

_cdm_cfg = _PostgresDriver().config
CDM_DB_HOST = _cdm_cfg.get("host")
CDM_DB_PORT = _cdm_cfg.get("port")
CDM_DB_NAME = _cdm_cfg.get("database")
CDM_DB_USER = _cdm_cfg.get("user")
CDM_DB_PASS = _cdm_cfg.get("password")
CDM_DB_SSLMODE = None

if _deployment_mode() == "azure":
    CDM_DB_HOST = os.environ.get("AZURE_PG_HOST")
    CDM_DB_USER = os.environ.get("AZURE_PG_USER")
    CDM_DB_PASS = os.environ.get("AZURE_PG_PASSWORD") or ""
    CDM_DB_PORT = int(os.environ.get("AZURE_PG_PORT") or 5432)
    CDM_DB_SSLMODE = "require"

_iot_cfg = _IoTDBDriver().config
IOTDB_HOST = _iot_cfg.get("host")
IOTDB_PORT = _iot_cfg.get("port")
IOTDB_USER = _iot_cfg.get("user")
IOTDB_PASSWORD = _iot_cfg.get("password")
IOTDB_DEVICE_ROOT = _iot_cfg.get("device_root")

_rustfs = _env.get("rustfs", {})
RUSTFS_ENDPOINT, RUSTFS_ACCESS_KEY, RUSTFS_SECRET_KEY = _get_rustfs_settings()

from p0.driver import object_store_container, object_store_scheme, storage_backend

STORAGE_BACKEND = storage_backend()
OBJECT_SCHEME = object_store_scheme()
RUSTFS_BUCKET = object_store_container()
RUSTFS_BASE_PREFIX = _rustfs.get("base_prefix", "staging")

RUSTFS_P0_PREFIX = "p0"
RUSTFS_P0_ROOT = f"{OBJECT_SCHEME}://{RUSTFS_BUCKET}/{RUSTFS_P0_PREFIX}"

RUSTFS_DATA_PNID = f"{RUSTFS_P0_ROOT}/data/pnid"

_cors_raw = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:3000,http://localhost:3001,http://localhost:3002",
)
CORS_ORIGINS: list[str] = [o.strip() for o in _cors_raw.split(",") if o.strip()]

API_TITLE = "DecisionOps CDM Admin API"
API_DESCRIPTION = """
Unified REST API for the DecisionOps Canonical Data Model (p0) — the industrial
data-onboarding and governance plane. Ingest raw sources (P&ID, SAP, documents,
timeseries), review and commit them into a multi-tenant Canonical Data Model
(PostgreSQL + IoTDB), and query the result.

**Auth:** all endpoints except `/api/health/*` require a Bearer token (Keycloak).
**Multi-tenant:** every data endpoint is scoped by `plant_code_id`.

## Sections (full surface)

| Router           | Purpose |
|------------------|---------|
| **Health**       | Liveness / readiness probes (no auth) |
| **Configuration**| Manage SAP tables, column-rename mappings, `user_config` overrides, view frozen CDM schemas |
| **Connectors**   | Upload files (P&ID / documents / timeseries) or connect cloud / OneDrive sources |
| **Pipeline**     | Trigger and monitor CDM pipeline runs (SAP, P&ID, TS, Docs, Full merge) |
| **Decision Context** | Review processed output, patch rows, and **commit** to the CDM (P&ID / SAP / Docs / TS) |
| **Flow**         | Per-file onboarding lifecycle state (uploaded → processed → committed) |
| **Logs**         | Pipeline log tails + structured activity feed, parsed log events, and summary counts |
| **CDM Data**     | Query committed CDM tables — equipment, work orders, relationships, arbitrary tables |
| **Ontology / Knowledge Graph** | Export the CDM as a connected knowledge graph |

## Conventions

- **Lifecycle:** upload → process (pipeline) → review/patch → commit. Each hop is
  recorded (`flow_state`, `pipeline_jobs`, `commit_audit`, `run_events`).
- **Idempotent commits:** re-committing the same file does not duplicate
  (per-file UPSERT / replace-by-source_file; see the Decision Context endpoints).
- **Errors:** `400` missing/invalid `plant_code_id`, `401` no/invalid token,
  `404` unknown resource, `409` safe-delete blocked by references.
"""
API_VERSION = "1.0.0"





_STAGING_PT_ROOT = f"{RUSTFS_P0_ROOT}/staging-pt"
_PROCESSED_ROOT = f"{RUSTFS_P0_ROOT}/processed_data"


def _sanitise_plant(plant_code_id: str | None) -> str:
    """Defensive scrub for the plant_code_id path segment."""
    if not plant_code_id or not _re.fullmatch(r"[A-Za-z0-9_-]{1,50}", plant_code_id):
        raise ValueError(
            f"invalid plant_code_id {plant_code_id!r} — must match [A-Za-z0-9_-]{{1,50}}"
        )
    return plant_code_id


def rustfs_staging_pnid(plant_code_id: str) -> str:
    return f"{_STAGING_PT_ROOT}/{_sanitise_plant(plant_code_id)}/pnid"


def rustfs_staging_docs(plant_code_id: str) -> str:
    return f"{_STAGING_PT_ROOT}/{_sanitise_plant(plant_code_id)}/documents"

def rustfs_staging_gloc(plant_code_id: str) -> str:
    return f"{_STAGING_PT_ROOT}/{_sanitise_plant(plant_code_id)}/gloc"

def rustfs_staging_alerts(plant_code_id: str) -> str:
    return f"{_STAGING_PT_ROOT}/{_sanitise_plant(plant_code_id)}/alerts"

def rustfs_staging_ts(plant_code_id: str) -> str:
    return f"{_STAGING_PT_ROOT}/{_sanitise_plant(plant_code_id)}/timeseries"


def rustfs_staging_sap(plant_code_id: str) -> str:
    return f"{_STAGING_PT_ROOT}/{_sanitise_plant(plant_code_id)}/sap"


_SHARED_STAGING_FOLDER: dict[str, str] = {
    "upd_event": "events",
    "trip_event": "events",
}


def rustfs_staging_findings(plant_code_id: str, connector: str) -> str:
    folder = _SHARED_STAGING_FOLDER.get(connector.lower(), connector.lower())
    return f"{_STAGING_PT_ROOT}/{_sanitise_plant(plant_code_id)}/{folder}"


def rustfs_processed_findings(plant_code_id: str, connector: str) -> str:
    return f"{_PROCESSED_ROOT}/{_sanitise_plant(plant_code_id)}/{connector.lower()}"


def rustfs_processed_pnid(plant_code_id: str) -> str:
    return f"{_PROCESSED_ROOT}/{_sanitise_plant(plant_code_id)}/pnid"


def rustfs_processed_docs(plant_code_id: str) -> str:
    return f"{_PROCESSED_ROOT}/{_sanitise_plant(plant_code_id)}/documents"


def rustfs_processed_ts(plant_code_id: str) -> str:
    return f"{_PROCESSED_ROOT}/{_sanitise_plant(plant_code_id)}/timeseries"

def rustfs_processed_gloc(plant_code_id: str) -> str:
    return f"{_PROCESSED_ROOT}/{_sanitise_plant(plant_code_id)}/gloc"

def rustfs_processed_alerts(plant_code_id: str) -> str:
    return f"{_PROCESSED_ROOT}/{_sanitise_plant(plant_code_id)}/alerts"


def rustfs_processed_sap(plant_code_id: str) -> str:
    return f"{_PROCESSED_ROOT}/{_sanitise_plant(plant_code_id)}/sap"


def rustfs_processed_sap_mapping(plant_code_id: str) -> str:
    return f"{_PROCESSED_ROOT}/{_sanitise_plant(plant_code_id)}/sap_mapping"


RUSTFS_OUT_PREFIX = f"{RUSTFS_P0_ROOT}/cdm_out"


def rustfs_out_dir(plant_code_id: str) -> str:
    """Plant-scoped canonical-output root: ``s3://…/p0/cdm_out/<plant>``."""
    return f"{RUSTFS_OUT_PREFIX}/{_sanitise_plant(plant_code_id)}"


class _S3PathLike(str):
    """Drop-in replacement for the old ``OUT_DIR = Path(...)`` value."""

    __slots__ = ()

    def __truediv__(self, other: str) -> "_S3PathLike":
        return _S3PathLike(f"{str(self).rstrip('/')}/{str(other).lstrip('/')}")

    def __rtruediv__(self, other: str) -> "_S3PathLike":  # pragma: no cover
        return _S3PathLike(f"{str(other).rstrip('/')}/{str(self).lstrip('/')}")


OUT_DIR: _S3PathLike = _S3PathLike(RUSTFS_OUT_PREFIX)
