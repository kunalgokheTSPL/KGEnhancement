"""p0's azure driver adapter over utility.drivers.azure_* (Azure Postgres + Azure SQL + ADLS)."""

from __future__ import annotations

import os

from utility.drivers.azure_adls_driver import (
    ensure_rustfs_bucket,
    get_rustfs_s3fs,
    get_rustfs_settings,
)
from utility.drivers.azure_postgres_driver import (
    AzurePostgresDriver as _UpstreamPostgresDriver,
)
from utility.drivers.azure_sql_driver import AzureSQLDriver
from utility.drivers.onprem_iotdb_driver import OnpremIoTDBDriver as IoTDBDriver

from ._plant_db import plant_scoped

PostgresDriver = plant_scoped(_UpstreamPostgresDriver)

__all__ = [
    "AzureSQLDriver",
    "IoTDBDriver",
    "PostgresDriver",
    "deployment_mode",
    "ensure_object_container",
    "get_object_fs",
    "get_rustfs_settings",
    "object_store_container",
    "object_store_scheme",
    "presign_object_url",
    "storage_backend",
    "timeseries_backend",
]


def deployment_mode() -> str:
    """This adapter is the azure backend."""
    return "azure"


def storage_backend() -> str:
    """Azure object store: ADLS."""
    return "adls"


def object_store_scheme() -> str:
    """URI scheme for ADLS ('abfs')."""
    return "abfs"


def timeseries_backend() -> str:
    """Azure timeseries store: Azure SQL."""
    return "azuresql"


def object_store_container() -> str:
    """ADLS container every p0 object lives under."""
    return os.environ.get("ADLS_CONTAINER") or "decisionops"


def get_object_fs(use_listings_cache: bool = False):
    """fsspec filesystem for ADLS — the single storage entry point."""
    return get_rustfs_s3fs(use_listings_cache=use_listings_cache)


def ensure_object_container(name: str) -> bool:
    """Idempotently create the ADLS container."""
    return ensure_rustfs_bucket(name)


def presign_object_url(container: str, key: str, expires_seconds: int = 3600) -> str:
    """Browser-usable temporary SAS URL for one ADLS object."""
    from datetime import datetime, timedelta, timezone

    from azure.storage.blob import BlobSasPermissions, generate_blob_sas

    account_name = os.environ.get("ADLS_ACCOUNT_NAME")
    account_key = os.environ.get("ADLS_ACCOUNT_KEY")
    if not account_key:
        raise ValueError(
            "SAS presigning needs ADLS_ACCOUNT_KEY (use Managed Identity + user delegation otherwise)"
        )
    sas = generate_blob_sas(
        account_name=account_name,
        container_name=container,
        blob_name=key,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(seconds=expires_seconds),
    )
    return f"https://{account_name}.blob.core.windows.net/{container}/{key}?{sas}"
