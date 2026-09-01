"""p0's on-prem driver adapter over utility.drivers.onprem_* (Postgres + IoTDB + RustFS)."""

from __future__ import annotations

import os

from utility.drivers.onprem_iotdb_driver import OnpremIoTDBDriver as IoTDBDriver
from utility.drivers.onprem_postgre_driver import (
    OnpremPostgresDriver as _UpstreamPostgresDriver,
)
from utility.drivers.onprem_rustfs_driver import (
    ensure_rustfs_bucket,
    get_rustfs_client,
    get_rustfs_s3fs,
    get_rustfs_settings,
)

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
    """This adapter is the on-prem backend."""
    return "onprem"


def storage_backend() -> str:
    """On-prem object store: RustFS."""
    return "rustfs"


def object_store_scheme() -> str:
    """URI scheme for RustFS ('s3')."""
    return "s3"


def timeseries_backend() -> str:
    """On-prem timeseries store: IoTDB."""
    return "iotdb"


def object_store_container() -> str:
    """RustFS bucket every p0 object lives under."""
    return os.environ.get("RUSTFS_BUCKET") or ""


def get_object_fs(use_listings_cache: bool = False):
    """fsspec filesystem for RustFS — the single storage entry point."""
    return get_rustfs_s3fs(use_listings_cache=use_listings_cache)


def ensure_object_container(name: str) -> bool:
    """Idempotently create the RustFS bucket."""
    return ensure_rustfs_bucket(name)


def presign_object_url(container: str, key: str, expires_seconds: int = 3600) -> str:
    """Browser-usable temporary URL for one RustFS object."""
    client = get_rustfs_client(signature_version="s3v4")
    return client.generate_presigned_url(
        "get_object", Params={"Bucket": container, "Key": key}, ExpiresIn=expires_seconds
    )


class AzureSQLDriver:
    """Placeholder so p0 can import the name on-prem; azure paths never run here."""

    def __init__(self, *args, **kwargs):
        raise RuntimeError(
            "AzureSQLDriver is unavailable on-prem (DEPLOYMENT_MODE=onprem); "
            "the azure timeseries backend only runs when DEPLOYMENT_MODE=azure"
        )
