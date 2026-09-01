"""p0's aws driver adapter over utility.drivers.aws_* (selection wired; S3 client is still a stub upstream)."""

from __future__ import annotations

import os

from utility.drivers.aws_iotdb_driver import AwsIoTDBDriver as IoTDBDriver
from utility.drivers.aws_postgres_driver import (
    AwsPostgresDriver as _UpstreamPostgresDriver,
)
from utility.drivers.aws_s3_driver import (
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
    """This adapter is the aws backend."""
    return "aws"


def storage_backend() -> str:
    """AWS object store: S3."""
    return "s3"


def object_store_scheme() -> str:
    """URI scheme for S3 ('s3')."""
    return "s3"


def timeseries_backend() -> str:
    """AWS timeseries store: IoTDB."""
    return "iotdb"


def object_store_container() -> str:
    """S3 bucket every p0 object lives under."""
    return os.environ.get("S3_BUCKET") or os.environ.get("RUSTFS_BUCKET") or ""


def get_object_fs(use_listings_cache: bool = False):
    """fsspec filesystem for S3 — the single storage entry point."""
    return get_rustfs_s3fs(use_listings_cache=use_listings_cache)


def ensure_object_container(name: str) -> bool:
    """Idempotently create the S3 bucket."""
    return ensure_rustfs_bucket(name)


def presign_object_url(container: str, key: str, expires_seconds: int = 3600) -> str:
    """Browser-usable temporary URL for one S3 object."""
    client = get_rustfs_client(signature_version="s3v4")
    if client is None:
        raise RuntimeError(
            "aws S3 client is not implemented in utility.drivers.aws_s3_driver.get_rustfs_client"
        )
    return client.generate_presigned_url(
        "get_object", Params={"Bucket": container, "Key": key}, ExpiresIn=expires_seconds
    )


class AzureSQLDriver:
    """Placeholder so p0 can import the name on aws; azure paths never run here."""

    def __init__(self, *args, **kwargs):
        raise RuntimeError(
            "AzureSQLDriver is unavailable on aws (DEPLOYMENT_MODE=aws); "
            "the azure timeseries backend only runs when DEPLOYMENT_MODE=azure"
        )
