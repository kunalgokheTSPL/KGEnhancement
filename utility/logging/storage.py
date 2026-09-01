import logging
import os
from abc import ABC, abstractmethod
from utility.drivers.onprem_rustfs_driver import OnpremRustfsDriver as RustfsDriver
from utility.logging.config import StorageBackendError

logger = logging.getLogger(__name__)


class StorageBackend(ABC):
    """Abstract interface defining required storage operations for log batches."""

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def upload(self, key: str, data: bytes) -> bool:
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        pass

    @abstractmethod
    def list_files(self, prefix: str = "") -> list:
        """List objects under prefix. Each dict must contain at least a 'Key' field."""
        pass

    @abstractmethod
    def read(self, key: str) -> bytes:
        """Read and return raw bytes of an object by its key."""
        pass


class RustFSStorageBackend(StorageBackend):
    """S3-compatible storage backend for uploading log files to RustFS wrapping the global RustfsDriver."""

    def __init__(
        self,
        bucket: str,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ):
        self.bucket = bucket
        self.driver = RustfsDriver(endpoint=endpoint, access_key=access_key, secret_key=secret_key)
        logger.info(
            f"RustFSStorageBackend configured for bucket='{self.bucket}' "
            f"endpoint='{self.driver.config['endpoint']}'"
        )

    def connect(self) -> None:
        """Connects the underlying RustfsDriver client and ensures the bucket exists."""
        try:
            self.driver.connect()
            try:
                self.driver.client.head_bucket(Bucket=self.bucket)
            except Exception:
                logger.info(f"Bucket '{self.bucket}' not found. Creating bucket...")
                self.driver.create_bucket(self.bucket)
        except Exception as e:
            raise StorageBackendError(f"Failed to connect to RustFS S3 endpoint: {e}") from e

    def upload(self, key: str, data: bytes) -> bool:
        if self.driver.client is None:
            self.connect()
        try:
            self.driver.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=data
            )
            logger.info(f"Successfully uploaded object '{key}' to bucket '{self.bucket}'")
            return True
        except Exception as e:
            logger.error(f"Failed to upload object '{key}' to RustFS: {e}")
            raise StorageBackendError(f"Failed to upload object '{key}': {e}") from e

    # FIXED: was 3 spaces (syntax error), now correctly indented at 4 spaces
    def delete(self, key: str) -> bool:
        if self.driver.client is None:
            self.connect()
        try:
            self.driver.client.delete_object(Bucket=self.bucket, Key=key)
            logger.info(f"Successfully deleted object '{key}' from bucket '{self.bucket}'")
            return True
        except Exception as e:
            logger.error(f"Failed to delete object '{key}' from RustFS: {e}")
            raise StorageBackendError(f"Failed to delete object '{key}': {e}") from e

    def exists(self, key: str) -> bool:
        if self.driver.client is None:
            self.connect()
        try:
            self.driver.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception as e:
            from botocore.exceptions import ClientError
            if isinstance(e, ClientError):
                error_code = e.response.get("Error", {}).get("Code")
                if error_code in ("404", "NoSuchKey"):
                    return False
            raise StorageBackendError(f"Exists check failed for object '{key}': {e}") from e

    def list_files(self, prefix: str = "") -> list:
        """List .pb objects under prefix using S3 list_objects_v2.
        Returns list of dicts with 'Key' field — same shape as ADLSStorageBackend.
        """
        if self.driver.client is None:
            self.connect()
        try:
            response = self.driver.client.list_objects_v2(
                Bucket=self.bucket, Prefix=prefix
            )
            return [
                {"Key": obj["Key"], "Size": obj.get("Size", 0)}
                for obj in response.get("Contents", [])
            ]
        except Exception as e:
            logger.error(f"Failed to list objects under prefix '{prefix}': {e}")
            return []

    def read(self, key: str) -> bytes:
        """Read raw bytes of a .pb file from RustFS using S3 get_object."""
        if self.driver.client is None:
            self.connect()
        s3_obj = self.driver.client.get_object(Bucket=self.bucket, Key=key)
        return s3_obj["Body"].read()


class ADLSStorageBackend(StorageBackend):
    """Azure Blob container backend, the azure counterpart of RustFSStorageBackend."""

    def __init__(
        self,
        bucket: str,
        # endpoint/access_key/secret_key accepted but not passed to ADLSDriver.
        # ADLSDriver reads Azure credentials from env vars internally
        # (same pattern as database_driver.py: ADLSDriver() called with no args).
        # These params are accepted only so get_log_storage() can call all
        # backends with the same signature without crashing.
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ):
        # CHANGED: ADLSDriver() called with no args — reads Azure config from env vars.
        # Previously: ADLSDriver(endpoint=endpoint, access_key=access_key, secret_key=secret_key)
        # That was wrong — ADLSDriver uses Azure-specific env vars (connection string etc.),
        # not RustFS-style endpoint/key params. See database_driver.py: ADLSDriver() takes no args.
        from utility.drivers.azure_adls_driver import ADLSDriver

        self.bucket = bucket
        self.driver = ADLSDriver()  # reads Azure credentials from env vars

        logger.info(
            f"ADLSStorageBackend configured for container='{self.bucket}'"
        )

    def connect(self) -> None:
        """Connects the blob client and ensures the container exists."""
        try:
            self.driver.connect()
            self.driver.ensure_bucket(self.bucket)
        except Exception as e:
            raise StorageBackendError(f"Failed to connect to Azure Blob endpoint: {e}") from e

    def _blob(self, key: str):
        if self.driver.client is None:
            self.connect()
        return self.driver.client.get_blob_client(container=self.bucket, blob=key)

    def upload(self, key: str, data: bytes) -> bool:
        try:
            self._blob(key).upload_blob(data, overwrite=True)
            logger.info(f"Successfully uploaded object '{key}' to container '{self.bucket}'")
            return True
        except Exception as e:
            logger.error(f"Failed to upload object '{key}' to ADLS: {e}")
            raise StorageBackendError(f"Failed to upload object '{key}': {e}") from e

    def delete(self, key: str) -> bool:
        try:
            self._blob(key).delete_blob()
            logger.info(f"Successfully deleted object '{key}' from container '{self.bucket}'")
            return True
        except Exception as e:
            logger.error(f"Failed to delete object '{key}' from ADLS: {e}")
            raise StorageBackendError(f"Failed to delete object '{key}': {e}") from e

    def exists(self, key: str) -> bool:
        try:
            return bool(self._blob(key).exists())
        except Exception as e:
            raise StorageBackendError(f"Exists check failed for object '{key}': {e}") from e

    def list_files(self, prefix: str = "") -> list:
        """List blobs under prefix using ADLSDriver.list_files.
        Returns list of dicts with 'Key' field — same shape as RustFSStorageBackend.
        """
        if self.driver.client is None:
            self.connect()
        return self.driver.list_files(self.bucket, prefix)

    def read(self, key: str) -> bytes:
        """Read raw bytes of a .pb file from Azure Blob using ADLSDriver.load."""
        if self.driver.client is None:
            self.connect()
        return self.driver.load(self.bucket, key)


# Log Storage Factory
# Follows same pattern as database_driver.py get_rustfs_connection(DEPLOYMENT_MODE)
# on_prem → RustFSStorageBackend (writes to RustFS / MinIO)
# azure   → ADLSStorageBackend   (writes to Azure Blob container)

def get_onprem_log_storage(bucket, endpoint=None, access_key=None, secret_key=None):
    return RustFSStorageBackend(
        bucket=bucket, endpoint=endpoint, access_key=access_key, secret_key=secret_key
    )


def get_azure_log_storage(bucket, endpoint=None, access_key=None, secret_key=None):
    return ADLSStorageBackend(bucket=bucket)  # ADLSDriver reads Azure creds from env vars


def get_log_storage(DEPLOYMENT_MODE, bucket, endpoint=None, access_key=None, secret_key=None):
    DEPLOYMENT_MODE = (DEPLOYMENT_MODE or "").lower().replace("-", "_")

    if DEPLOYMENT_MODE in ("on_prem", "onprem"):
        return get_onprem_log_storage(bucket, endpoint, access_key, secret_key)

    if DEPLOYMENT_MODE == "azure":
        return get_azure_log_storage(bucket, endpoint, access_key, secret_key)

    raise ValueError(
        f"Unsupported DEPLOYMENT_MODE: '{DEPLOYMENT_MODE}'. "
        "Supported values are: on_prem, azure"
    )