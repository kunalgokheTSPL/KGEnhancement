
import os
import socket
import logging
from urllib.parse import urlparse

import adlfs

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


# ######################### ADLSDriver Azure #######################################

def _dns_resolves(hostname: str) -> bool:
    try:
        socket.gethostbyname(hostname)
        return True
    except Exception:
        return False

def _resolve_rustfs_endpoint() -> str:
    """Resolve the Azure Blob Storage endpoint """
    connection_string = os.getenv("ADLS_CONNECTION_STRING")
    if connection_string:
        return connection_string

    account_name = os.getenv("ADLS_ACCOUNT_NAME") or "ADLS-ACCOUNT-NAME"
    if not account_name:
        raise ValueError(
            "ADLS_ACCOUNT_NAME environment variable is not set "
            "(and no ADLS_CONNECTION_STRING fallback is set either)."
        )

    account_url = f"https://{account_name}.blob.core.windows.net"

    hostname = urlparse(account_url).hostname
    if hostname and not _dns_resolves(hostname):
        logger.warning(
            f"Azure Blob Storage host '{hostname}' does not resolve from this "
            f"machine - check ADLS_ACCOUNT_NAME / network access, or point "
            f"ADLS_CONNECTION_STRING at a local emulator (Azurite) for dev."
        )

    return account_url

def _resolve_rustfs_credentials() -> tuple[str, str]:
    account_name = os.getenv("ADLS_ACCOUNT_NAME", "ADLS-ACCOUNT-NAME")
    account_key = os.getenv("ADLS_ACCOUNT_KEY", "ADLS-ACCOUNT-KEY")
    return account_name, account_key

def get_rustfs_settings() -> tuple[str, str, str]:
    endpoint = _resolve_rustfs_endpoint()
    access_key, secret_key = _resolve_rustfs_credentials()

    return endpoint, access_key, secret_key

def get_rustfs_client(
    signature_version: str | None = "s3v4",
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
):

    if endpoint is None or access_key is None or secret_key is None:
        resolved_endpoint, resolved_access_key, resolved_secret_key = get_rustfs_settings()
        endpoint = endpoint or resolved_endpoint
        access_key = access_key if access_key is not None else resolved_access_key
        secret_key = secret_key if secret_key is not None else resolved_secret_key

    timeout_kwargs = {
        "connection_timeout": int(os.getenv("ADLS_CONNECT_TIMEOUT", "5")),
        "read_timeout": int(os.getenv("ADLS_READ_TIMEOUT", "60")),
    }

    if endpoint and endpoint.strip().lower().startswith(
        ("defaultendpointsprotocol=", "usedevelopmentstorage=")
    ):
        logger.info("Azure Blob Storage: connecting with connection string.")
        return BlobServiceClient.from_connection_string(endpoint, **timeout_kwargs)

    account_url = endpoint or f"https://{access_key}.blob.core.windows.net"

    if secret_key:
        logger.info("Azure Blob Storage: connecting with account key.")
        return BlobServiceClient(account_url=account_url, credential=secret_key, **timeout_kwargs)

    logger.info("Azure Blob Storage: connecting with DefaultAzureCredential (Managed Identity).")
    return BlobServiceClient(
        account_url=account_url, credential=DefaultAzureCredential(), **timeout_kwargs
    )


def get_rustfs_s3fs(use_listings_cache: bool = False):
    endpoint, access_key, secret_key = get_rustfs_settings()
    connection_string = os.getenv("ADLS_CONNECTION_STRING")

    if connection_string:
        return adlfs.AzureBlobFileSystem(
            connection_string=connection_string, use_listings_cache=use_listings_cache
        )
    if secret_key:
        return adlfs.AzureBlobFileSystem(
            account_name=access_key, account_key=secret_key, use_listings_cache=use_listings_cache
        )
    return adlfs.AzureBlobFileSystem(
        account_name=access_key, credential=DefaultAzureCredential(), use_listings_cache=use_listings_cache
    )


def ensure_rustfs_bucket(bucket: str) -> bool:
    client = get_rustfs_client(signature_version=None)
    container_client = client.get_container_client(bucket)
    try:
        container_client.get_container_properties()
        return True
    except ResourceNotFoundError:
        pass
    except Exception as exc:
        logger.error(f"Failed to check container '{bucket}': {exc}")
        return False

    try:
        container_client.create_container()
        return True
    except ResourceExistsError:
        return True
    except Exception as exc:
        logger.error(f"Failed to create container '{bucket}': {exc}")
        return False


# ######################### ADLSDriver #######################################

class ADLSDriver:
    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None
    ):

        
        default_endpoint, default_access_key, default_secret_key = get_rustfs_settings()
        resolved_endpoint = endpoint or default_endpoint
        resolved_access_key = access_key if access_key is not None else default_access_key
        resolved_secret_key = secret_key if secret_key is not None else default_secret_key

        hostname = None
        if resolved_endpoint and "://" in resolved_endpoint:
            hostname = urlparse(resolved_endpoint).hostname

        self.config = {
            "endpoint": resolved_endpoint,
            "access_id": resolved_access_key,
            "secret_key": resolved_secret_key,
            "host": hostname or resolved_access_key,
            "port": 443,
        }
        self.client: BlobServiceClient | None = None

    def connect(self):
        logger.info(f"Connecting to Azure Blob Storage at endpoint={self.config['endpoint']}")
        self.client = get_rustfs_client(
            signature_version=None,
            endpoint=self.config["endpoint"],
            access_key=self.config["access_id"],
            secret_key=self.config["secret_key"],
        )
        return self.client

    def get_s3fs(self, use_listings_cache: bool = False):
        return get_rustfs_s3fs(use_listings_cache=use_listings_cache)

    def ensure_bucket(self, bucket_name: str) -> bool:
        if not self.client:
            self.connect()
        return ensure_rustfs_bucket(bucket_name)

    def create_bucket(self, bucket_name):
        if not self.client:
            self.connect()
        self.client.create_container(bucket_name)

    def upload(self, bucket, local_path, key):
        if not self.client:
            self.connect()
        self.ensure_bucket(bucket)
        blob_client = self.client.get_blob_client(container=bucket, blob=key)
        with open(local_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)

    def upload_folder(self, bucket, local_folder_path, target_prefix):
        IGNORE_DIRS = {"__pycache__", ".pytest_cache"}

        if not os.path.exists(local_folder_path):
            raise ValueError(f"Folder not found: {local_folder_path}")

        for root, dirs, files in os.walk(local_folder_path):
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

            for file in files:
                if file.startswith("."):
                    continue

                local_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(local_file_path, local_folder_path)
                relative_path = relative_path.replace("\\", "/")
                key = f"{target_prefix}/{relative_path}"

                print(f"Uploading: {local_file_path} → {key}")
                self.upload(bucket, local_file_path, key)

        print("Folder upload completed")

    def download(self, bucket, key, local_path):
        if not self.client:
            self.connect()
        blob_client = self.client.get_blob_client(container=bucket, blob=key)
        parent_dir = os.path.dirname(local_path)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(blob_client.download_blob().readall())

    def load(self, bucket, key):
        if not self.client:
            self.connect()
        blob_client = self.client.get_blob_client(container=bucket, blob=key)
        return blob_client.download_blob().readall()

    def list_files(self, bucket, prefix=""):
        if not self.client:
            self.connect()
        container_client = self.client.get_container_client(bucket)
        blobs = container_client.list_blobs(name_starts_with=prefix)
        return [
            {"Key": b.name, "Size": b.size, "LastModified": b.last_modified}
            for b in blobs
        ]

    def get_folder_structure(self, bucket, prefix=""):
        if not self.client:
            self.connect()

        if prefix and not prefix.endswith("/"):
            prefix += "/"

        container_client = self.client.get_container_client(bucket)

        result = {"path": prefix, "folders": [], "files": []}

        for item in container_client.walk_blobs(name_starts_with=prefix, delimiter="/"):
            if hasattr(item, "prefix"):  # BlobPrefix -> "folder"
                folder_path = item.prefix
                folder_name = folder_path.rstrip("/").split("/")[-1]
                result["folders"].append({"name": folder_name, "path": folder_path})
            else:  # BlobProperties -> "file"
                key = item.name
                if key == prefix:
                    continue
                file_name = key.split("/")[-1]
                result["files"].append({"name": file_name, "path": key})

        return result

    def create_folder(self, bucket, folder_path):
        if not self.client:
            self.connect()
        self.ensure_bucket(bucket)
        if not folder_path.endswith("/"):
            folder_path += "/"
        blob_client = self.client.get_blob_client(container=bucket, blob=f"{folder_path}.keep")
        blob_client.upload_blob(b"", overwrite=True)
        print(f"Folder created: {bucket}/{folder_path}")

    def close(self):
        self.client = None

    def delete_file(self, bucket, key):
        if not self.client:
            self.connect()
        self.ensure_bucket(bucket)
        try:
            blob_client = self.client.get_blob_client(container=bucket, blob=key)
            blob_client.delete_blob()
            logger.info(f"Deleted blob: {bucket}/{key}")
        except ResourceNotFoundError:
            pass
        except Exception as exc:
            logger.error(f"Failed to delete blob '{bucket}/{key}': {exc}")
