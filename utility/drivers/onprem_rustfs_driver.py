import os
import logging
from urllib.parse import urlparse

import boto3
import s3fs
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# ######################### OnpremRustfsDriver Helper Functions #######################################

def _resolve_rustfs_endpoint() -> str:
    explicit = os.getenv("RUSTFS_ENDPOINT")
    host = os.getenv("RUSTFS_HOST")
    port = os.getenv("RUSTFS_PORT")
 
    if explicit:
        return explicit.rstrip("/")
 
    if host:
        secure = str(os.getenv("RUSTFS_SECURE", "")).lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
 
        scheme = "https" if secure else "http"
 
        if port:
            return f"{scheme}://{host}:{port}"
 
        return f"{scheme}://{host}"
 
    # Local-development fallback only when no RustFS configuration exists
    return "http://localhost:9000"


def _resolve_rustfs_credentials() -> tuple[str, str]:
    # Resolve credentials purely from environment variables without hardcoded passwords/secrets.
    # If falling back to localhost automatically, prioritize dev override environment variables.
    endpoint = _resolve_rustfs_endpoint()
    parsed = urlparse(endpoint)
    if parsed.hostname in ("localhost", "127.0.0.1"):
        dev_access = os.getenv("RUSTFS_DEV_ACCESS_KEY")
        dev_secret = os.getenv("RUSTFS_DEV_SECRET_KEY")
        if dev_access and dev_secret:
            return dev_access, dev_secret


    access_key = (
        os.getenv("RUSTFS_ACCESS_KEY")
        or os.getenv("RUSTFS_ACCESS_ID")
        or os.getenv("RUSTFS_DEV_ACCESS_KEY")
        or ""
    )
    secret_key = (
        os.getenv("RUSTFS_SECRET_KEY")
        or os.getenv("RUSTFS_DEV_SECRET_KEY")
        or ""
    )
    return access_key, secret_key


def get_rustfs_settings() -> tuple[str, str, str]:
    """Return (endpoint, access_key, secret_key) for RustFS / S3-compatible storage."""
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
        access_key = access_key or resolved_access_key
        secret_key = secret_key or resolved_secret_key
    kwargs = {
        "service_name": "s3",
        "endpoint_url": endpoint,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "region_name": os.getenv("AWS_REGION", "us-east-1"),
    }
    # Always bound the connection so an unreachable / wedged RustFS endpoint can
    # never hang a caller indefinitely (e.g. the best-effort ensure_bucket at API
    # boot). connect_timeout guards the TCP connect; read_timeout stays generous
    # so large uploads aren't cut off. Both env-tunable.
    config_kwargs = {
        "connect_timeout": int(os.getenv("RUSTFS_CONNECT_TIMEOUT", "5")),
        "read_timeout": int(os.getenv("RUSTFS_READ_TIMEOUT", "60")),
        "retries": {"max_attempts": 2, "mode": "standard"},
    }
    if signature_version:
        config_kwargs["signature_version"] = signature_version
    kwargs["config"] = Config(**config_kwargs)
    return boto3.client(**kwargs)


def get_rustfs_s3fs(use_listings_cache: bool = False):
    """Return an s3fs filesystem bound to RustFS."""

    endpoint, access_key, secret_key = get_rustfs_settings()
    return s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": endpoint},
        use_listings_cache=use_listings_cache,
    )


def ensure_rustfs_bucket(bucket: str) -> bool:
    """Idempotently create an S3 bucket if it does not exist."""
    client = get_rustfs_client(signature_version=None)
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code not in ("404", "NoSuchBucket", "NotFound"):
            return False
    try:
        client.create_bucket(Bucket=bucket)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            return True
        return False


# ######################### OnpremRustfsDriver #######################################
class OnpremRustfsDriver:
    def __init__(self, endpoint: str | None = None, access_key: str | None = None, secret_key: str | None = None):
        resolved_endpoint = endpoint or _resolve_rustfs_endpoint()
        default_access_key, default_secret_key = _resolve_rustfs_credentials()
        resolved_access_key = access_key or default_access_key
        resolved_secret_key = secret_key or default_secret_key

        parsed = urlparse(resolved_endpoint)
        self.config = {
            "endpoint": resolved_endpoint,
            "access_id": resolved_access_key,
            "secret_key": resolved_secret_key,
            "host": parsed.hostname or os.getenv("RUSTFS_HOST"),
            "port": parsed.port or int(os.getenv("RUSTFS_PORT", 9000)),
        }
        self.client = None

    def connect(self):
        logger.info(f"Connecting to RustFS at endpoint={self.config['endpoint']}")
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

    def close(self):
        self.client = None

    def create_bucket(self, bucket_name):
        """Create bucket (top-level folder)"""
        if not self.client:
            self.connect()
        self.client.create_bucket(Bucket=bucket_name)

    def upload(self, bucket, local_path, key):
        if not self.client:
            self.connect()
        self.client.upload_file(local_path, bucket, key)

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
        self.client.download_file(bucket, key, local_path)

    def load(self, bucket, key):
        if not self.client:
            self.connect()
        obj = self.client.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read()

    def list_files(self, bucket, prefix=""):
        if not self.client:
            self.connect()
        response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return response.get("Contents", [])

    def get_folder_structure(self, bucket, prefix=""):
        if not self.client:
            self.connect()

        if prefix and not prefix.endswith("/"):
            prefix += "/"

        response = self.client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter="/",
        )

        result = {"path": prefix, "folders": [], "files": []}

        for cp in response.get("CommonPrefixes", []):
            folder_path = cp.get("Prefix")
            folder_name = folder_path.rstrip("/").split("/")[-1]
            result["folders"].append({"name": folder_name, "path": folder_path})

        for obj in response.get("Contents", []):
            key = obj.get("Key")
            if key == prefix:
                continue
            file_name = key.split("/")[-1]
            result["files"].append({"name": file_name, "path": key})

        return result

    def create_folder(self, bucket, folder_path):
        if not self.client:
            self.connect()
        if not folder_path.endswith("/"):
            folder_path += "/"
        self.client.put_object(Bucket=bucket, Key=folder_path)
        print(f"Folder created: {bucket}/{folder_path}")
        
    def delete_file(self, bucket, key):
        if not self.client:
            self.connect()
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
            print(f"Deleted file: {bucket}/{key}")
        except self.client.exceptions.NoSuchKey:
            pass
        except Exception as exc:
            logger.error(f"Failed to delete file '{bucket}/{key}': {exc}")


