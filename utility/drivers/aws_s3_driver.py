import os
import logging

logger = logging.getLogger(__name__)


# ######################### AwsRustfsDriver Helper Functions #######################################

def _resolve_rustfs_endpoint() -> str:
    return os.getenv("AWS_RUSTFS_ENDPOINT", "")


def _resolve_rustfs_credentials() -> tuple[str, str]:
    return (
        os.getenv("AWS_RUSTFS_ACCESS_KEY", ""),
        os.getenv("AWS_RUSTFS_SECRET_KEY", ""),
    )


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
    """
    Dummy AWS RustFS client.
    Replace with actual implementation when needed.
    """
    return None


def get_rustfs_s3fs(use_listings_cache: bool = False):
    """
    Dummy AWS s3fs filesystem.
    Replace with actual implementation when needed.
    """
    return None


def ensure_rustfs_bucket(bucket: str) -> bool:
    """
    Dummy bucket creation.
    """
    return True


# ######################### AwsRustfsDriver #######################################

class S3Driver:
    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
    ):
        resolved_endpoint = endpoint or _resolve_rustfs_endpoint()
        default_access_key, default_secret_key = _resolve_rustfs_credentials()

        self.config = {
            "endpoint": resolved_endpoint,
            "access_id": access_key or default_access_key,
            "secret_key": secret_key or default_secret_key,
        }

        self.client = None

    def connect(self):
        """
        Dummy connect.
        """
        return self.client

    def get_s3fs(self, use_listings_cache: bool = False):
        return None

    def ensure_bucket(self, bucket_name: str) -> bool:
        return True

    def close(self):
        self.client = None

    def create_bucket(self, bucket_name):
        pass

    def upload(self, bucket, local_path, key):
        pass

    def upload_folder(self, bucket, local_folder_path, target_prefix):
        pass

    def download(self, bucket, key, local_path):
        pass

    def load(self, bucket, key):
        return b""

    def list_files(self, bucket, prefix=""):
        return []

    def get_folder_structure(self, bucket, prefix=""):
        return {
            "path": prefix,
            "folders": [],
            "files": [],
        }

    def create_folder(self, bucket, folder_path):
        pass

    def delete_file(self, bucket, key):
        pass

