"""
Document source connectors — Local, S3, ADLS, GCS.

Each connector lists document files from the source, reads them into memory,
and produces a DataFrame of file metadata + raw content for downstream staging.
"""

import logging
import re
from pathlib import Path

import pandas as pd

from connectors.base import BaseConnector
import boto3
import requests

logger = logging.getLogger(__name__)




def _matches(filename: str, extensions: list, include_re, exclude_re) -> bool:
    if exclude_re and exclude_re.search(filename):
        return False
    if include_re and not include_re.search(filename):
        return False
    return any(filename.lower().endswith(ext) for ext in extensions)


def _build_metadata_row(filename: str, doc_type: str, size_bytes: int) -> dict:
    return {
        "filename": filename,
        "document_type": doc_type,
        "file_extension": Path(filename).suffix.lower(),
        "size_bytes": size_bytes,
    }




class LocalDocumentConnector(BaseConnector):
    DATA_TYPE = "documents"

    def validate_config(self):
        src = self.source_cfg
        if not src.get("base_path"):
            raise ValueError(
                "source.base_path is required for local document connector"
            )
        if not src.get("document_types"):
            raise ValueError("source.document_types must define at least one type")

    def connect(self):
        base = Path(self.source_cfg["base_path"])
        if not base.exists():
            raise FileNotFoundError(f"base_path does not exist: {base}")

    def extract(self) -> list:
        base = Path(self.source_cfg["base_path"])
        filters = self.source_cfg.get("filters", {})
        include_re = (
            re.compile(filters["filename_pattern"])
            if filters.get("filename_pattern")
            else None
        )
        exclude_re = (
            re.compile(filters["exclude_pattern"])
            if filters.get("exclude_pattern")
            else None
        )
        max_size = (filters.get("max_file_size_mb") or 9999) * 1024 * 1024

        results = []
        for doc_type, spec in self.source_cfg["document_types"].items():
            type_dir = base / spec["path"]
            if not type_dir.exists():
                logger.warning("Document type dir not found, skipping: %s", type_dir)
                continue

            rows = []
            for fp in sorted(type_dir.rglob("*")):
                if not fp.is_file():
                    continue
                if fp.stat().st_size > max_size:
                    continue
                if not _matches(
                    fp.name, spec.get("extensions", []), include_re, exclude_re
                ):
                    continue
                rows.append(_build_metadata_row(fp.name, doc_type, fp.stat().st_size))

                self.destination.write_file(
                    self.DATA_TYPE,
                    fp.name,
                    fp.read_bytes(),
                    document_type=doc_type,
                )

            if rows:
                results.append(
                    {
                        "df": pd.DataFrame(rows),
                        "filename": f"{doc_type}_manifest.parquet",
                        "document_type": doc_type,
                    }
                )
        return results




class S3DocumentConnector(BaseConnector):
    DATA_TYPE = "documents"

    def validate_config(self):
        src = self.source_cfg
        if not src.get("bucket"):
            raise ValueError("source.bucket is required")
        conn = src.get("connection", {})
        if not conn.get("access_key") or not conn.get("secret_key"):
            raise ValueError("source.connection.access_key and secret_key are required")

    def connect(self):
        conn = self.source_cfg["connection"]
        self._s3 = boto3.client(
            "s3",
            aws_access_key_id=conn["access_key"],
            aws_secret_access_key=conn["secret_key"],
            region_name=conn.get("region", "us-east-1"),
        )
        self._s3.head_bucket(Bucket=self.source_cfg["bucket"])

    def extract(self) -> list:
        bucket = self.source_cfg["bucket"]
        base_prefix = self.source_cfg.get("prefix", "").strip("/")
        filters = self.source_cfg.get("filters", {})
        include_re = (
            re.compile(filters["filename_pattern"])
            if filters.get("filename_pattern")
            else None
        )
        exclude_re = (
            re.compile(filters["exclude_pattern"])
            if filters.get("exclude_pattern")
            else None
        )
        max_size = (filters.get("max_file_size_mb") or 9999) * 1024 * 1024

        results = []
        for doc_type, spec in self.source_cfg["document_types"].items():
            prefix = f"{base_prefix}/{spec['path']}".strip("/") + "/"
            paginator = self._s3.get_paginator("list_objects_v2")
            rows = []

            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    fname = key.rsplit("/", 1)[-1]
                    if not fname:
                        continue
                    if obj["Size"] > max_size:
                        continue
                    if not _matches(
                        fname, spec.get("extensions", []), include_re, exclude_re
                    ):
                        continue

                    rows.append(_build_metadata_row(fname, doc_type, obj["Size"]))
                    body = self._s3.get_object(Bucket=bucket, Key=key)["Body"].read()
                    self.destination.write_file(
                        self.DATA_TYPE,
                        fname,
                        body,
                        document_type=doc_type,
                    )

            if rows:
                results.append(
                    {
                        "df": pd.DataFrame(rows),
                        "filename": f"{doc_type}_manifest.parquet",
                        "document_type": doc_type,
                    }
                )
        return results




class ADLSDocumentConnector(BaseConnector):
    DATA_TYPE = "documents"

    def validate_config(self):
        src = self.source_cfg
        if not src.get("container"):
            raise ValueError("source.container is required")
        conn = src.get("connection", {})
        if not conn.get("account_name"):
            raise ValueError("source.connection.account_name is required")

    def connect(self):
        from azure.identity import ClientSecretCredential
        from azure.storage.filedatalake import DataLakeServiceClient

        conn = self.source_cfg["connection"]
        credential = conn.get("account_key") or conn.get("sas_token")
        if not credential and conn.get("client_id"):
            credential = ClientSecretCredential(
                conn["tenant_id"],
                conn["client_id"],
                conn["client_secret"],
            )
        self._adls = DataLakeServiceClient(
            account_url=f"https://{conn['account_name']}.dfs.core.windows.net",
            credential=credential,
        )
        self._fs = self._adls.get_file_system_client(self.source_cfg["container"])

    def extract(self) -> list:
        base_prefix = self.source_cfg.get("prefix", "").strip("/")
        filters = self.source_cfg.get("filters", {})
        include_re = (
            re.compile(filters["filename_pattern"])
            if filters.get("filename_pattern")
            else None
        )
        exclude_re = (
            re.compile(filters["exclude_pattern"])
            if filters.get("exclude_pattern")
            else None
        )
        max_size = (filters.get("max_file_size_mb") or 9999) * 1024 * 1024

        results = []
        for doc_type, spec in self.source_cfg["document_types"].items():
            prefix = f"{base_prefix}/{spec['path']}".strip("/")
            rows = []
            for path_item in self._fs.get_paths(path=prefix, recursive=True):
                if path_item.is_directory:
                    continue
                fname = path_item.name.rsplit("/", 1)[-1]
                size = path_item.content_length or 0
                if size > max_size:
                    continue
                if not _matches(
                    fname, spec.get("extensions", []), include_re, exclude_re
                ):
                    continue

                rows.append(_build_metadata_row(fname, doc_type, size))
                fc = self._fs.get_file_client(path_item.name)
                data = fc.download_file().readall()
                self.destination.write_file(
                    self.DATA_TYPE,
                    fname,
                    data,
                    document_type=doc_type,
                )
            if rows:
                results.append(
                    {
                        "df": pd.DataFrame(rows),
                        "filename": f"{doc_type}_manifest.parquet",
                        "document_type": doc_type,
                    }
                )
        return results




class GCSDocumentConnector(BaseConnector):
    DATA_TYPE = "documents"

    def validate_config(self):
        src = self.source_cfg
        if not src.get("bucket"):
            raise ValueError("source.bucket is required")

    def connect(self):
        from google.cloud import storage as gcs_storage

        conn = self.source_cfg.get("connection", {})
        if conn.get("service_account_json"):
            self._client = gcs_storage.Client.from_service_account_json(
                conn["service_account_json"]
            )
        else:
            self._client = gcs_storage.Client(project=conn.get("project_id"))
        self._bucket = self._client.bucket(self.source_cfg["bucket"])

    def extract(self) -> list:
        base_prefix = self.source_cfg.get("prefix", "").strip("/")
        filters = self.source_cfg.get("filters", {})
        include_re = (
            re.compile(filters["filename_pattern"])
            if filters.get("filename_pattern")
            else None
        )
        exclude_re = (
            re.compile(filters["exclude_pattern"])
            if filters.get("exclude_pattern")
            else None
        )
        max_size = (filters.get("max_file_size_mb") or 9999) * 1024 * 1024

        results = []
        for doc_type, spec in self.source_cfg["document_types"].items():
            prefix = f"{base_prefix}/{spec['path']}".strip("/") + "/"
            rows = []
            for blob in self._client.list_blobs(self._bucket, prefix=prefix):
                fname = blob.name.rsplit("/", 1)[-1]
                if not fname:
                    continue
                if (blob.size or 0) > max_size:
                    continue
                if not _matches(
                    fname, spec.get("extensions", []), include_re, exclude_re
                ):
                    continue

                rows.append(_build_metadata_row(fname, doc_type, blob.size or 0))
                data = blob.download_as_bytes()
                self.destination.write_file(
                    self.DATA_TYPE,
                    fname,
                    data,
                    document_type=doc_type,
                )
            if rows:
                results.append(
                    {
                        "df": pd.DataFrame(rows),
                        "filename": f"{doc_type}_manifest.parquet",
                        "document_type": doc_type,
                    }
                )
        return results




class OneDriveDocumentConnector(BaseConnector):
    """Documents from OneDrive / SharePoint via Microsoft Graph API."""

    DATA_TYPE = "documents"

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("tenant_id"):
            raise ValueError("connection.tenant_id is required")
        if not conn.get("client_id"):
            raise ValueError("connection.client_id is required")
        if not conn.get("client_secret"):
            raise ValueError("connection.client_secret is required")

    def connect(self):
        conn = self.source_cfg["connection"]
        token_url = (
            f"https://login.microsoftonline.com/{conn['tenant_id']}/oauth2/v2.0/token"
        )
        resp = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": conn["client_id"],
                "client_secret": conn["client_secret"],
                "scope": conn.get("scope", "https://graph.microsoft.com/.default"),
            },
        )
        resp.raise_for_status()
        self._token = resp.json()["access_token"]
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {self._token}"
        self._timeout = conn.get("timeout_seconds", 300)

        site_id = self.source_cfg.get("site_id")
        drive_id = self.source_cfg.get("drive_id")
        if drive_id:
            self._drive_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}"
        elif site_id:
            self._drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive"
        else:
            self._drive_url = "https://graph.microsoft.com/v1.0/me/drive"
        logger.info("OneDrive document connector authenticated")

    def extract(self) -> list:
        folder_path = self.source_cfg.get("folder_path", "").strip("/")
        filters = self.source_cfg.get("filters", {})
        include_re = (
            re.compile(filters["filename_pattern"])
            if filters.get("filename_pattern")
            else None
        )
        exclude_re = (
            re.compile(filters["exclude_pattern"])
            if filters.get("exclude_pattern")
            else None
        )
        max_size = (filters.get("max_file_size_mb") or 9999) * 1024 * 1024

        results = []
        for doc_type, spec in self.source_cfg["document_types"].items():
            sub_path = f"{folder_path}/{spec['path']}".strip("/")
            items = self._list_items(sub_path)
            rows = []
            for item in items:
                fname = item["name"]
                size = item.get("size", 0)
                if size > max_size:
                    continue
                if not _matches(
                    fname, spec.get("extensions", []), include_re, exclude_re
                ):
                    continue
                rows.append(_build_metadata_row(fname, doc_type, size))
                data = self._download(item["@microsoft.graph.downloadUrl"])
                self.destination.write_file(
                    self.DATA_TYPE,
                    fname,
                    data,
                    document_type=doc_type,
                )
            if rows:
                results.append(
                    {
                        "df": pd.DataFrame(rows),
                        "filename": f"{doc_type}_manifest.parquet",
                        "document_type": doc_type,
                    }
                )
        return results

    def _list_items(self, path: str) -> list:
        if path:
            url = f"{self._drive_url}/root:/{path}:/children"
        else:
            url = f"{self._drive_url}/root/children"
        items = []
        while url:
            resp = self._session.get(url, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("value", []):
                if "file" in item:
                    items.append(item)
                elif "folder" in item:
                    child_path = f"{path}/{item['name']}".strip("/")
                    items.extend(self._list_items(child_path))
            url = data.get("@odata.nextLink")
        return items

    def _download(self, download_url: str) -> bytes:
        resp = self._session.get(download_url, timeout=self._timeout)
        resp.raise_for_status()
        return resp.content
