"""
Destination writer — decouples connector output from storage backend.

Reads a destination YAML config and provides write_dataframe() / write_file()
methods that place data into the correct staging path regardless of backend.
"""

import os
import io
import re
import logging
from pathlib import Path
from typing import Optional

import boto3
import yaml
import pandas as pd

logger = logging.getLogger(__name__)


def _resolve_env(val: str) -> str:
    """Replace ${VAR:-default} patterns with environment values."""

    def _repl(m):
        var = m.group(1)
        default = m.group(3) if m.group(3) is not None else ""
        return os.environ.get(var, default)

    if not isinstance(val, str):
        return val
    return re.sub(r"\$\{([A-Z_][A-Z0-9_]*)(?:(:-)(.*?))?\}", _repl, val)


def _resolve_config(cfg):
    """Recursively resolve env vars in a config dict."""
    if isinstance(cfg, dict):
        return {k: _resolve_config(v) for k, v in cfg.items()}
    if isinstance(cfg, list):
        return [_resolve_config(v) for v in cfg]
    if isinstance(cfg, str):
        return _resolve_env(cfg)
    return cfg


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base (override wins on conflicts)."""
    merged = base.copy()
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


class DestinationWriter:
    """Write staging data to any supported backend."""

    def __init__(self, config_path: str, destination_type: str = None):
        with open(config_path, "r") as f:
            raw = yaml.safe_load(f)
        raw = _resolve_config(raw)

        if "destinations" in raw and "common" in raw:
            common = raw.get("common", {})
            destinations = raw["destinations"]
            if destination_type and destination_type in destinations:
                dest_cfg = destinations[destination_type]
            elif destination_type:
                raise ValueError(
                    f"Unknown destination type '{destination_type}'. "
                    f"Available: {list(destinations.keys())}"
                )
            else:
                destination_type = next(iter(destinations))
                dest_cfg = destinations[destination_type]
            self.cfg = _deep_merge(common, dest_cfg)
        elif "destination" in raw:
            self.cfg = raw["destination"]
        else:
            raise ValueError("Config must have 'destinations' or 'destination' key")

        self.dest_type = self.cfg["type"]
        self.layout = self.cfg.get("layout", {})
        self.fmt_map = self.cfg.get("format", {})
        self.options = self.cfg.get("options", {})
        self._init_backend()


    def _init_backend(self):
        if self.dest_type == "local":
            self.base_path = Path(self.cfg["base_path"])
            self.base_path.mkdir(parents=True, exist_ok=True)
        elif self.dest_type in ("s3", "rustfs"):
            conn = self.cfg.get("connection", {})
            kwargs = dict(
                aws_access_key_id=conn.get("access_key"),
                aws_secret_access_key=conn.get("secret_key"),
                region_name=conn.get("region", "us-east-1"),
            )
            if conn.get("endpoint_url"):
                kwargs["endpoint_url"] = conn["endpoint_url"]
            kwargs["use_ssl"] = conn.get("use_ssl", True)
            self._s3 = boto3.client("s3", **kwargs)
            self._bucket = self.cfg["bucket"]
            self._prefix = self.cfg.get("base_prefix", "")
        elif self.dest_type == "adls":
            from azure.storage.filedatalake import DataLakeServiceClient

            conn = self.cfg.get("connection", {})
            self._adls = DataLakeServiceClient(
                account_url=f"https://{conn['account_name']}.dfs.core.windows.net",
                credential=conn.get("account_key") or conn.get("sas_token"),
            )
            self._container = self.cfg["container"]
            self._prefix = self.cfg.get("base_prefix", "")
        elif self.dest_type == "gcs":
            from google.cloud import storage as gcs_storage

            conn = self.cfg.get("connection", {})
            if conn.get("service_account_json"):
                self._gcs = gcs_storage.Client.from_service_account_json(
                    conn["service_account_json"]
                )
            else:
                self._gcs = gcs_storage.Client(project=conn.get("project_id"))
            self._bucket_obj = self._gcs.bucket(self.cfg["bucket"])
            self._prefix = self.cfg.get("base_prefix", "")


    def resolve_path(self, data_type: str, **kwargs) -> str:
        """Build the destination path from layout template + kwargs."""
        template = self.layout.get(data_type, data_type)
        _DEFAULTS = {
            "document_type": "general",
            "table_name": "data",
            "file_type": "data",
        }
        path = template.format_map({**_DEFAULTS, **kwargs})
        return path

    def write_dataframe(
        self,
        df: pd.DataFrame,
        data_type: str,
        filename: str,
        **path_kwargs,
    ):
        """Write a DataFrame to staging in the configured format."""
        fmt = self.fmt_map.get(data_type, "parquet")
        sub = self.resolve_path(data_type, **path_kwargs)
        compression = self.options.get("compression", "snappy")

        if fmt == "parquet":
            if not filename.endswith(".parquet"):
                filename = filename.rsplit(".", 1)[0] + ".parquet"
            for col in df.columns:
                if df[col].dtype == "object":
                    df[col] = df[col].astype(str)
            buf = io.BytesIO()
            df.to_parquet(buf, index=False, compression=compression, engine="pyarrow")
            buf.seek(0)
            self._write_bytes(sub, filename, buf.getvalue())
        elif fmt == "csv":
            if not filename.endswith(".csv"):
                filename = filename.rsplit(".", 1)[0] + ".csv"
            data = df.to_csv(index=False).encode("utf-8")
            self._write_bytes(sub, filename, data)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        logger.info(
            "Wrote %d rows → %s/%s (%s)", len(df), sub, filename, self.dest_type
        )

    def write_file(self, data_type: str, filename: str, content: bytes, **path_kwargs):
        """Write raw bytes (e.g. a PDF) to staging."""
        sub = self.resolve_path(data_type, **path_kwargs)
        self._write_bytes(sub, filename, content)
        logger.info("Wrote file → %s/%s (%s)", sub, filename, self.dest_type)


    def _write_bytes(self, sub_path: str, filename: str, data: bytes):
        if self.dest_type == "local":
            dest = self.base_path / sub_path
            dest.mkdir(parents=True, exist_ok=True)
            (dest / filename).write_bytes(data)
        elif self.dest_type in ("s3", "rustfs"):
            key = f"{self._prefix}/{sub_path}/{filename}".strip("/")
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=data)
        elif self.dest_type == "adls":
            fs = self._adls.get_file_system_client(self._container)
            file_path = f"{self._prefix}/{sub_path}/{filename}".strip("/")
            fc = fs.get_file_client(file_path)
            fc.upload_data(data, overwrite=self.options.get("overwrite", True))
        elif self.dest_type == "gcs":
            blob_name = f"{self._prefix}/{sub_path}/{filename}".strip("/")
            blob = self._bucket_obj.blob(blob_name)
            blob.upload_from_string(data)
