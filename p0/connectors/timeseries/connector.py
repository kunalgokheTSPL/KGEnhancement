"""
Timeseries source connectors — Local, S3, ADLS, GCS + Historian systems.

File-based connectors read CSV/Parquet/XLSX timeseries exports.
Historian connectors connect to PI, Wonderware, PHD, Exaquantum, DeltaV, AVEVA.
"""

import io
import logging
import re
import time
from pathlib import Path

import pandas as pd

from connectors.base import BaseConnector
import boto3
import requests

try:
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.identity import ClientSecretCredential
except ImportError:
    DataLakeServiceClient = ClientSecretCredential = None
try:
    from google.cloud import storage as gcs_storage
except ImportError:
    gcs_storage = None
try:
    from requests_kerberos import HTTPKerberosAuth
except ImportError:
    HTTPKerberosAuth = None
try:
    import pyodbc
except ImportError:
    pyodbc = None


logger = logging.getLogger(__name__)

SUPPORTED_EXTS = {".csv", ".parquet", ".xlsx", ".xls"}


def _detect_format(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return {".csv": "csv", ".parquet": "parquet", ".xlsx": "xlsx", ".xls": "xlsx"}.get(
        ext, "csv"
    )


def _read_ts_bytes(data: bytes, fmt: str, encoding: str = "utf-8") -> pd.DataFrame:
    if fmt == "parquet":
        return pd.read_parquet(io.BytesIO(data))
    elif fmt in ("xlsx", "xls"):
        return pd.read_excel(io.BytesIO(data))
    else:
        return pd.read_csv(io.BytesIO(data), encoding=encoding)


def _melt_wide_format(df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
    """Convert wide-format (one column per tag) to long-format."""
    if ts_col not in df.columns:
        return df
    id_vars = [ts_col]
    value_vars = [c for c in df.columns if c != ts_col]
    melted = df.melt(id_vars=id_vars, var_name="tag_name", value_name="value")
    return melted




class LocalTimeseriesConnector(BaseConnector):
    DATA_TYPE = "timeseries"

    def validate_config(self):
        if not self.source_cfg.get("base_path"):
            raise ValueError("source.base_path is required")

    def connect(self):
        base = Path(self.source_cfg["base_path"])
        if not base.exists():
            raise FileNotFoundError(f"base_path does not exist: {base}")

    def extract(self):
        base = Path(self.source_cfg["base_path"])
        exts = set(self.source_cfg.get("extensions", [".csv", ".parquet"]))
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")
        schema = self.source_cfg.get("schema", {})
        wide = schema.get("wide_format", False)
        ts_col = schema.get("timestamp_column", "timestamp")
        filters = self.source_cfg.get("filters", {})
        max_size = (filters.get("max_file_size_mb") or 9999) * 1024 * 1024
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
        file_type = self.source_cfg.get("file_type", "data")

        results = []
        for fp in sorted(base.rglob("*")):
            if not fp.is_file() or fp.suffix.lower() not in exts:
                continue
            if fp.stat().st_size > max_size:
                continue
            if exclude_re and exclude_re.search(fp.name):
                continue
            if include_re and not include_re.search(fp.name):
                continue
            fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fp.name)
            try:
                df = _read_ts_bytes(fp.read_bytes(), fmt, encoding)
            except Exception as exc:
                logger.warning("Skipping unreadable file %s: %s", fp.name, exc)
                continue
            if wide:
                df = _melt_wide_format(df, ts_col)
            out_filename = fp.stem + ".parquet"
            results.append({"df": df, "filename": out_filename, "file_type": file_type})
            logger.info(
                "Read %d rows from %s → %s/%s",
                len(df),
                fp.name,
                file_type,
                out_filename,
            )

        return results


class S3TimeseriesConnector(BaseConnector):
    DATA_TYPE = "timeseries"

    def validate_config(self):
        if not self.source_cfg.get("bucket"):
            raise ValueError("source.bucket is required")

    def connect(self):

        conn = self.source_cfg["connection"]
        self._s3 = boto3.client(
            "s3",
            aws_access_key_id=conn["access_key"],
            aws_secret_access_key=conn["secret_key"],
            region_name=conn.get("region", "us-east-1"),
        )
        self._s3.head_bucket(Bucket=self.source_cfg["bucket"])

    def extract(self):
        bucket = self.source_cfg["bucket"]
        prefix = (self.source_cfg.get("prefix", "") + "/").lstrip("/")
        exts = set(self.source_cfg.get("extensions", [".csv", ".parquet"]))
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")
        schema = self.source_cfg.get("schema", {})
        wide = schema.get("wide_format", False)
        ts_col = schema.get("timestamp_column", "timestamp")

        frames = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                fname = obj["Key"].rsplit("/", 1)[-1]
                if not fname or Path(fname).suffix.lower() not in exts:
                    continue
                body = self._s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fname)
                df = _read_ts_bytes(body, fmt, encoding)
                if wide:
                    df = _melt_wide_format(df, ts_col)
                frames.append(df)

        if not frames:
            return []
        combined = pd.concat(frames, ignore_index=True)
        return [{"df": combined, "filename": "timeseries_data.parquet"}]


class ADLSTimeseriesConnector(BaseConnector):
    DATA_TYPE = "timeseries"

    def validate_config(self):
        if not self.source_cfg.get("container"):
            raise ValueError("source.container is required")

    def connect(self):

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

    def extract(self):
        prefix = self.source_cfg.get("prefix", "").strip("/")
        exts = set(self.source_cfg.get("extensions", [".csv", ".parquet"]))
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")
        schema = self.source_cfg.get("schema", {})
        wide = schema.get("wide_format", False)
        ts_col = schema.get("timestamp_column", "timestamp")

        frames = []
        for p in self._fs.get_paths(path=prefix, recursive=True):
            if p.is_directory:
                continue
            fname = p.name.rsplit("/", 1)[-1]
            if Path(fname).suffix.lower() not in exts:
                continue
            fc = self._fs.get_file_client(p.name)
            body = fc.download_file().readall()
            fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fname)
            df = _read_ts_bytes(body, fmt, encoding)
            if wide:
                df = _melt_wide_format(df, ts_col)
            frames.append(df)

        if not frames:
            return []
        combined = pd.concat(frames, ignore_index=True)
        return [{"df": combined, "filename": "timeseries_data.parquet"}]


class GCSTimeseriesConnector(BaseConnector):
    DATA_TYPE = "timeseries"

    def validate_config(self):
        if not self.source_cfg.get("bucket"):
            raise ValueError("source.bucket is required")

    def connect(self):

        conn = self.source_cfg.get("connection", {})
        if conn.get("service_account_json"):
            self._client = gcs_storage.Client.from_service_account_json(
                conn["service_account_json"]
            )
        else:
            self._client = gcs_storage.Client(project=conn.get("project_id"))
        self._bucket = self._client.bucket(self.source_cfg["bucket"])

    def extract(self):
        prefix = (self.source_cfg.get("prefix", "") + "/").lstrip("/")
        exts = set(self.source_cfg.get("extensions", [".csv", ".parquet"]))
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")
        schema = self.source_cfg.get("schema", {})
        wide = schema.get("wide_format", False)
        ts_col = schema.get("timestamp_column", "timestamp")

        frames = []
        for blob in self._client.list_blobs(self._bucket, prefix=prefix):
            fname = blob.name.rsplit("/", 1)[-1]
            if not fname or Path(fname).suffix.lower() not in exts:
                continue
            body = blob.download_as_bytes()
            fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fname)
            df = _read_ts_bytes(body, fmt, encoding)
            if wide:
                df = _melt_wide_format(df, ts_col)
            frames.append(df)

        if not frames:
            return []
        combined = pd.concat(frames, ignore_index=True)
        return [{"df": combined, "filename": "timeseries_data.parquet"}]




class OneDriveTimeseriesConnector(BaseConnector):
    """Timeseries exports from OneDrive / SharePoint via Microsoft Graph API."""

    DATA_TYPE = "timeseries"

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
        logger.info("OneDrive timeseries connector authenticated")

    def extract(self):
        folder_path = self.source_cfg.get("folder_path", "").strip("/")
        exts = set(self.source_cfg.get("extensions", [".csv", ".parquet"]))
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")
        schema = self.source_cfg.get("schema", {})
        wide = schema.get("wide_format", False)
        ts_col = schema.get("timestamp_column", "timestamp")

        items = self._list_items(folder_path)
        frames = []
        for item in items:
            fname = item["name"]
            if Path(fname).suffix.lower() not in exts:
                continue
            data = self._download(item["@microsoft.graph.downloadUrl"])
            fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fname)
            df = _read_ts_bytes(data, fmt, encoding)
            if wide:
                df = _melt_wide_format(df, ts_col)
            frames.append(df)
            logger.info("Read %d rows from %s (OneDrive)", len(df), fname)

        if not frames:
            return []
        combined = pd.concat(frames, ignore_index=True)
        return [{"df": combined, "filename": "timeseries_data.parquet"}]

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




class OsisoftPIConnector(BaseConnector):
    """OSIsoft PI via PI Web API (REST)."""

    DATA_TYPE = "timeseries"

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("base_url"):
            raise ValueError("connection.base_url is required")

    def connect(self):

        conn = self.source_cfg["connection"]
        self._base = conn["base_url"].rstrip("/")
        self._session = requests.Session()
        self._session.verify = conn.get("verify_ssl", False)
        self._timeout = conn.get("timeout_seconds", 300)

        if conn.get("auth_type") == "kerberos":
            self._session.auth = HTTPKerberosAuth()
        elif conn.get("auth_type") == "bearer":
            self._session.headers["Authorization"] = f"Bearer {conn['bearer_token']}"
        else:
            self._session.auth = (conn["username"], conn["password"])

        resp = self._session.get(f"{self._base}/system", timeout=self._timeout)
        resp.raise_for_status()
        logger.info("PI Web API connected: %s", resp.json().get("ProductTitle", "OK"))

    def extract(self):
        ext = self.source_cfg.get("extraction", {})
        tag_list = ext.get("tag_list") or []
        tag_filter = ext.get("tag_filter", "*")
        start = ext.get("start_time", "*-30d")
        end = ext.get("end_time", "*")
        mode = ext.get("mode", "recorded")
        interval = ext.get("interval")
        include_meta = self.source_cfg.get("include_metadata", True)
        batch_cfg = self.source_cfg.get("batch", {})
        chunk_size = batch_cfg.get("chunk_size", 1000)
        retry_count = batch_cfg.get("retry_count", 3)
        retry_delay = batch_cfg.get("retry_delay_seconds", 10)

        da = ext.get("pi_data_archive")

        if not tag_list:
            search_url = f"{self._base}/search/query"
            params = {"q": f"name:{tag_filter}", "count": 10000}
            if da:
                params["scope"] = f"pi:{da}"
            resp = self._session.get(search_url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            items = resp.json().get("Items", [])
            tag_list = [it["Name"] for it in items if "Name" in it]
            logger.info(
                "Discovered %d PI points matching '%s'", len(tag_list), tag_filter
            )

        if not tag_list:
            logger.warning("No PI tags found")
            return []

        all_rows = []
        for i in range(0, len(tag_list), chunk_size):
            chunk = tag_list[i : i + chunk_size]
            for tag in chunk:
                for attempt in range(1, retry_count + 1):
                    try:
                        if mode == "interpolated" and interval:
                            url = f"{self._base}/streams/{tag}/interpolated"
                            params = {
                                "startTime": start,
                                "endTime": end,
                                "interval": interval,
                            }
                        else:
                            url = f"{self._base}/streams/{tag}/recorded"
                            params = {"startTime": start, "endTime": end}
                        resp = self._session.get(
                            url, params=params, timeout=self._timeout
                        )
                        resp.raise_for_status()
                        items = resp.json().get("Items", [])
                        for item in items:
                            all_rows.append(
                                {
                                    "tag_name": tag,
                                    "timestamp": item.get("Timestamp"),
                                    "value": item.get("Value"),
                                    "quality": item.get("Good", True),
                                }
                            )
                        break
                    except Exception as e:
                        logger.warning("PI tag %s attempt %d: %s", tag, attempt, e)
                        if attempt == retry_count:
                            logger.error(
                                "Failed to retrieve %s after %d attempts",
                                tag,
                                retry_count,
                            )
                        else:
                            time.sleep(retry_delay)

        results = []
        if all_rows:
            df = pd.DataFrame(all_rows)
            results.append({"df": df, "filename": "timeseries_data.parquet"})

        if include_meta and tag_list:
            meta_rows = []
            for tag in tag_list:
                try:
                    resp = self._session.get(
                        f"{self._base}/points/{tag}",
                        timeout=self._timeout,
                    )
                    if resp.ok:
                        info = resp.json()
                        meta_rows.append(
                            {
                                "tag_name": tag,
                                "description": info.get("Descriptor", ""),
                                "engineering_units": info.get("EngineeringUnits", ""),
                                "point_type": info.get("PointType", ""),
                            }
                        )
                except Exception:
                    pass
            if meta_rows:
                results.append(
                    {
                        "df": pd.DataFrame(meta_rows),
                        "filename": "timeseries_metadata.parquet",
                    }
                )
        return results


class _SQLHistorianBase(BaseConnector):
    """Shared logic for SQL-based historians (Wonderware, Exaquantum, DeltaV, AVEVA)."""

    DATA_TYPE = "timeseries"

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("host"):
            raise ValueError("connection.host is required")

    def _build_connection_string(self):
        conn = self.source_cfg["connection"]
        driver = conn.get("driver", "ODBC Driver 18 for SQL Server")
        parts = [
            f"DRIVER={{{driver}}}",
            f"SERVER={conn['host']},{conn.get('port', 1433)}",
            f"DATABASE={conn.get('database', 'Runtime')}",
        ]
        if conn.get("trusted_connection"):
            parts.append("Trusted_Connection=yes")
        else:
            parts.append(f"UID={conn.get('username', '')}")
            parts.append(f"PWD={conn.get('password', '')}")
        if conn.get("encrypt"):
            parts.append("Encrypt=yes")
        if conn.get("trust_server_certificate"):
            parts.append("TrustServerCertificate=yes")
        return ";".join(parts)

    def connect(self):

        conn_str = self._build_connection_string()
        self._conn = pyodbc.connect(conn_str)
        logger.info(
            "SQL historian connection established to %s",
            self.source_cfg["connection"]["host"],
        )

    def _fetch_tags(self, tag_table, tag_filter, tag_list) -> list:
        cursor = self._conn.cursor()
        if tag_list:
            placeholders = ",".join(["?" for _ in tag_list])
            cursor.execute(
                f"SELECT * FROM {tag_table} WHERE TagName IN ({placeholders})", tag_list
            )
        else:
            cursor.execute(
                f"SELECT * FROM {tag_table} WHERE TagName LIKE ?", [tag_filter]
            )
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        return [dict(zip(columns, row)) for row in rows]

    def _fetch_history(self, tags, start_time, end_time, **kwargs) -> pd.DataFrame:
        """Override in subclass for historian-specific history queries."""
        raise NotImplementedError

    def extract(self):
        ext = self.source_cfg.get("extraction", {})
        tag_table = ext.get("tag_table", "Tag")
        tag_filter = ext.get("tag_filter", "%")
        tag_list = ext.get("tag_list")
        start_time = ext.get("start_time")
        end_time = ext.get("end_time")
        include_meta = self.source_cfg.get("include_metadata", True)

        tag_info = self._fetch_tags(tag_table, tag_filter, tag_list)
        tag_names = [
            t.get("TagName") or t.get("ItemName") or t.get("ParamName", "")
            for t in tag_info
        ]
        logger.info("Found %d tags", len(tag_names))

        results = []

        if tag_names:
            df = self._fetch_history(tag_names, start_time, end_time, **ext)
            if not df.empty:
                results.append({"df": df, "filename": "timeseries_data.parquet"})

        if include_meta and tag_info:
            meta_df = pd.DataFrame(tag_info)
            results.append({"df": meta_df, "filename": "timeseries_metadata.parquet"})

        return results


class WonderwareConnector(_SQLHistorianBase):
    """Wonderware / AVEVA InTouch Historian."""

    def _fetch_history(self, tags, start_time, end_time, **kwargs):
        history_table = kwargs.get("history_table", "History")
        mode = kwargs.get("retrieval_mode", "Cyclic")
        resolution = kwargs.get("resolution_ms", 60000)

        tag_csv = ",".join([f"'{t}'" for t in tags])
        sql = f"""
            SELECT TagName, DateTime, Value, Quality
            FROM {history_table}
            WHERE TagName IN ({tag_csv})
              AND wwRetrievalMode = '{mode}'
              AND wwResolution = {resolution}
        """
        if start_time:
            sql += f" AND DateTime >= '{start_time}'"
        if end_time:
            sql += f" AND DateTime <= '{end_time}'"

        return pd.read_sql(sql, self._conn)


class HoneywellPHDConnector(BaseConnector):
    """Honeywell PHD / Uniformance Historian."""

    DATA_TYPE = "timeseries"

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("host") and not conn.get("api_url"):
            raise ValueError("connection.host or connection.api_url is required")

    def connect(self):
        conn = self.source_cfg["connection"]
        if conn.get("api_url"):
            self._mode = "api"
            self._session = requests.Session()
            self._session.auth = (conn.get("username", ""), conn.get("password", ""))
            self._session.verify = conn.get("use_ssl", False)
            self._base = conn["api_url"].rstrip("/")
            resp = self._session.get(f"{self._base}/api/v1/status")
            resp.raise_for_status()
        else:
            self._mode = "odbc"
            conn_str = (
                f"DRIVER={{Honeywell PHD Driver}};"
                f"SERVER={conn['host']};"
                f"PORT={conn.get('port', 3100)};"
                f"UID={conn.get('username', '')};"
                f"PWD={conn.get('password', '')}"
            )
            self._conn = pyodbc.connect(conn_str)
        logger.info("PHD connection established (%s)", self._mode)

    def extract(self):
        ext = self.source_cfg.get("extraction", {})
        tag_list = ext.get("tag_list") or []
        tag_filter = ext.get("tag_filter", "*")
        start_time = ext.get("start_time")
        end_time = ext.get("end_time")
        sample_type = ext.get("sample_type", "Snapshot")
        frequency = ext.get("sample_frequency_seconds", 60)
        include_meta = self.source_cfg.get("include_metadata", True)
        batch_cfg = self.source_cfg.get("batch", {})
        chunk_size = batch_cfg.get("chunk_size", 200)

        results = []
        if self._mode == "api":
            if not tag_list:
                resp = self._session.get(
                    f"{self._base}/api/v1/tags",
                    params={"filter": tag_filter, "limit": 10000},
                )
                resp.raise_for_status()
                tag_list = [t["TagName"] for t in resp.json().get("tags", [])]

            all_rows = []
            for i in range(0, len(tag_list), chunk_size):
                chunk = tag_list[i : i + chunk_size]
                body = {
                    "tags": chunk,
                    "startTime": start_time,
                    "endTime": end_time,
                    "sampleType": sample_type,
                    "sampleFrequency": frequency,
                }
                resp = self._session.post(f"{self._base}/api/v1/history", json=body)
                if resp.ok:
                    for rec in resp.json().get("data", []):
                        all_rows.append(rec)

            if all_rows:
                results.append(
                    {
                        "df": pd.DataFrame(all_rows),
                        "filename": "timeseries_data.parquet",
                    }
                )
        else:
            cursor = self._conn.cursor()
            if not tag_list:
                cursor.execute(
                    "SELECT TagName FROM PHD_Tags WHERE TagName LIKE ?",
                    [tag_filter.replace("*", "%")],
                )
                tag_list = [row[0] for row in cursor.fetchall()]

            tag_csv = ",".join([f"'{t}'" for t in tag_list])
            sql = f"SELECT TagName, Timestamp, Value FROM PHD_History WHERE TagName IN ({tag_csv})"
            if start_time:
                sql += f" AND Timestamp >= '{start_time}'"
            if end_time:
                sql += f" AND Timestamp <= '{end_time}'"
            df = pd.read_sql(sql, self._conn)
            if not df.empty:
                results.append({"df": df, "filename": "timeseries_data.parquet"})

        return results


class YokogawaExaquantumConnector(_SQLHistorianBase):
    """Yokogawa Exaquantum (SQL Server backend)."""

    def _fetch_history(self, tags, start_time, end_time, **kwargs):
        history_fn = kwargs.get("history_function", "ExaqHistoryAG")
        interval = kwargs.get("sample_interval_seconds", 60)

        tag_csv = ",".join([f"'{t}'" for t in tags])
        sql = f"""
            SELECT ItemName, DateTime, Value, Quality
            FROM {history_fn}
            WHERE ItemName IN ({tag_csv})
              AND IntervalSeconds = {interval}
        """
        if start_time:
            sql += f" AND DateTime >= '{start_time}'"
        if end_time:
            sql += f" AND DateTime <= '{end_time}'"

        return pd.read_sql(sql, self._conn)


class EmersonDeltaVConnector(_SQLHistorianBase):
    """Emerson DeltaV Continuous Historian."""

    def _fetch_history(self, tags, start_time, end_time, **kwargs):
        history_table = kwargs.get("history_table", "History")
        interval = kwargs.get("sample_interval_seconds", 60)

        tag_csv = ",".join([f"'{t}'" for t in tags])
        sql = f"""
            SELECT ParamName AS TagName, TimeStamp, Value, Status
            FROM {history_table}
            WHERE ParamName IN ({tag_csv})
        """
        if start_time:
            sql += f" AND TimeStamp >= '{start_time}'"
        if end_time:
            sql += f" AND TimeStamp <= '{end_time}'"

        return pd.read_sql(sql, self._conn)


class AVEVAHistorianConnector(BaseConnector):
    """AVEVA Historian (REST API or SQL)."""

    DATA_TYPE = "timeseries"

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("api_url") and not conn.get("host"):
            raise ValueError("connection.api_url or connection.host is required")

    def connect(self):
        conn = self.source_cfg["connection"]
        ext = self.source_cfg.get("extraction", {})
        self._method = ext.get("method", "api")

        if self._method == "api":
            self._session = requests.Session()
            self._session.verify = conn.get("verify_ssl", True)
            if conn.get("auth_type") == "bearer":
                self._session.headers["Authorization"] = (
                    f"Bearer {conn['bearer_token']}"
                )
            else:
                self._session.auth = (
                    conn.get("username", ""),
                    conn.get("password", ""),
                )
            self._base = conn["api_url"].rstrip("/")
        else:
            driver = conn.get("driver", "ODBC Driver 18 for SQL Server")
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={conn['host']},{conn.get('port', 1433)};"
                f"DATABASE={conn.get('database', 'Runtime')};"
                f"UID={conn.get('username', '')};"
                f"PWD={conn.get('password', '')};"
                "Encrypt=yes;TrustServerCertificate=yes"
            )
            self._conn = pyodbc.connect(conn_str)
        logger.info("AVEVA Historian connected (%s)", self._method)

    def extract(self):
        ext = self.source_cfg.get("extraction", {})
        tag_filter = ext.get("tag_filter", "*")
        tag_list = ext.get("tag_list")
        start_time = ext.get("start_time")
        end_time = ext.get("end_time")
        include_meta = self.source_cfg.get("include_metadata", True)
        batch_cfg = self.source_cfg.get("batch", {})
        chunk_size = batch_cfg.get("chunk_size", 500)

        results = []

        if self._method == "api":
            if not tag_list:
                resp = self._session.get(
                    f"{self._base}/tags",
                    params={"filter": tag_filter, "limit": 10000},
                )
                resp.raise_for_status()
                tag_list = [t["TagName"] for t in resp.json().get("tags", [])]

            all_rows = []
            for i in range(0, len(tag_list or []), chunk_size):
                chunk = tag_list[i : i + chunk_size]
                body = {
                    "tags": chunk,
                    "startTime": start_time,
                    "endTime": end_time,
                    "retrievalMode": ext.get("retrieval_mode", "Cyclic"),
                    "resolution": ext.get("resolution_ms", 60000),
                }
                resp = self._session.post(f"{self._base}/history", json=body)
                if resp.ok:
                    all_rows.extend(resp.json().get("data", []))

            if all_rows:
                results.append(
                    {
                        "df": pd.DataFrame(all_rows),
                        "filename": "timeseries_data.parquet",
                    }
                )
        else:
            cursor = self._conn.cursor()
            if not tag_list:
                cursor.execute(
                    "SELECT TagName FROM Tag WHERE TagName LIKE ?",
                    [tag_filter.replace("*", "%")],
                )
                tag_list = [r[0] for r in cursor.fetchall()]

            tag_csv = ",".join([f"'{t}'" for t in tag_list])
            sql = f"SELECT TagName, DateTime, Value, Quality FROM History WHERE TagName IN ({tag_csv})"
            if start_time:
                sql += f" AND DateTime >= '{start_time}'"
            if end_time:
                sql += f" AND DateTime <= '{end_time}'"
            df = pd.read_sql(sql, self._conn)
            if not df.empty:
                results.append({"df": df, "filename": "timeseries_data.parquet"})

        return results
