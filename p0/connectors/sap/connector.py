"""
SAP source connectors — Local, S3, ADLS, GCS + SAP system connectors.

File-based connectors (local/cloud) read exported CSV/Parquet/XLSX table files.
SAP system connectors connect directly via RFC, SQL, or OData.
"""

import io
import logging
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
    import pyrfc
except ImportError:
    pyrfc = None
try:
    from hdbcli import dbapi
except ImportError:
    dbapi = None

logger = logging.getLogger(__name__)

SUPPORTED_EXTS = {".csv", ".parquet", ".xlsx", ".xls"}


def _detect_format(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    return {".csv": "csv", ".parquet": "parquet", ".xlsx": "xlsx", ".xls": "xlsx"}.get(
        ext, "csv"
    )


def _read_table_bytes(data: bytes, fmt: str, encoding: str = "utf-8") -> pd.DataFrame:
    if fmt == "parquet":
        return pd.read_parquet(io.BytesIO(data))
    elif fmt in ("xlsx", "xls"):
        return pd.read_excel(io.BytesIO(data))
    else:
        return pd.read_csv(io.BytesIO(data), encoding=encoding)


def _table_name_from_file(filename: str) -> str:
    return Path(filename).stem.upper()




class LocalSapConnector(BaseConnector):
    DATA_TYPE = "sap"

    def validate_config(self):
        if not self.source_cfg.get("base_path"):
            raise ValueError("source.base_path is required")

    def connect(self):
        base = Path(self.source_cfg["base_path"])
        if not base.exists():
            raise FileNotFoundError(f"base_path does not exist: {base}")

    def extract(self):
        base = Path(self.source_cfg["base_path"])
        allowed = self.source_cfg.get("tables")
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")
        max_size = (
            (self.source_cfg.get("filters", {}).get("max_file_size_mb") or 9999)
            * 1024
            * 1024
        )

        results = []
        for fp in sorted(base.rglob("*")):
            if not fp.is_file() or fp.suffix.lower() not in SUPPORTED_EXTS:
                continue
            if fp.stat().st_size > max_size:
                continue
            table = _table_name_from_file(fp.name)
            if allowed and table not in [t.upper() for t in allowed]:
                continue
            fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fp.name)
            df = _read_table_bytes(fp.read_bytes(), fmt, encoding)
            logger.info("Read %d rows from %s (%s)", len(df), table, fp.name)
            results.append(
                {"df": df, "filename": f"{table}.parquet", "table_name": table}
            )
        return results


class S3SapConnector(BaseConnector):
    DATA_TYPE = "sap"

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
        allowed = self.source_cfg.get("tables")
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")

        results = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                fname = obj["Key"].rsplit("/", 1)[-1]
                if not fname or Path(fname).suffix.lower() not in SUPPORTED_EXTS:
                    continue
                table = _table_name_from_file(fname)
                if allowed and table not in [t.upper() for t in allowed]:
                    continue
                body = self._s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
                fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fname)
                df = _read_table_bytes(body, fmt, encoding)
                logger.info(
                    "Read %d rows from %s (s3://%s/%s)",
                    len(df),
                    table,
                    bucket,
                    obj["Key"],
                )
                results.append(
                    {"df": df, "filename": f"{table}.parquet", "table_name": table}
                )
        return results


class ADLSSapConnector(BaseConnector):
    DATA_TYPE = "sap"

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
        allowed = self.source_cfg.get("tables")
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")

        results = []
        for p in self._fs.get_paths(path=prefix, recursive=True):
            if p.is_directory:
                continue
            fname = p.name.rsplit("/", 1)[-1]
            if Path(fname).suffix.lower() not in SUPPORTED_EXTS:
                continue
            table = _table_name_from_file(fname)
            if allowed and table not in [t.upper() for t in allowed]:
                continue
            fc = self._fs.get_file_client(p.name)
            body = fc.download_file().readall()
            fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fname)
            df = _read_table_bytes(body, fmt, encoding)
            results.append(
                {"df": df, "filename": f"{table}.parquet", "table_name": table}
            )
        return results


class GCSSapConnector(BaseConnector):
    DATA_TYPE = "sap"

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
        allowed = self.source_cfg.get("tables")
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")

        results = []
        for blob in self._client.list_blobs(self._bucket, prefix=prefix):
            fname = blob.name.rsplit("/", 1)[-1]
            if not fname or Path(fname).suffix.lower() not in SUPPORTED_EXTS:
                continue
            table = _table_name_from_file(fname)
            if allowed and table not in [t.upper() for t in allowed]:
                continue
            body = blob.download_as_bytes()
            fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fname)
            df = _read_table_bytes(body, fmt, encoding)
            results.append(
                {"df": df, "filename": f"{table}.parquet", "table_name": table}
            )
        return results




class OneDriveSapConnector(BaseConnector):
    """SAP table exports from OneDrive / SharePoint via Microsoft Graph API."""

    DATA_TYPE = "sap"

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
        logger.info("OneDrive SAP connector authenticated")

    def extract(self):
        folder_path = self.source_cfg.get("folder_path", "").strip("/")
        allowed = self.source_cfg.get("tables")
        fmt_hint = self.source_cfg.get("file_format", "auto")
        encoding = self.source_cfg.get("encoding", "utf-8")

        items = self._list_items(folder_path)
        results = []
        for item in items:
            fname = item["name"]
            if Path(fname).suffix.lower() not in SUPPORTED_EXTS:
                continue
            table = _table_name_from_file(fname)
            if allowed and table not in [t.upper() for t in allowed]:
                continue
            data = self._download(item["@microsoft.graph.downloadUrl"])
            fmt = fmt_hint if fmt_hint != "auto" else _detect_format(fname)
            df = _read_table_bytes(data, fmt, encoding)
            logger.info("Read %d rows from %s (OneDrive)", len(df), table)
            results.append(
                {"df": df, "filename": f"{table}.parquet", "table_name": table}
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




class _SapRFCBase(BaseConnector):
    """Shared logic for RFC-based SAP connectors (ERP 6.0 / S/4HANA On-Prem)."""

    DATA_TYPE = "sap"

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("ashost") and not conn.get("mshost"):
            raise ValueError("connection.ashost or connection.mshost is required")
        if not conn.get("user"):
            raise ValueError("connection.user is required")
        if not conn.get("passwd"):
            raise ValueError("connection.passwd is required")

    def connect(self):
        conn = self.source_cfg["connection"]
        params = {
            k: v
            for k, v in conn.items()
            if v
            and k
            in (
                "ashost",
                "sysnr",
                "client",
                "user",
                "passwd",
                "lang",
                "saprouter",
                "mshost",
                "msserv",
                "group",
            )
        }
        self._conn = pyrfc.Connection(**params)
        logger.info(
            "RFC connection established to %s", conn.get("ashost") or conn.get("mshost")
        )

    def extract(self):
        tables = self.source_cfg.get("tables", [])
        ext = self.source_cfg.get("extraction", {})
        fm = ext.get("function_module", "RFC_READ_TABLE")
        pkg_size = ext.get("package_size", 50000)
        delimiter = ext.get("delimiter", "|")
        batch_cfg = self.source_cfg.get("batch", {})
        retry_count = batch_cfg.get("retry_count", 3)
        retry_delay = batch_cfg.get("retry_delay_seconds", 30)

        results = []
        for tbl in tables:
            name = tbl["name"]
            fields = tbl.get("fields", [])
            where = tbl.get("where", "")
            logger.info("Extracting SAP table %s via %s ...", name, fm)

            for attempt in range(1, retry_count + 1):
                try:
                    all_rows = []
                    offset = 0
                    while True:
                        params = {
                            "QUERY_TABLE": name,
                            "DELIMITER": delimiter,
                            "ROWSKIPS": offset,
                            "ROWCOUNT": pkg_size,
                        }
                        if fields:
                            params["FIELDS"] = [{"FIELDNAME": f} for f in fields]
                        if where:
                            params["OPTIONS"] = [{"TEXT": where}]
                        result = self._conn.call(fm, **params)
                        data_rows = result.get("DATA", [])
                        field_info = result.get("FIELDS", [])
                        if not data_rows:
                            break
                        col_names = [f["FIELDNAME"] for f in field_info]
                        for row in data_rows:
                            vals = row["WA"].split(delimiter)
                            all_rows.append(
                                dict(zip(col_names, [v.strip() for v in vals]))
                            )
                        offset += len(data_rows)
                        if len(data_rows) < pkg_size:
                            break

                    df = pd.DataFrame(all_rows)
                    logger.info("Extracted %d rows from %s", len(df), name)
                    results.append(
                        {"df": df, "filename": f"{name}.parquet", "table_name": name}
                    )
                    break
                except Exception as e:
                    logger.warning(
                        "Attempt %d/%d for %s failed: %s", attempt, retry_count, name, e
                    )
                    if attempt == retry_count:
                        raise
                    time.sleep(retry_delay)
        return results


class SapERP6Connector(_SapRFCBase):
    """SAP ERP 6.0 via RFC."""

    pass


class SapS4HANAOnPremConnector(_SapRFCBase):
    """SAP S/4HANA On-Premise via RFC (with optional OData fallback)."""

    pass


class SapBW4HANAConnector(BaseConnector):
    """SAP BW/4HANA via ODP/RFC."""

    DATA_TYPE = "sap"

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("host"):
            raise ValueError("connection.host is required")
        if not conn.get("user"):
            raise ValueError("connection.user is required")

    def connect(self):
        conn = self.source_cfg["connection"]
        self._conn = pyrfc.Connection(
            ashost=conn["host"],
            sysnr=conn.get("instance_number", "00"),
            client=conn.get("client", "100"),
            user=conn["user"],
            passwd=conn["password"],
            lang=conn.get("language", "EN"),
        )
        logger.info("ODP connection to BW/4HANA %s established", conn["host"])

    def extract(self):
        tables = self.source_cfg.get("tables", [])
        ext = self.source_cfg.get("extraction", {})
        pkg_size = ext.get("package_size", 50000)
        batch_cfg = self.source_cfg.get("batch", {})
        retry_count = batch_cfg.get("retry_count", 3)
        retry_delay = batch_cfg.get("retry_delay_seconds", 30)

        results = []
        for tbl in tables:
            name = tbl["name"]
            fields = tbl.get("fields", [])
            logger.info("Extracting %s from BW/4HANA via ODP...", name)

            for attempt in range(1, retry_count + 1):
                try:
                    all_rows = []
                    offset = 0
                    while True:
                        params = {
                            "QUERY_TABLE": name,
                            "DELIMITER": "|",
                            "ROWSKIPS": offset,
                            "ROWCOUNT": pkg_size,
                        }
                        if fields:
                            params["FIELDS"] = [{"FIELDNAME": f} for f in fields]
                        result = self._conn.call("RFC_READ_TABLE", **params)
                        data_rows = result.get("DATA", [])
                        field_info = result.get("FIELDS", [])
                        if not data_rows:
                            break
                        col_names = [f["FIELDNAME"] for f in field_info]
                        for row in data_rows:
                            vals = row["WA"].split("|")
                            all_rows.append(
                                dict(zip(col_names, [v.strip() for v in vals]))
                            )
                        offset += len(data_rows)
                        if len(data_rows) < pkg_size:
                            break

                    df = pd.DataFrame(all_rows)
                    logger.info("Extracted %d rows from %s", len(df), name)
                    results.append(
                        {"df": df, "filename": f"{name}.parquet", "table_name": name}
                    )
                    break
                except Exception as e:
                    logger.warning(
                        "Attempt %d/%d for %s: %s", attempt, retry_count, name, e
                    )
                    if attempt == retry_count:
                        raise
                    time.sleep(retry_delay)
        return results


class SapHANAConnector(BaseConnector):
    """SAP HANA via direct SQL (hdbcli)."""

    DATA_TYPE = "sap"

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("host"):
            raise ValueError("connection.host is required")
        if not conn.get("user"):
            raise ValueError("connection.user is required")

    def connect(self):
        conn = self.source_cfg["connection"]
        self._conn = dbapi.connect(
            address=conn["host"],
            port=int(conn.get("port", 30015)),
            user=conn["user"],
            password=conn["password"],
            databaseName=conn.get("database") or None,
            encrypt=conn.get("encrypt", True),
            sslValidateCertificate=conn.get("sslValidateCertificate", False),
        )
        self._schema = conn.get("schema", "SAPABAp1")
        logger.info("HANA connection to %s established", conn["host"])

    def extract(self):
        tables = self.source_cfg.get("tables", [])
        ext = self.source_cfg.get("extraction", {})
        fetch_size = ext.get("fetch_size", 50000)
        batch_cfg = self.source_cfg.get("batch", {})
        retry_count = batch_cfg.get("retry_count", 3)
        retry_delay = batch_cfg.get("retry_delay_seconds", 10)

        results = []
        for tbl in tables:
            name = tbl["name"]
            custom_query = tbl.get("query")
            fields = tbl.get("fields", [])
            where = tbl.get("where", "")

            if custom_query:
                sql = custom_query
            else:
                cols = ", ".join(fields) if fields else "*"
                sql = f'SELECT {cols} FROM "{self._schema}"."{name}"'
                if where:
                    sql += f" WHERE {where}"

            logger.info("Executing: %s", sql[:200])
            for attempt in range(1, retry_count + 1):
                try:
                    cursor = self._conn.cursor()
                    cursor.execute(sql)
                    columns = [desc[0] for desc in cursor.description]
                    rows = []
                    while True:
                        batch = cursor.fetchmany(fetch_size)
                        if not batch:
                            break
                        rows.extend(batch)
                    cursor.close()
                    df = pd.DataFrame(rows, columns=columns)
                    logger.info("Extracted %d rows from %s", len(df), name)
                    results.append(
                        {"df": df, "filename": f"{name}.parquet", "table_name": name}
                    )
                    break
                except Exception as e:
                    logger.warning(
                        "Attempt %d/%d for %s: %s", attempt, retry_count, name, e
                    )
                    if attempt == retry_count:
                        raise
                    time.sleep(retry_delay)
        return results


class SapS4HANACloudConnector(BaseConnector):
    """SAP S/4HANA Cloud via OData v4 API."""

    DATA_TYPE = "sap"

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("base_url"):
            raise ValueError("connection.base_url is required")
        if conn.get("auth_type") == "oauth2":
            if not conn.get("token_url") or not conn.get("client_id"):
                raise ValueError("OAuth2 requires token_url and client_id")

    def connect(self):
        conn = self.source_cfg["connection"]
        self._base_url = conn["base_url"].rstrip("/")
        self._session = requests.Session()
        self._session.verify = conn.get("verify_ssl", True)

        if conn.get("auth_type") == "oauth2":
            token_resp = requests.post(
                conn["token_url"],
                data={"grant_type": "client_credentials"},
                auth=(conn["client_id"], conn["client_secret"]),
                verify=conn.get("verify_ssl", True),
            )
            token_resp.raise_for_status()
            token = token_resp.json()["access_token"]
            self._session.headers["Authorization"] = f"Bearer {token}"
        else:
            self._session.auth = (conn.get("username", ""), conn.get("password", ""))

        logger.info("S/4HANA Cloud OData session established")

    def extract(self):
        entities = self.source_cfg.get("entities", [])
        ext = self.source_cfg.get("extraction", {})
        page_size = ext.get("page_size", 5000)
        max_pages = ext.get("max_pages", 0)
        timeout = ext.get("timeout_seconds", 300)
        batch_cfg = self.source_cfg.get("batch", {})
        retry_count = batch_cfg.get("retry_count", 3)
        retry_delay = batch_cfg.get("retry_delay_seconds", 10)

        results = []
        for entity in entities:
            entity_set = entity["name"]
            sap_table = entity.get("sap_table", entity_set)
            select = entity.get("select", [])
            filter_expr = entity.get("filter")

            logger.info("Fetching OData entity %s ...", entity_set)
            all_rows = []
            url = f"{self._base_url}/sap/opu/odata4/sap/api_equipment/srvd_a2x/sap/{entity_set}"
            params = {"$top": page_size, "$format": "json"}
            if select:
                params["$select"] = ",".join(select)
            if filter_expr:
                params["$filter"] = filter_expr

            page_count = 0
            for attempt in range(1, retry_count + 1):
                try:
                    while url:
                        resp = self._session.get(url, params=params, timeout=timeout)
                        resp.raise_for_status()
                        data = resp.json()
                        all_rows.extend(data.get("value", []))
                        page_count += 1
                        if max_pages and page_count >= max_pages:
                            break
                        url = data.get("@odata.nextLink")
                        params = {}
                    break
                except Exception as e:
                    logger.warning(
                        "Attempt %d/%d for %s: %s", attempt, retry_count, entity_set, e
                    )
                    if attempt == retry_count:
                        raise
                    time.sleep(retry_delay)

            df = pd.DataFrame(all_rows)
            logger.info("Extracted %d rows from %s", len(df), entity_set)
            results.append(
                {"df": df, "filename": f"{sap_table}.parquet", "table_name": sap_table}
            )
        return results




class MaximoConnector(BaseConnector):
    """IBM Maximo EAM via OSLC REST API."""

    DATA_TYPE = "sap"

    _OBJECT_TABLE_MAP = {
        "MXASSET": "EQUI",
        "MXWO": "AUFK",
        "MXSR": "QMEL",
        "MXJOBPLAN": "PLKO",
        "MXINVENTORY": "MARD",
        "MXPM": "MHIS",
    }

    def validate_config(self):
        conn = self.source_cfg.get("connection", {})
        if not conn.get("base_url"):
            raise ValueError("connection.base_url is required")
        auth = conn.get("auth_type", "apikey")
        if auth == "apikey" and not conn.get("api_key"):
            raise ValueError("connection.api_key is required for apikey auth")
        if auth == "basic":
            if not conn.get("username") or not conn.get("password"):
                raise ValueError(
                    "connection.username and password required for basic auth"
                )
        if auth == "oauth2":
            if not conn.get("token_url") or not conn.get("client_id"):
                raise ValueError("OAuth2 requires token_url and client_id")

    def connect(self):
        conn = self.source_cfg["connection"]
        self._base = conn["base_url"].rstrip("/")
        self._session = requests.Session()
        self._session.verify = conn.get("verify_ssl", True)
        self._timeout = conn.get("timeout_seconds", 300)

        auth_type = conn.get("auth_type", "apikey")
        if auth_type == "apikey":
            self._session.headers["apikey"] = conn["api_key"]
        elif auth_type == "oauth2":
            token_resp = requests.post(
                conn["token_url"],
                data={
                    "grant_type": "client_credentials",
                    "client_id": conn["client_id"],
                    "client_secret": conn.get("client_secret", ""),
                },
                verify=conn.get("verify_ssl", True),
            )
            token_resp.raise_for_status()
            token = token_resp.json()["access_token"]
            self._session.headers["Authorization"] = f"Bearer {token}"
        else:
            self._session.auth = (conn["username"], conn["password"])

        self._session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        logger.info("Maximo OSLC session established (%s auth)", auth_type)

    def extract(self):
        object_structures = self.source_cfg.get("object_structures", [])
        ext = self.source_cfg.get("extraction", {})
        page_size = ext.get("page_size", 500)
        max_pages = ext.get("max_pages", 0)
        batch_cfg = self.source_cfg.get("batch", {})
        retry_count = batch_cfg.get("retry_count", 3)
        retry_delay = batch_cfg.get("retry_delay_seconds", 10)

        results = []
        for obj in object_structures:
            obj_name = obj["name"]
            select = obj.get("select", [])
            where_clause = obj.get("where", "")
            sap_table = obj.get(
                "sap_table_equivalent", self._OBJECT_TABLE_MAP.get(obj_name, obj_name)
            )

            logger.info("Fetching Maximo object structure %s ...", obj_name)
            url = f"{self._base}/oslc/os/{obj_name.lower()}"
            params = {
                "lean": 1,
                "oslc.pageSize": page_size,
            }
            if select:
                params["oslc.select"] = ",".join(select)
            if where_clause:
                params["oslc.where"] = where_clause

            all_rows = []
            page_count = 0
            for attempt in range(1, retry_count + 1):
                try:
                    current_url = url
                    current_params = params
                    while current_url:
                        resp = self._session.get(
                            current_url,
                            params=current_params,
                            timeout=self._timeout,
                        )
                        resp.raise_for_status()
                        data = resp.json()
                        members = data.get("member", data.get("rdfs:member", []))
                        all_rows.extend(members)
                        page_count += 1
                        if max_pages and page_count >= max_pages:
                            break
                        href = (
                            data.get("responseInfo", {}).get("nextPage", {}).get("href")
                        )
                        if href:
                            current_url = href
                            current_params = {}
                        else:
                            current_url = None
                    break
                except Exception as e:
                    logger.warning(
                        "Attempt %d/%d for %s: %s", attempt, retry_count, obj_name, e
                    )
                    if attempt == retry_count:
                        raise
                    time.sleep(retry_delay)

            if all_rows:
                df = pd.DataFrame(all_rows)
                drop_cols = [
                    c for c in df.columns if c.startswith(("rdf:", "oslc:", "spi:"))
                ]
                df = df.drop(columns=drop_cols, errors="ignore")
                logger.info(
                    "Extracted %d rows from %s → %s", len(df), obj_name, sap_table
                )
                results.append(
                    {
                        "df": df,
                        "filename": f"{sap_table}.parquet",
                        "table_name": sap_table,
                    }
                )

        return results
