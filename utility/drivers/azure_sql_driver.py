import os
import pyodbc
import logging
import pandas as pd

from urllib.parse import quote_plus
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

DEVICE_ROOT = os.getenv("IOTDB_DEVICE_ROOT", "root.decisionops.sensors")


class AzureSQLDriver:

    def __init__(self, database: str | None = None):
        self.config = {
            "server": os.getenv("AZURE_SQL_SERVER"),
            "database": os.getenv("AZURE_SQL_DB"),
            "user": os.getenv("AZURE_SQL_USER"),
            "password": os.getenv("AZURE_SQL_PASSWORD"),
            "driver": os.getenv("AZURE_SQL_DRIVER"),
            "device_root": DEVICE_ROOT,
        }

        self.conn = None
        self.engine = None
    # Need to fix 
    @property
    def device_root(self):
        return self.config["device_root"]

    def as_dict(self):
        return dict(self.config)

    def insert_tablet_url(self):
        logger.warning("insert_tablet_url() is a no-op under Azure SQL.")
        return None

    def query_url(self):
        logger.warning("query_url() is a no-op under Azure SQL.")
        return None

    def non_query_url(self):
        logger.warning("non_query_url() is a no-op under Azure SQL.")
        return None

    def _build_connection_string(self):
        return (
            f"DRIVER={{{self.config['driver']}}};"
            f"SERVER={self.config['server']};"
            f"DATABASE={self.config['database']};"
            f"UID={self.config['user']};"
            f"PWD={self.config['password']};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=60;"
        )

    def connect(self):
        if self.conn:
            return self.conn

        conn_str = self._build_connection_string()
        self.conn = pyodbc.connect(conn_str, timeout=60)

        logger.info(
            f"Connected to Azure SQL: {self.config['server']} / {self.config['database']}"
        )

        return self.conn

    def get_engine(self):
        if self.engine:
            return self.engine

        params = quote_plus(self._build_connection_string())

        self.engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={params}",
            pool_pre_ping=True,
        )

        return self.engine

    def execute(self, sql, params=None):
        conn = self.connect()
        cursor = conn.cursor()

        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        conn.commit()
        cursor.close()

    def query(self, sql, row_limit=None):
        conn = self.connect()

        if (
            row_limit
            and sql.strip().lower().startswith("select")
            and "top " not in sql.lower()
        ):
            sql = sql.replace("SELECT", f"SELECT TOP {row_limit}", 1)

        return pd.read_sql(sql, conn)

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

        if self.engine:
            self.engine.dispose()
            self.engine = None
 