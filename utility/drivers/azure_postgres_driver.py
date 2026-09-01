import os
from urllib.parse import quote_plus
import psycopg2
from sqlalchemy import create_engine
from utility.middleware import plant_code_ctx, DatabaseNotFoundError
 
class AzurePostgresDriver:
    def __init__(self):
        self.config = {
            "host": os.getenv("AZURE_PG_HOST"),
            "database": "decisionops",
            "user": os.getenv("AZURE_PG_USER"),
            "password": os.getenv("AZURE_PG_PASSWORD") or "",
            "port": int(os.getenv("AZURE_PG_PORT") or 5432),
        }
 
        plant_code_id = plant_code_ctx.get()
        if plant_code_id:
            db_name_clean = self.config["database"].lower().replace("_", "")
            plant_code_clean = plant_code_id.lower().replace("_", "").replace("-", "")
            if not db_name_clean.endswith(plant_code_clean):
                self.config["database"] = (
                    f"{self.config['database']}_{plant_code_id.lower().replace('-','_')}"
                )
 
        self.conn = None
        self.engine = None
 
    def connect(self, **kwargs):
        if self.conn:
            return self.conn
 
        params = {
            "host": self.config["host"],
            "dbname": self.config["database"],
            "user": self.config["user"],
            "password": self.config["password"],
            "port": self.config["port"],
            "sslmode": "require",
            "connect_timeout": kwargs.get("connect_timeout", 10),
        }
        try:
            self.conn = psycopg2.connect(**params)
        except psycopg2.OperationalError as e:
            err_str = str(e).lower()
            if (getattr(e, "pgcode", None) == "3D000" or "does not exist" in err_str) and self.config.get("database") == "decisionops":
                try:
                    # Connect to system database 'postgres' to run CREATE DATABASE
                    sys_params = params.copy()
                    sys_params["dbname"] = "postgres"
                    sys_params["connect_timeout"] = 5
                    sys_conn = psycopg2.connect(**sys_params)
                    sys_conn.autocommit = True
                    with sys_conn.cursor() as cur:
                        cur.execute(f'CREATE DATABASE "{self.config["database"]}"')
                    sys_conn.close()
                    # Retry connection to target database
                    self.conn = psycopg2.connect(**params)
                except Exception:
                    raise e
            else:
                raise DatabaseNotFoundError(
                    f"Database '{self.config['database']}' does not exist."
                ) from e
 
        return self.conn
 
    def get_engine(self):
        if self.engine:
            return self.engine
 
        try:
            self.connect()
        except Exception:
            pass
 
        user = quote_plus(self.config["user"])
        password = quote_plus(self.config["password"])
 
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
            "?sslmode=require",
            pool_pre_ping=True,
        )
 
        return self.engine
 
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
 
        if self.engine:
            self.engine.dispose()
            self.engine = None