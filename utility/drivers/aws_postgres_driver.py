
import os
from utility.middleware import plant_code_ctx

from dotenv import load_dotenv
from utility.secret_manager import load_secrets

load_dotenv()
#load_secrets()


######################### AWSPostgresDriver #######################################

# Currently this is dummy DB
class AwsPostgresDriver:
    def __init__(self):
        self.config = {
            "host": os.getenv("AWS_POSTGRES_HOST"),
            "database": "decisionops",
            "user": os.getenv("AWS_POSTGRES_USER"),
            "password": os.getenv("AWS_POSTGRES_PASSWORD") or "",
            "port": int(os.getenv("AWS_POSTGRES_PORT") or 5432),
        }

        plant_code_id = plant_code_ctx.get()
        if plant_code_id:
            self.config["database"] = (
                f"{self.config['database']}_{plant_code_id.lower().replace('-','_')}"
            )

        self.conn = None
        self.engine = None

    def connect(self, **kwargs):
        """
        Dummy connect method.
        Replace with actual AWS PostgreSQL connection logic when needed.
        """
        return self.conn

    def get_engine(self):
        """
        Dummy SQLAlchemy engine method.
        Replace with actual engine creation when needed.
        """
        return self.engine

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

        if self.engine:
            self.engine.dispose()
            self.engine = None

