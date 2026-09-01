import os
import logging

from dotenv import load_dotenv
from utility.secret_manager import load_secrets

load_dotenv()
#load_secrets()
logger = logging.getLogger(__name__)


# ######################### AWSIoTDBDriver #######################################

class AwsIoTDBDriver:
    def __init__(
        self, host=None, port=None, user=None, password=None, device_root=None
    ):
        self.config = {
            "host": host or os.getenv("AWS_IOTDB_HOST", "localhost"),
            "port": port or int(os.getenv("AWS_IOTDB_PORT", 18080)),
            "user": user or os.getenv("AWS_IOTDB_USER", "root"),
            "password": password or os.getenv("AWS_IOTDB_PASSWORD", "root"),
            "device_root": device_root
            or os.getenv("AWS_IOTDB_DEVICE_ROOT", "root.decisionops.sensors"),
        }

        self.base_url = (
            f"http://{self.config['host']}:{self.config['port']}/rest/v2"
        )
        self.auth = (self.config["user"], self.config["password"])

    @property
    def device_root(self) -> str:
        return self.config["device_root"]

    def as_dict(self) -> dict:
        """Dummy connection dict."""
        return {
            "base_url": self.base_url,
            "auth": self.auth,
        }

    def insert_tablet_url(self) -> str:
        return f"{self.base_url}/insertTablet"

    def query_url(self) -> str:
        return f"{self.base_url}/query"

    def non_query_url(self) -> str:
        return f"{self.base_url}/nonQuery"

    def connect(self):
        """
        Dummy connect method.
        Replace with actual AWS IoTDB connection logic when implemented.
        """
        return {
            "base_url": self.base_url,
            "auth": self.auth,
        }

    def query(self, sql, row_limit: int | None = None):
        """
        Dummy query method.
        Replace with actual AWS IoTDB query logic when implemented.
        """
        return {}

    def close(self):
        pass

