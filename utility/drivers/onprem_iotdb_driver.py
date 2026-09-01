import requests
import os

# ######################### OnpremIoTDBDriver #######################################

class OnpremIoTDBDriver:
    def __init__(
        self, host=None, port=None, user=None, password=None, device_root=None
    ):
        self.config = {
            "host": host or os.getenv("IOTDB_HOST", "localhost"),
            "port": port or int(os.getenv("IOTDB_PORT", 18080)),
            "user": user or os.getenv("IOTDB_USER", "root"),
            "password": password or os.getenv("IOTDB_PASSWORD", "root"),
            "device_root": device_root
            or os.getenv("IOTDB_DEVICE_ROOT", "root.decisionops.sensors"),
        }
        self.base_url = f"http://{self.config['host']}:{self.config['port']}/rest/v2"
        self.auth = (self.config["user"], self.config["password"])

    @property
    def device_root(self) -> str:
        return self.config["device_root"]

    def as_dict(self) -> dict:
        """Connection dict compatible with legacy iotdb_driver helpers."""
        return {"base_url": self.base_url, "auth": self.auth}

    def insert_tablet_url(self) -> str:
        return f"{self.base_url}/insertTablet"

    def query_url(self) -> str:
        return f"{self.base_url}/query"

    def non_query_url(self) -> str:
        return f"{self.base_url}/nonQuery"

    def connect(self):
        try:
            response = requests.post(
                self.query_url(),
                json={"sql": "SHOW DATABASES"},
                auth=self.auth,
                timeout=60,
            )

            response.raise_for_status()

            return {
                "base_url": self.base_url,
                "auth": self.auth,
            }

        except Exception as e:
            raise Exception(f"IoTDB connection error: {e}")

    def query(self, sql, row_limit: int | None = None):
        payload = {"sql": sql}
        if row_limit is not None:
            payload["row_limit"] = row_limit
        response = requests.post(
            f"{self.base_url}/query",
            json=payload,
            auth=self.auth,
            headers={"Content-Type": "application/json"},
            timeout=45,
        )
        if response.status_code != 200:
            raise Exception(f"IoTDB query failed: {response.text}")
        data = response.json()
        if "code" in data and "message" in data:
            raise RuntimeError(f"IoTDB Error {data['code']}: {data['message']}")
        return data

    def close(self):
        pass

