import os

from redis.asyncio import Redis
from redis_entraid.cred_provider import create_from_default_azure_credential


class AzureRedisDriver:
    """Driver for connecting to an Azure Redis instance."""

    def __init__(
        self,
        host=None,
        port=None,
        username=None,
        password=None,
        db=None,
    ):
        self.config = {
            "host": host or os.getenv("REDIS_HOST"),
            "port": port or int(os.getenv("REDIS_PORT")),
            "username": (
                username
                if username is not None
                else os.getenv("REDIS_USERNAME", "")
            ),
            "password": (
                password
                if password is not None
                else os.getenv("REDIS_PASSWORD", "")
            ),
            "db": (
                db
                if db is not None
                else int(os.getenv("REDIS_DB", 0))
            ),
        }

        credential_provider = create_from_default_azure_credential(
            ("https://redis.azure.com/.default",)
        )

        self.redis = Redis(
            host=self.config["host"],
            port=self.config["port"],
            ssl=True,
            ssl_cert_reqs=None,      # for testing only
            credential_provider=credential_provider,
            db=self.config["db"],
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

    def as_dict(self) -> dict:
        """Connection dict compatible with legacy Redis helpers."""
        return {
            "host": self.config["host"],
            "port": self.config["port"],
            "username": self.config["username"],
            "db": self.config["db"],
        }

    @property
    def host(self) -> str:
        return self.config["host"]

    @property
    def port(self) -> int:
        return self.config["port"]

    @property
    def username(self) -> str:
        return self.config["username"]

    @property
    def db(self) -> int:
        return self.config["db"]

    async def connect(self):
        """Validate the Redis connection."""

        try:
            await self.redis.ping()
            return self.redis

        except Exception as e:
            raise ConnectionError(
                f"Redis connection error: {e}"
            ) from e

    async def ping(self) -> bool:
        """Check whether Redis is reachable."""

        try:
            return await self.redis.ping()

        except Exception as e:
            raise ConnectionError(
                f"Redis ping failed: {e}"
            ) from e

    async def get(self, key: str):
        """Get a value from Redis."""

        try:
            return await self.redis.get(key)

        except Exception as e:
            raise RuntimeError(
                f"Redis GET failed for key '{key}': {e}"
            ) from e

    async def set(
        self,
        key: str,
        value,
        ex: int | None = None,
    ):
        """Set a value in Redis.

        Args:
            key: Redis key.
            value: Value to store.
            ex: Optional expiration time in seconds.
        """

        try:
            return await self.redis.set(
                key,
                value,
                ex=ex,
            )

        except Exception as e:
            raise RuntimeError(
                f"Redis SET failed for key '{key}': {e}"
            ) from e

    async def delete(self, key: str):
        """Delete a Redis key."""

        try:
            return await self.redis.delete(key)

        except Exception as e:
            raise RuntimeError(
                f"Redis DELETE failed for key '{key}': {e}"
            ) from e

    async def exists(self, key: str) -> bool:
        """Check whether a Redis key exists."""

        try:
            return bool(await self.redis.exists(key))

        except Exception as e:
            raise RuntimeError(
                f"Redis EXISTS failed for key '{key}': {e}"
            ) from e

    async def close(self):
        """Close the Redis connection pool."""

        try:
            await self.redis.aclose()

        except Exception as e:
            raise RuntimeError(
                f"Redis close failed: {e}"
            ) from e