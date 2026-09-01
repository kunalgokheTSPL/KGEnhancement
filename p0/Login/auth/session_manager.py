import json
import secrets
import time

from typing import Optional

from utility.database_driver import redis_driver as redis_client


class SessionManager:

    def __init__(self):
        self.driver = redis_client()
        self.redis = self.driver.redis

    async def create_session(
        self,
        uid: str,
        token_id: Optional[str],
        access_token: str,
        expires_at: int,
        email: str,
        username: str,
        tenant_id: str,
        roles: list[str],
        features: list[str],
    ) -> str:

        ttl = expires_at - int(time.time())

        if ttl <= 0:
            raise ValueError(
                "Access token has already expired"
            )

        session_id = secrets.token_urlsafe(32)

        session_key = f"session:{session_id}"

        await self.redis.hset(
            session_key,
            mapping={
                "uid": uid,
                "token_id": token_id or "",
                "access_token": access_token,
                "email": email,
                "username": username,
                "tenant_id": tenant_id,
                "roles": json.dumps(roles),
                "features": json.dumps(features),
                "expires_at": str(expires_at),
            },
        )

        await self.redis.expire(
            session_key,
            ttl,
        )

        return session_id

    async def get_session(
        self,
        session_id: str,
    ) -> Optional[dict]:

        session_key = f"session:{session_id}"

        data = await self.redis.hgetall(
            session_key
        )

        if not data:
            return None

        expires_at = int(
            data.get("expires_at", "0")
        )

        # Extra protection in case Redis TTL and
        # expires_at get out of sync.
        if expires_at <= int(time.time()):
            await self.delete_session(session_id)
            return None

        permissions_raw = data.get("permissions")
        permissions = json.loads(permissions_raw) if permissions_raw else None

        return {
            "session_id": session_id,
            "uid": data.get("uid"),
            "token_id": data.get("token_id"),
            "access_token": data.get("access_token"),
            "email": data.get("email"),
            "username": data.get("username"),
            "tenant_id": data.get("tenant_id"),

            "roles": json.loads(
                data.get("roles", "[]")
            ),

            "features": json.loads(
                data.get("features", "[]")
            ),

            "permissions": permissions,

            "expires_at": expires_at,
        }

    async def save_permissions(
        self,
        session_id: str,
        permissions: dict,
    ) -> None:
        """Save user permissions inside the Redis session hash."""
        if not session_id:
            return
        session_key = f"session:{session_id}"
        await self.redis.hset(
            session_key,
            "permissions",
            json.dumps(permissions),
        )

    async def delete_session(
        self,
        session_id: str,
    ) -> bool:

        session_key = f"session:{session_id}"

        deleted = await self.redis.delete(
            session_key
        )

        return deleted > 0