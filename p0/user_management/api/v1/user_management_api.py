import logging
from http import HTTPStatus
from typing import Optional
import os

from fastapi import Cookie, Depends, HTTPException, APIRouter

from p0.Login.auth.auth_deps import (
    get_current_user,
    TokenData,
    session_manager,
)
from p0.Login.auth.providers.entra import (
    entra_provider,
)
from p0.user_management.authorization.permission_check import (
    build_permission_dict,
    query_get_all_user_permissions,
    normalize_username,
)
from p0.user_management.authorization.session_auth import extract_username_from_email_or_username
from p0.user_management.user_registry import (
    ConfigRequest,
    ViewRequest,
    normalize_list,
    query_config_request,
    query_view_request,
    query_get_user_permissions
)
from p2.utility.database_driver import PostgresDriver

logger = logging.getLogger(__name__)


def get_connection():
    """Acquire a PostgreSQL connection."""
    custom_config = {
        "database": "decisionops"
    }
    driver = PostgresDriver()
    driver.config.update(custom_config)
    return driver.connect()

# PERMISSION REGISTRY
class PermissionRegistry:

    def __init__(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        try:
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_management (
                    username VARCHAR(255) NOT NULL,
                    feature VARCHAR(255) NOT NULL,
                    v BOOLEAN DEFAULT FALSE,
                    c BOOLEAN DEFAULT FALSE,
                    m BOOLEAN DEFAULT FALSE,
                    d BOOLEAN DEFAULT FALSE,
                    plant_code_id VARCHAR(255) NOT NULL,
                    UNIQUE (
                        username,
                        feature,
                        plant_code_id
                    )
                );
            """)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(str(e)) from e

    # CONFIG PERMISSIONS
    def configure_permissions(
        self,
        data: ConfigRequest
    ):
        usernames = normalize_list(
            data.users,
            "users"
        )
        features = normalize_list(
            data.features,
            "feature"
        )
        plant_code_id = data.plant_code_id.strip().lower()
        if not plant_code_id:
            raise ValueError(
                "plant_code_id is required"
            )
        try:
            self.cursor.execute(
                query_config_request,
                (
                    plant_code_id,
                    usernames,
                    features,
                    data.permissions.v,
                    data.permissions.c,
                    data.permissions.m,
                    data.permissions.d
                )
            )
            self.conn.commit()
            return {
                "message":
                    "Permissions configured successfully",
                "plant_code_id":
                    plant_code_id,
                "users":
                    usernames,
                "features":
                    features,
                "permissions": {
                    "v": data.permissions.v,
                    "c": data.permissions.c,
                    "m": data.permissions.m,
                    "d": data.permissions.d
                },
                "combinations_updated":
                    len(usernames) * len(features)
            }

        except Exception as e:
            self.conn.rollback()
            raise Exception(str(e)) from e

    # VIEW PERMISSIONS
    def view_permissions(
        self,
        data: ViewRequest
    ):
        usernames = normalize_list(
            data.users,
            "users"
        )
        features = normalize_list(
            data.features,
            "feature"
        )
        plant_code_id = data.plant_code_id.strip().lower()
        if not plant_code_id:
            raise ValueError(
                "plant_code_id is required"
            )
        try:
            self.cursor.execute(
                query_view_request,
                (
                    usernames,
                    features,
                    plant_code_id
                )
            )
            row = self.cursor.fetchone()
            return {
                "users":
                    usernames,
                "features":
                    features,
                "permissions": {
                    "v": row[0],
                    "c": row[1],
                    "m": row[2],
                    "d": row[3]
                },
                "plant_code_id":
                    plant_code_id
            }
        except Exception as e:
            self.conn.rollback()
            raise Exception(str(e)) from e

    # GET ALL USER PERMISSIONS (grouped by plant_code_id -> feature -> letters)
    def get_all_user_permissions(self, username):
        username = normalize_username(username)
        try:
            self.cursor.execute(
                query_get_all_user_permissions,
                (username,)
            )
            rows = self.cursor.fetchall()
            return build_permission_dict(rows)
        except Exception as e:
            self.conn.rollback()
            raise Exception(str(e)) from e

    # CLOSE
    def close(self):

        if self.cursor:
            self.cursor.close()

        if self.conn:
            self.conn.close()


# APIROUTER
router = APIRouter(
    prefix="/p0/userManagement",
    tags=["User Permission Management"]
)


def get_permission_registry():
    """Per-request PermissionRegistry, closed after the request completes.

    Replaces the old module-level singleton, which held one Postgres
    connection/cursor open for the lifetime of the app and shared it
    across every concurrent request.
    """
    registry = PermissionRegistry()
    try:
        yield registry
    finally:
        registry.close()


@router.get(
    "/groupMembers",
    status_code=HTTPStatus.OK,
    summary="Get list of users in specified Entra ID security groups",
)
async def get_group_members(
    current_user: TokenData = Depends(get_current_user),
    access_token: Optional[str] = Cookie(default=None),
):
    """
    Fetches member details for standard admin and user security groups.
    """
    admin_group_str = os.getenv("ENTRA_ADMIN_GROUP", "")
    user_group_str = os.getenv("ENTRA_USER_GROUP", "")

    admin_groups = [
        g.strip()
        for g in admin_group_str.split(",")
        if g.strip()
    ]

    user_groups = [
        g.strip()
        for g in user_group_str.split(",")
        if g.strip()
    ]

    target_groups = admin_groups + user_groups

    group_users = {}

    for group_name in target_groups:
        try:
            members = await entra_provider.get_group_members_by_name(
                access_token=access_token,
                group_name=group_name,
            )
            group_users[group_name] = [
                {
                    "id": member.get("id"),
                    "name": member.get("displayName"),
                    "email": member.get("mail") or member.get("userPrincipalName"),
                }
                for member in members
            ]
        except Exception as err:
            group_users[group_name] = {"error": str(err)}

    return {
        "success": True,
        "data": group_users,
    }


# CONFIG API
@router.put("/config")
async def config_permission(
    data: ConfigRequest,
    current_user: TokenData = Depends(get_current_user),
    registry: PermissionRegistry = Depends(get_permission_registry),
):

    try:

        res = registry.configure_permissions(
            data
        )

        # Update cached permissions in Redis for active sessions of all updated users
        try:
            usernames = normalize_list(data.users, "users")
            logger.debug("Users being updated in Postgres: %s", usernames)
            user_new_permissions = {}

            async for session_key in session_manager.redis.scan_iter("session:*"):
                session_data = await session_manager.redis.hgetall(session_key)
                if not session_data:
                    continue

                session_username = extract_username_from_email_or_username(
                    session_data.get("email"),
                    session_data.get("username")
                )

                if session_username not in usernames:
                    continue

                if session_username not in user_new_permissions:
                    user_new_permissions[session_username] = registry.get_all_user_permissions(session_username)
                    logger.debug(
                        "Fetched new permissions for %s: %s",
                        session_username,
                        user_new_permissions[session_username],
                    )

                sid = session_key.split("session:", 1)[1]
                await session_manager.save_permissions(sid, user_new_permissions[session_username])
                logger.debug("Updated Redis session %s for user %s", sid, session_username)

        except Exception:
            logger.exception("Error updating cached permissions in config_permission")

        return res

    except Exception as e:

        raise HTTPException(
            status_code=500,
            detail=str(e)
        ) from e


# VIEW API
@router.post("/view")
def view_permission(
    data: ViewRequest,
    current_user: TokenData = Depends(get_current_user),
    registry: PermissionRegistry = Depends(get_permission_registry),
):

    try:
        return registry.view_permissions(
            data
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        ) from e


@router.get(
    "/featureList",
    status_code=HTTPStatus.OK,
    summary="Get list of feature",
)
async def get_feature_list(
    current_user: TokenData = Depends(get_current_user),
    access_token: Optional[str] = Cookie(default=None),
):
    return {
        "success": True,
        "data": [
            "modelFactory",
            "symbolicAI",
            "decisionTemplate"
        ]
    }