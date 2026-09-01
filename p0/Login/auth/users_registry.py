"""
auth/users_registry.py
──────────────────────
Public routes + debug endpoint to diagnose Keycloak login issues.
"""

import httpx
from fastapi import APIRouter
from http import HTTPStatus
from fastapi.responses import JSONResponse
from p0.Login.utils.config import settings

router = APIRouter(tags=["Public"])

@router.get("/health", status_code=HTTPStatus.OK, summary="Health check (public)")
async def health():
    return {
        "success": True,
        "message": "Health check successful",
        "data": {
            "status": "ok"
        },
    }


@router.get("/", status_code=HTTPStatus.OK, summary="API root (public)")
async def root():
    return {
        "success": True,
        "message": "API information retrieved successfully",
        "data": {
            "service": "Keycloak + FastAPI Auth",
            "docs": "/docs",
        },
    }


@router.get(
    "/debug/login-test",
    status_code=HTTPStatus.OK,
    summary="Debug — raw Keycloak login test",
)
async def debug_login_test():
    """
    Calls Keycloak token endpoint directly with hardcoded test user
    and returns the RAW response so we can see exactly what Keycloak says.
    """
    try:
        async with httpx.AsyncClient() as client:
            # 1. Check realm is reachable
            realm_resp = await client.get(settings.realm_url)

            # 2. Try token endpoint with john_doe
            token_resp = await client.post(
                settings.token_endpoint,
                data={
                    "grant_type": "password",
                    "client_id": settings.keycloak_client_id,
                    "client_secret": settings.keycloak_client_secret,
                    "username": "john_doe",
                    "password": "Secret123!",
                },
            )

            # 3. Get user details via admin token
            admin_resp = await client.post(
                settings.token_endpoint,
                data={
                    "grant_type": "client_credentials",
                    "client_id": settings.keycloak_client_id,
                    "client_secret": settings.keycloak_client_secret,
                },
            )
            admin_token = admin_resp.json().get("access_token", "")

            user_resp = await client.get(
                f"{settings.admin_api_url}/users",
                headers={"Authorization": f"Bearer {admin_token}"},
                params={"username": "john_doe", "exact": True},
            )
            users = user_resp.json()
            user_id = users[0]["id"] if users else None

            required_actions = None
            user_detail = {}

            if user_id:
                detail_resp = await client.get(
                    f"{settings.admin_api_url}/users/{user_id}",
                    headers={"Authorization": f"Bearer {admin_token}"},
                )
                user_detail = detail_resp.json()
                required_actions = user_detail.get("requiredActions")

        return {
            "success": True,
            "message": "Debug login test completed successfully",
            "data": {
                "realm_status": realm_resp.status_code,
                "token_endpoint": settings.token_endpoint,
                "login_status_code": token_resp.status_code,
                "login_raw_response": token_resp.json(),
                "user_found": bool(users),
                "user_id": user_id,
                "required_actions_on_user": required_actions,
                "email_verified": user_detail.get("emailVerified") if user_id else None,
                "enabled": user_detail.get("enabled") if user_id else None,
            },
        }

    except Exception as e:
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [
                    {
                        "field": "server",
                        "message": str(e),
                    }
                ],
            },
        )

