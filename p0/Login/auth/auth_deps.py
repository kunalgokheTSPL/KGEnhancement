"""
Authentication Dependencies

Replaces Keycloak authentication with Microsoft Entra ID while
keeping the rest of the application unchanged.
"""

from typing import List, Optional
from fastapi import Cookie, Depends, HTTPException, status,Response
from pydantic import BaseModel, Field
from jose.exceptions import ExpiredSignatureError
from p0.Login.auth.providers.entra import entra_provider
from p0.Login.auth.session_manager import SessionManager
import os
import traceback

isSecure = False

session_manager = SessionManager()

# ----------------------------------------------------------------------
# Models
# ----------------------------------------------------------------------

class TokenData(BaseModel):
    session_id: str
    sub: str
    email: str
    username: str
    roles: List[str] = Field(default_factory=list)
    # groups: List[str] = []
    tenant_id: Optional[str] = None

# ----------------------------------------------------------------------
# Verify Me
# ----------------------------------------------------------------------

async def verify_me(
    access_token: str,
) -> TokenData:
    """
    Validate an Entra ID access token and convert it into TokenData.
    """

    try:
        user = await entra_provider.authenticate(access_token)

        # Extract group names
        group_names = [
            group.get("displayName")
            for group in user.get("groups", [])
            if group.get("displayName")
        ]

        # --------------------------------------------------------------
        # Temporary Role Mapping
        # --------------------------------------------------------------

        roles = []

        admin_group_str = os.getenv("ENTRA_ADMIN_GROUP", "")
        user_group_str = os.getenv("ENTRA_USER_GROUP", "")

        admin_groups = {
            g.strip()
            for g in admin_group_str.split(",")
            if g.strip()
        }

        user_groups = {
            g.strip()
            for g in user_group_str.split(",")
            if g.strip()
        }

        if any(
            group in group_names
            for group in admin_groups
        ):
            roles.append("admin")

        if any(
            group in group_names
            for group in user_groups
        ):
            roles.append("user")

        # Default role
        if not roles:
            roles.append("user")

        return TokenData(
            session_id="",
            sub=user["sub"],
            email=user["email"],
            username=user["username"],
            roles=roles,
            tenant_id=user["tenant_id"],
        )

    except Exception as ex:
        # Do not expose internal authentication errors
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
        )


# ----------------------------------------------------------------------
# Current User
# ----------------------------------------------------------------------
async def get_current_user(
    response: Response,
    access_token: Optional[str] = Cookie(default=None),
    refresh_token: Optional[str] = Cookie(default=None),
    session_id: Optional[str] = Cookie(default=None),
) -> TokenData:

    try:

        # ==========================================================
        # 1. Check existing session FIRST
        # ==========================================================

        if session_id:

            session = await session_manager.get_session(
                session_id
            )

            if session and access_token and access_token == session.get("access_token"):

                # Session is valid and matches the current access token.
                # DO NOT validate access token again.
                return TokenData(
                    session_id=session["session_id"],
                    sub=session["uid"],
                    email=session["email"],
                    username=session["username"],
                    roles=session["roles"],
                    tenant_id=session["tenant_id"],
                )

            # Session expired / does not exist or access token doesn't match
            response.delete_cookie(
                key="session_id",
                httponly=True,
                secure=isSecure,
                samesite="lax",
            )

        # ==========================================================
        # 2. No valid session -> access token is required
        # ==========================================================

        if not access_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing access token",
            )

        user = None
        token_refreshed = False

        # ==========================================================
        # 3. Validate access token
        # ==========================================================

        try:

            print("Validating access token")

            user = await entra_provider.authenticate(
                access_token
            )

            print("Access token valid")

        except ExpiredSignatureError:

            print("Access token expired")

            user = None

        except Exception as ex:

            print(
                f"Access token invalid: {ex}"
            )

            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid access token",
            )

        # ==========================================================
        # 4. Access token invalid -> refresh token
        # ==========================================================

        if user is None:

            if not refresh_token:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=(
                        "Access token expired "
                        "and refresh token missing"
                    ),
                )

            try:

                print("Refreshing access token")

                token_response = (
                    await entra_provider.refresh_token(
                        refresh_token
                    )
                )

            except Exception as ex:

                print(
                    f"Token refresh failed: {ex}"
                )

                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or expired refresh token",
                )

            # ------------------------------------------------------
            # New access token
            # ------------------------------------------------------

            new_access_token = (
                token_response.get(
                    "access_token"
                )
            )

            if not new_access_token:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=(
                        "Refresh response missing "
                        "access token"
                    ),
                )

            # ------------------------------------------------------
            # Authenticate new access token
            # ------------------------------------------------------

            try:

                print(
                    "Validating refreshed access token"
                )

                user = await entra_provider.authenticate(
                    new_access_token
                )

            except Exception:

                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=(
                        "Invalid refreshed "
                        "access token"
                    ),
                )

            token_refreshed = True

            # ------------------------------------------------------
            # Update access token cookie
            # ------------------------------------------------------

            response.set_cookie(
                key="access_token",
                value=new_access_token,
                httponly=True,
                secure=isSecure,
                samesite="lax",
                path= "/"
            )

            # ------------------------------------------------------
            # Update refresh token if Entra rotated it
            # ------------------------------------------------------

            new_refresh_token = (
                token_response.get(
                    "refresh_token"
                )
            )

            if new_refresh_token:

                response.set_cookie(
                    key="refresh_token",
                    value=new_refresh_token,
                    httponly=True,
                    secure=isSecure,
                    samesite="lax",
                    path= "/"
                )

        # ==========================================================
        # 5. Extract groups
        # ==========================================================

        group_names = [
            group.get("displayName")
            for group in user.get("groups", [])
            if group.get("displayName")
        ]

        # ==========================================================
        # 6. Role mapping
        # ==========================================================

        roles = []

        admin_group_str = os.getenv("ENTRA_ADMIN_GROUP", "")
        user_group_str = os.getenv("ENTRA_USER_GROUP", "")

        admin_groups = {
            g.strip()
            for g in admin_group_str.split(",")
            if g.strip()
        }

        user_groups = {
            g.strip()
            for g in user_group_str.split(",")
            if g.strip()
        }

        if any(
            group in group_names
            for group in admin_groups
        ):
            roles.append("admin")

        if any(
            group in group_names
            for group in user_groups
        ):
            roles.append("user")

        if not roles:
            roles.append("user")

        # ==========================================================
        # 7. Create new session
        # ==========================================================

        new_session_id = (
            await session_manager.create_session(
                uid=user["uid"],
                token_id=user["token_id"],
                access_token=user["access_token"],
                expires_at=user["expires_at"],
                email=user["email"],
                username=user["username"],
                tenant_id=user["tenant_id"],
                roles=roles,
                features=[""],
            )
        )

        # ==========================================================
        # 8. Set session cookie
        # ==========================================================

        response.set_cookie(
            key="session_id",
            value=new_session_id,
            httponly=True,
            secure=isSecure,
            samesite="lax",
            path="/"
        )

        # ==========================================================
        # 9. Return user
        # ==========================================================

        return TokenData(
            session_id=new_session_id,
            sub=user["sub"],
            email=user["email"],
            username=user["username"],
            roles=roles,
            tenant_id=user["tenant_id"],
        )

    except HTTPException:
        raise

    except Exception:

        traceback.print_exc()

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired authentication",
        )
        
async def delete_current_user(
    session_id: str = Cookie(default=None),
) -> None:
    """
    Delete the current user's session.
    """
    
    try:
        await session_manager.delete_session(session_id)
    except Exception as ex:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to delete session",
        )

# ----------------------------------------------------------------------
# Role Dependency
# ----------------------------------------------------------------------

def require_roles(required_roles: List[str]):

    async def role_checker(
        current_user: TokenData = Depends(get_current_user),
    ):

        if not required_roles:
            return current_user

        if any(
            role in current_user.roles
            for role in required_roles
        ):
            return current_user

        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Insufficient permissions",
        )

    return role_checker
