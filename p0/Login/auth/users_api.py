"""
auth/users_api.py
─────────────────
FastAPI routes:
  POST /login   → Keycloak tokens + your DB user profile
  POST /signup  → create in Keycloak + insert into users table
  GET  /me      → token claims (any authenticated user)
  GET  /admin   → admin role required
"""

from http import HTTPStatus
from typing import Optional
import os

from fastapi import (
    APIRouter,
    Cookie,
    Depends,
    Response,
)
from fastapi.responses import JSONResponse

from pydantic import (
    BaseModel,
    Field,
    field_validator,
)
from pydantic import ValidationInfo

from p0.Login.auth.auth_deps import (
    TokenData,
    get_current_user,
    verify_me,
    delete_current_user,
    session_manager,
)

from p0.Login.auth.providers.entra import (
    entra_provider,
)

from p0.user_management.authorization.permission_check import get_all_user_permissions
from p0.user_management.authorization.session_auth import extract_username_from_email_or_username

router = APIRouter(tags=["Authentication"])

AUTH_ENABLED = os.getenv("AUTH_ENABLED", "TRUE").lower()

# ─────────────────────────────────────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────────────────────────────────────

class MeRequest(BaseModel):

    access_token: str = Field(
        ...,
        description="Microsoft Entra Access Token"
    )

    @field_validator("access_token")
    @classmethod
    def validate_required(
        cls,
        value,
        info: ValidationInfo
    ):
        if not value:
            raise ValueError(
                f"{info.field_name} cannot be empty"
            )
        return value


class UserProfile(BaseModel):
    """Your DB user record returned alongside the token."""

    id: str
    name: str
    email: str
    role: str
    created_at: Optional[str] = None
    group_name: Optional[str] = None

    @field_validator("id", "name", "email", "role")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value


# class LoginResponse(BaseModel):

#     access_token: str

#     token_type: str

#     expires_in: int

#     user: Optional["UserProfile"] = None


# class DeleteUserRequest(BaseModel):
#     email: EmailStr = Field(..., example="john@example.com")

#     @field_validator("email")
#     @classmethod
#     def validate_required(cls, value, info: ValidationInfo):
#         if not value:
#             raise ValueError(f"{info.field_name} cannot be empty")
#         return value


# class CreateGroupRequest(BaseModel):
#     group_name: str = Field(..., min_length=1, max_length=50, example="engineering")

#     @field_validator("group_name")
#     @classmethod
#     def validate_required(cls, value, info: ValidationInfo):
#         if not value:
#             raise ValueError(f"{info.field_name} cannot be empty")
#         return value


# class AssignGroupRequest(BaseModel):
#     email: EmailStr = Field(..., example="john@example.com")
#     group: str = Field(..., example="engineering")

#     @field_validator("email", "group")
#     @classmethod
#     def validate_required(cls, value, info: ValidationInfo):
#         if not value:
#             raise ValueError(f"{info.field_name} cannot be empty")
#         return value


# class UpdateRoleRequest(BaseModel):
#     email: EmailStr = Field(..., example="john@example.com")
#     new_role: str = Field(..., example="admin")
#     old_role: str = Field(..., example="user")

#     @field_validator("new_role", "old_role")
#     @classmethod
#     def validate_role(cls, v: str) -> str:
#         if v not in {"user", "admin"}:
#             raise ValueError("Role must be 'user' or 'admin'")
#         return v


# class UpdateGroupRequest(BaseModel):
#     email: EmailStr = Field(..., example="john@example.com")
#     new_group: str = Field(..., example="engineering")
#     old_group: str | None = Field(default=None, example="HR")

#     @field_validator("email", "new_group")
#     @classmethod
#     def validate_required(cls, value, info: ValidationInfo):
#         if not value:
#             raise ValueError(f"{info.field_name} cannot be empty")
#         return value


# class RefreshRequest(BaseModel):
#     refresh_token: str = Field(..., description="Refresh token received from /login")

#     @field_validator("refresh_token")
#     @classmethod
#     def validate_required(cls, value, info: ValidationInfo):
#         if not value:
#             raise ValueError(f"{info.field_name} cannot be empty")
#         return value


# class LogoutRequest(BaseModel):
#     refresh_token: str = Field(..., description="Refresh token received from /login")

#     @field_validator("refresh_token")
#     @classmethod
#     def validate_required(cls, value, info: ValidationInfo):
#         if not value:
#             raise ValueError(f"{info.field_name} cannot be empty")
#         return value


# class RefreshResponse(BaseModel):
#     access_token: str
#     refresh_token: str
#     token_type: str
#     expires_in: int
#     refresh_expires_in: int

#     @field_validator(
#         "access_token",
#         "refresh_token",
#         "token_type",
#         "expires_in",
#         "refresh_expires_in",
#     )
#     @classmethod
#     def validate_required(cls, value, info: ValidationInfo):
#         if not value:
#             raise ValueError(f"{info.field_name} cannot be empty")
#         return value


# class SignupRequest(BaseModel):
#     username: str = Field(..., min_length=3, max_length=50, example="john_doe")
#     email: EmailStr = Field(..., example="john@example.com")
#     password: str = Field(..., example="Secret123!")
#     role: str = Field(default="user", example="user")
#     group: str | None = Field(
#         default=None, example="HR", description="Optional group to assign the user to"
#     )

#     @field_validator("password")
#     @classmethod
#     def validate_password(cls, v: str) -> str:
#         errors = []
#         if len(v) < 8:
#             errors.append("at least 8 characters")
#         if not re.search(r"[A-Z]", v):
#             errors.append("at least one uppercase letter")
#         if not re.search(r"[a-z]", v):
#             errors.append("at least one lowercase letter")
#         if not re.search(r"\d", v):
#             errors.append("at least one digit")
#         if not re.search(r"[!@#$%^&*(),.?\":{}|<>_\-]", v):
#             errors.append("at least one special character")
#         if errors:
#             raise ValueError("Password must contain: " + ", ".join(errors))
#         return v

#     @field_validator("username")
#     @classmethod
#     def validate_username(cls, v: str) -> str:
#         if not re.match(r"^[a-zA-Z0-9_]+$", v):
#             raise ValueError(
#                 "Username may only contain letters, digits, and underscores"
#             )
#         return v

#     @field_validator("role")
#     @classmethod
#     def validate_role(cls, v: str) -> str:
#         if v not in {"user", "admin"}:
#             raise ValueError("Role must be 'user' or 'admin'")
#         return v


# class SignupResponse(BaseModel):
#     user_id: str
#     username: str
#     email: str
#     role: str
#     group: str | None = None

#     @field_validator("user_id", "username", "email", "role")
#     @classmethod
#     def validate_required(cls, value, info: ValidationInfo):
#         if not value:
#             raise ValueError(f"{info.field_name} cannot be empty")
#         return value



# @router.post(
#     "/me",
#     status_code=HTTPStatus.OK,
#     summary="Authenticate Microsoft Entra access token ",
# )
# async def me(
#     body: MeRequest,
#     response: Response,
# ):
#     try:

#         user = await entra_provider.authenticate(
#             body.access_token
#         )

#         response.set_cookie(
#             key="access_token",
#             value=body.access_token,
#             httponly=True,
#             secure=False,          # Change to True in production
#             samesite="lax",
#             max_age=3600,
#             path="/",
#         )

#         return {
#             "success": True,
#             "message": "User authenticated successfully",
#             "data": {
#                 "access_token": body.access_token,
#                 "token_type": "Bearer",
#                 "expires_in": 3600,
#                 "user": {
#                     "id": user["sub"],
#                     "name": user["username"],
#                     "email": user["email"],
#                     "role": "user",
#                 },
#             },
#         }

    # except Exception as ex:

    #     return JSONResponse(
    #         status_code=HTTPStatus.UNAUTHORIZED,
    #         content={
    #             "success": False,
    #             "message": "Authentication failed",
    #             "errors": [
    #                 {
    #                     "field": "access_token",
    #                     "message": str(ex),
    #                 }
    #             ],
    #         },
    #     )

@router.get(
    "/me",
    status_code=HTTPStatus.OK,
    summary="Validate current Microsoft Entra session",
)
async def me(
    response: Response,
    current_user: TokenData = Depends(get_current_user),
):
    """
    Validates the access token stored in the HttpOnly cookie.

    Flow:
    Browser
        ↓
    access_token cookie
        ↓
    get_current_user()
        ↓
    entra_provider.authenticate()
        ↓
    validate_access_token()
        ↓
    Microsoft Graph (/me/memberOf)
        ↓
    Return authenticated user
    """

    response.set_cookie(
        key="session_id",
        value=current_user.session_id,
        httponly=True,
        secure=False, # Change to True in production
        samesite="lax",
        path="/",
    )

    try:
        username = extract_username_from_email_or_username(current_user.email, current_user.username)
        if username and current_user.session_id:
            permissions = get_all_user_permissions(username)
            await session_manager.save_permissions(current_user.session_id, permissions)
    except Exception as e:
        print(f"Error caching permissions in /me: {e}")

    return {
        "success": True,
        "authenticated": True,
        "session_id": current_user.session_id,
        "data": {
            "user": {
                "name": current_user.username,
                "email": current_user.email,
                "role": (
                    current_user.roles[0]
                    if current_user.roles
                    else "user"
                ),
            }
        },
    }

@router.post(
    "/logout",
    status_code=HTTPStatus.OK,
    summary="Logout current user",
)
async def logout(response: Response,current_user: TokenData = Depends(delete_current_user)):

    response.delete_cookie(
        key="access_token",
        path="/",
    )
    response.delete_cookie(
        key="session_id",   
        path="/",
    )

    return {
        "success": True,
        "message": "Logged out successfully",
    }
