from typing import Set

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from p0.Login.auth.session_manager import SessionManager

session_manager = SessionManager()

class EntraAuthMiddleware(BaseHTTPMiddleware):

    # APIs that do NOT require authentication
    PUBLIC_PATHS: Set[str] = {
        "/",
        "/me",
        "/health",
        "/docs",
        "/swagger",
        "/redoc",
        "/openapi.json",
    }

    async def dispatch(
        self,
        request: Request,
        call_next,
    ):

        # ------------------------------------------------------
        # OPTIONS
        # ------------------------------------------------------

        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path

        # ------------------------------------------------------
        # Public endpoints
        # ------------------------------------------------------

        if path in self.PUBLIC_PATHS:
            return await call_next(request)

        if path == "/p0/login/me":
            return await call_next(request)

        # ------------------------------------------------------
        # Swagger / OpenAPI
        # ------------------------------------------------------

        if (
            path.startswith("/swagger")
            or path.startswith("/docs")
            or path.startswith("/redoc")
        ):
            return await call_next(request)

        # ------------------------------------------------------
        # Get application session
        # ------------------------------------------------------

        session_id = request.cookies.get(
            "session_id"
        )
        access_token = request.cookies.get(
            "access_token"
        )


        if not session_id:
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "Missing session"
                },
            )

        if not access_token:
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "Missing access token"
                },
            )

        # ------------------------------------------------------
        # Validate session against Redis
        # ------------------------------------------------------

        try:
            session = await session_manager.get_session(
                session_id
            )

        except Exception:
            return JSONResponse(
                status_code=500,
                content={
                    "detail": "Session service unavailable"
                },
            )

        if not session:
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "Session expired or logged out"
                },
            )

        # ------------------------------------------------------
        # Validate access token
        # ------------------------------------------------------

        stored_access_token = session.get("access_token")

        if not stored_access_token:
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "Invalid session"
                },
            )

        if access_token != stored_access_token:
            return JSONResponse(
                status_code=401,
                content={
                    "detail": "Invalid access token"
                },
            )
        
        # ------------------------------------------------------
        # Store authentication context
        # ------------------------------------------------------

        request.state.session_id = session_id
        request.state.uid = session["uid"]
        request.state.user = session
        request.state.roles = session.get("roles",[])
        request.state.features = session.get("features",[])

        return await call_next(request)