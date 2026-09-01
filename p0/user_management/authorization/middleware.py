import json

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from p0.user_management.authorization.session_auth import get_username_from_session

# Which URL prefix maps to which feature in user_management.feature
# Values are lowercase since they're always compared against lowercased
# permission keys (see feature.lower() below) and the DB stores them
# lowercase too.
FEATURE_MAP = {
    "/p2/modelFactory": "modelfactory"
}

# Which HTTP method maps to which permission column
METHOD_PERMISSION_MAP = {
    "GET": "v",
    "POST": "c",
    "PUT": "m",
    "PATCH": "m",
    "DELETE": "d",
}
PERMISSION_MAP = {
    "v": "view",
    "c": "create",
    "m": "modify",
    "d": "delete"
}

class UserPermissionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        method = request.method.upper()
        path = request.url.path

        if method == "OPTIONS":
            return await call_next(request)

        feature = next(
            (f for prefix, f in FEATURE_MAP.items() if path.startswith(prefix)),
            None,
        )
        if not feature:
            return await call_next(request)

        # 1. Fetch username from Redis using session_id
        session_id = request.cookies.get("session_id")
        username = await get_username_from_session(session_id)
        if not username:
            msg = "Access Denied: no valid session"
            print(msg)
            return JSONResponse(status_code=401, content={"detail": msg})

        # Bypass checks for admin role
        # user = getattr(request.state, "user", None)
        # if user and isinstance(user, dict) and "admin" in user.get("roles", []):
        #     request.state.username = username
        #     return await call_next(request)

        # 2. Extract plant_code_id
        plant_code_id = None
        if request.method in {"POST", "PUT", "PATCH"}:
            body = await request.body()

            async def receive(_body=body):
                return {"type": "http.request", "body": _body, "more_body": False}

            request._receive = receive
            if body:
                try:
                    payload = json.loads(body)
                    val = payload.get("plant_code_id")
                    if val:
                        plant_code_id = str(val).lower()
                except Exception:
                    pass
        elif request.method in {"GET", "DELETE"}:
            val = request.query_params.get("plant_code_id")
            if val:
                plant_code_id = val.lower()

        if not plant_code_id:
            msg = "Access Denied: missing plant_code_id"
            print(msg)
            return JSONResponse(status_code=400, content={"detail": msg})

        # 3. Check user permissions from Redis session
        user = getattr(request.state, "user", None)
        user_permissions = user.get("permissions") if isinstance(user, dict) else None
        if user_permissions is None:
            msg = "Access Denied: Permissions not found in session. Please hit /me to initialize permissions."
            print(msg)
            return JSONResponse(status_code=401, content={"detail": msg})

        # 4. Check what permissions are assigned to that user for this feature
        plant_permissions = user_permissions.get(plant_code_id, {})
        allowed = set(plant_permissions.get(feature, []))
        required = METHOD_PERMISSION_MAP.get(method)

        # 5. Assign access only if the required permission is present
        if required in allowed:
            request.state.username = username
            return await call_next(request)

        # 6. Otherwise, access denied
        msg = f"Access Denied: You don't have {PERMISSION_MAP[required].capitalize()} permission on {feature.capitalize()} for plant {plant_code_id.capitalize()}"
        print(msg)
        return JSONResponse(status_code=403, content={"detail": msg})