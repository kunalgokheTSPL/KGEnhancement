"""
Request-scoped actor capture for the activity log.

A single ASGI middleware reads the bearer token from each request and stashes a
display "actor" (email / username / subject) in a contextvar. Audit writers
(`audit.record_event`, `audit.upsert_job`) read it, so EVERY mutating action is
attributed to the real user automatically — no per-endpoint auth dependency, no
risk to the upload flow.

Important: this does NOT perform authentication. Auth is still enforced by the
route-level dependencies (`require_admin` / `require_authenticated`). Here we
only decode the JWT *claims* (unverified) to label who did it — read-only audit
metadata. If the token is missing/garbage the actor is simply 'system'.
"""

from __future__ import annotations
from jose import jwt
import threading
from contextvars import ContextVar, copy_context

from fastapi import Request

from utility.middleware import plant_code_ctx


_current_actor: ContextVar[str] = ContextVar("current_actor", default="system")


def get_actor() -> str:
    """The actor for the current request (email/username/sub), or 'system'."""
    try:
        return _current_actor.get()
    except Exception:
        return "system"


def set_actor(actor: str | None) -> None:
    _current_actor.set(actor or "system")


def get_plant() -> str | None:
    """The plant_code_id for the current request, or None if unscoped."""
    try:
        return plant_code_ctx.get()
    except Exception:
        return None


def set_plant(plant_code_id: str | None) -> None:
    plant_code_ctx.set(plant_code_id or None)


def _actor_from_token(token: str) -> str:
    """Best-effort display name from a JWT's claims (UNVERIFIED — label only)."""
    try:
        claims = jwt.get_unverified_claims(token)
        return (
            claims.get("email")
            or claims.get("preferred_username")
            or claims.get("username")
            or claims.get("sub")
            or "system"
        )
    except Exception:
        return "system"


async def _plant_from_request(request: Request) -> str | None:
    """Pull plant_code_id from query string, JSON body, or multipart form (uploads)."""
    plant = request.query_params.get("plant_code_id")
    if plant or request.method not in ("POST", "PUT", "PATCH"):
        return plant or None

    content_type = request.headers.get("content-type", "").lower()
    if "application/json" in content_type:
        try:
            body = await request.json()
        except ValueError:
            body = None
        if isinstance(body, dict):
            return body.get("plant_code_id") or None
    elif (
        "multipart/form-data" in content_type
        or "application/x-www-form-urlencoded" in content_type
    ):
        try:
            value = (await request.form()).get("plant_code_id")
        except Exception:
            value = None
        if isinstance(value, str):
            return value or None
    return None


async def request_context_dep(request: Request) -> None:
    """Populate the request-scoped contextvars: the audit actor and plant_code_id."""
    token = request.cookies.get("access_token")
    if not token:
        auth = request.headers.get("authorization") or request.headers.get(
            "Authorization"
        )
        if auth and auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
    set_actor(_actor_from_token(token) if token else "system")

    set_plant(await _plant_from_request(request))


def spawn_with_context(target, args=(), kwargs=None, daemon: bool = True) -> threading.Thread:
    """Start a thread that inherits this request's contextvars (actor, plant).

    Background work (connector ingest, pipeline runs) writes audit/flow rows too, and
    a bare threading.Thread does NOT carry contextvars — get_actor() would fall back
    to "system" and the trail would lose who triggered it."""
    ctx = copy_context()
    t = threading.Thread(
        target=lambda: ctx.run(target, *args, **(kwargs or {})), daemon=daemon
    )
    t.start()
    return t
