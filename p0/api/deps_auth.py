"""
Auth dependencies for the CDM Admin API (p0).

Everything under ``/api/*`` except ``/api/health/*`` is admin-only — uploads,
pipeline runs, commits, ontology exports, log reads. We delegate the actual
JWT verification to the Login service's Keycloak-backed dependencies so
there's a single source of truth for "what does authenticated/admin mean".

Two public symbols:

  ``require_authenticated``
        Re-export of ``p0.Login.auth.auth_deps.get_current_user``. Use when
        any logged-in user should reach an endpoint (rare in p0 — kept for
        symmetry with the Login service).

  ``require_admin``
        ``Depends(require_roles("admin"))`` instance. Used as the
        router-level dependency on every protected p0 router so individual
        route decorators stay clean. Composes correctly when added at both
        levels — FastAPI deduplicates via use_cache=True, so we don't pay
        for the realm-role check twice in a single request.
"""

from __future__ import annotations


from p0.Login.auth.auth_deps import (
    TokenData,
    get_current_user,
    require_roles,
)


require_authenticated = get_current_user

require_admin = require_roles("admin")


__all__ = [
    "TokenData",
    "require_admin",
    "require_authenticated",
]
