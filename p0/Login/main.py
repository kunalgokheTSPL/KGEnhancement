"""
main.py
───────
FastAPI entry point.

Startup order (critical):
  1. load_secrets() — decrypts .env.enc → populates os.environ
  2. Settings()     — reads from os.environ (set in step 1)
  3. FastAPI router    — uses settings

If secrets are loaded AFTER Settings() is instantiated,
config.py will raise ValidationError for missing values.
"""


# ── Step 1: Load encrypted secrets FIRST before any other imports ─────────
# This populates os.environ with all values from .env.enc
# Settings() in config.py reads from os.environ, so this must come first

# ── Step 2: Now safe to import everything else ────────────────────────────
from fastapi import APIRouter

from p0.Login.auth.users_api import router as auth_router
# from p0.Login.auth.users_registry import router as public_router


# ── Step 3: Create FastAPI router ────────────────────────────────────────────

router = APIRouter(prefix="/p0/login")

# router.include_router(public_router)
router.include_router(auth_router)

