"""Standalone p0 app — for local development and Swagger testing.

The shared gateway (``backend/app.py``) mounts every pod (p0/p1/p2) behind one
``/docs``. This module builds an app with ONLY the p0 routers, so p0 can be run
and exercised on its own without touching the shared gateway file.

Routes are identical to the gateway's (the routers carry their own prefixes),
so ``/p0/cdm/...`` paths are the same either way.

Run:
    export MASTER_KEY=...  PYTHONPATH=$PWD
    uvicorn p0.dev_server:app --host 0.0.0.0 --port 8000
Then open http://localhost:8000/docs
"""

from __future__ import annotations

from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from utility.middleware import plant_code_ctx
from utility.secret_manager import load_secrets

load_secrets()

# Only after secrets/config are loaded:
from p0.api.main import router as cdmApp
from p0.Login.main import router as loginApp
from p0.dataquality.app import router as dataQualityApp


app = FastAPI(
    title="p0 — CDM Admin API (dev)",
    swagger_ui_parameters={"withCredentials": True},
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Same {success, message, errors[]} envelope the gateway returns."""
    errors = [
        {
            "field": error["loc"][-1],
            "message": error["msg"].replace("Value error, ", ""),
        }
        for error in exc.errors()
    ]
    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={"success": False, "message": "Validation failed", "errors": errors},
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def plant_code_middleware(request: Request, call_next):
    """Seed plant_code_ctx from the query string with a per-request reset boundary."""
    token = plant_code_ctx.set(request.query_params.get("plant_code_id"))
    try:
        return await call_next(request)
    finally:
        plant_code_ctx.reset(token)


app.include_router(loginApp)
app.include_router(cdmApp)
app.include_router(dataQualityApp)
