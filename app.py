from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from utility.middleware import plant_code_ctx
from utility.secret_manager import load_secrets
from fastapi.exceptions import RequestValidationError, HTTPException
from fastapi.responses import JSONResponse
from http import HTTPStatus
import json

load_secrets()

# NEW: Logging framework imports (uncommented and activated)
from utility.logging import LoggingManager, ApiLoggingMiddleware, LoggingSettings
from utility.logging.error_utils import attach_error_location

from utility.logging.api.v1.logging_api import router as loggingApiApp
from p0.Login.main import router as loginApp
from p0.Login.auth.middleware import EntraAuthMiddleware
from p0.user_management.authorization.middleware import UserPermissionMiddleware
from p0.api.main import router as cdmApp
# from p0.dataquality.app import router as dataQualityApp
from p0.user_management.api.v1.user_management_api import router as userManagementApp
# from p1.agent_factory.main import router as agentFactoryApp
from p1.dashboard.app import router as dashboardApp
from p1.decision_engine.app import router as decisionModelApp
from p1.cpcp.main import router as cpcpApp
# from p1.plantgpt.app import router as plantGptApp
# from p1.plantgpt.memory import memory as plantgpt_chat_memory
# from p1.plantgpt.utils import (
#     bedrock_model as plantgpt_bedrock_model,
#     embedding_service as plantgpt_embedding_service,
#     memory_client as plantgpt_memory_client,
# )
# from p1.knowledge_graph.app import router as knowledgeGraphApp
# from p1.kpi.main import router as kpiApp
# from p1.master_agent.app import router as aiveeApp
from p1.risk_template.main import router as riskTemplateApp
# from p1.symbolic_ai.main import router as symbolicAiApp
#from p1.visuals.main import router as visualsApp
#from p1.workflow_management.main import router as workflowManagementApp
# from p1.workspace.main import router as workspaceApp
from p2.app import router as p2App
#from utility.cloud_setup import router as cloudSetup


app = FastAPI(title="DecisionOps Gateway")

# NEW: Initialize logging framework (was fully commented out)
logging_settings = LoggingSettings()
logging_manager = LoggingManager(config=logging_settings)

@app.on_event("startup")
def startup_logging():
    logging_manager.start()

@app.on_event("shutdown")
def shutdown_logging():
    logging_manager.stop()

@app.get("/")
async def root():
    return {
        "service": "DecisionOps Gateway",
        "status": "ok",
        "message": "API gateway is running",
    }


# ORIGINAL
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = []

    for error in exc.errors():
        errors.append(
            {
                "field": error["loc"][-1],
                "message": error["msg"].replace("Value error, ", ""),
            }
        )

    return JSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "success": False,
            "message": "Validation failed",
            "errors": errors,
        },
    )
# NEW: HTTPException handler
#   1. error_detail  — captures "pipeline_id '0' is not valid" into logs
#   2. original exc  — captures file/function/line from chained exception
#   3. request_body  — captures sanitized request body for 4xx failures
_SENSITIVE_FIELDS = {"password", "token", "secret", "access_token", "refresh_token", "api_key"}
# _MAX_BODY_BYTES = 5120  # 5KB cap

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    try:
        if not hasattr(request.state, "logging_context"):
            request.state.logging_context = {}

        request.state.logging_context["error_detail"] = str(exc.detail)

        # captures original exception via Python exception chaining
        # __cause__  = explicit chain: raise X from Y
        # __context__ = implicit chain: raise X inside except Y
        original_exc = exc.__cause__ or exc.__context__
        if original_exc is not None:
            attach_error_location(request, original_exc)

        # NEW: capture sanitized request body for 4xx failures only
        if 400 <= exc.status_code < 500:
            try:
                raw_body = await request.body()
                if raw_body:
                    body_data = json.loads(raw_body)
                    if isinstance(body_data, dict):
                        sanitized = {
                            k: "***" if k.lower() in _SENSITIVE_FIELDS else v
                            for k, v in body_data.items()
                        }
                        request.state.logging_context["request_body"] = json.dumps(sanitized)
            except Exception:
                pass

    except Exception:
        pass

    return JSONResponse(
        status_code=exc.status_code,
        headers=getattr(exc, "headers", None),
        content={"detail": exc.detail},
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ORIGINAL
@app.middleware("http")
async def plant_code_middleware(request: Request, call_next):
    plant_code_id = None

    # POST / PUT / PATCH -> get plant_code_id from JSON payload
    if request.method in {"POST", "PUT", "PATCH"}:
        try:
            body = await request.body()

            if body:
                payload = json.loads(body)
                plant_code_id = payload.get("plant_code_id")

        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    # GET / DELETE -> get plant_code_id from query parameters
    elif request.method in {"GET", "DELETE"}:
        plant_code_id = request.query_params.get("plant_code_id")

    token = plant_code_ctx.set(plant_code_id)

    try:
        return await call_next(request)
    finally:
        plant_code_ctx.reset(token)


@app.exception_handler(Exception)
async def global_exception_handler(
    request: Request,
    exc: Exception,
):
    attach_error_location(request, exc)  # NEW: captures file/function/line into RustFS logs
    print(f"Unhandled error: {exc}")

    return JSONResponse(
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        content={
            "success": False,
            "message": "Internal server error",
            "errors": [
                {
                    "field": "server",
                    "message": str(exc),
                }
            ],
        },
    )

app.add_middleware(ApiLoggingMiddleware, manager=logging_manager)
app.add_middleware(UserPermissionMiddleware)
app.add_middleware(
    EntraAuthMiddleware
)

# ---------------------------
# P0
# ---------------------------
app.include_router(loginApp)
app.include_router(cdmApp)
# app.include_router(dataQualityApp)
app.include_router(userManagementApp)

# NEW: logging read API activated (was commented out)
app.include_router(loggingApiApp)

# ============================================================
# PlantGPT daily maintenance: roll up every user's aged-out turns into
# day-summaries, regardless of whether that user chats again -- see
# p1/plantgpt/memory/memory.py::rollup_all_users. Runs once a day at
# 02:00 UTC, off-peak. Previously owned by plantgpt's own standalone app;
# now started/stopped alongside the gateway.
# ============================================================
# plantgpt_scheduler = BackgroundScheduler()
# plantgpt_scheduler.add_job(
#     lambda: plantgpt_chat_memory.rollup_all_users(
#         plantgpt_memory_client, plantgpt_embedding_service, plantgpt_bedrock_model
#     ),
#     CronTrigger(hour=2, minute=0, timezone="UTC"),
#     id="daily_chat_rollup",
#     replace_existing=True,
# )


# @app.on_event("startup")
# def startup_plantgpt_scheduler():
#     plantgpt_scheduler.start()


# @app.on_event("shutdown")
# def shutdown_plantgpt_scheduler():
#     plantgpt_scheduler.shutdown(wait=False)


# ---------------------------
# P1
# ---------------------------
# app.include_router(agentFactoryApp)
# app.include_router(aiveeApp)
app.include_router(dashboardApp)
app.include_router(decisionModelApp)
# app.include_router(knowledgeGraphApp)
# app.include_router(kpiApp)
app.include_router(riskTemplateApp)
# app.include_router(symbolicAiApp)
#app.include_router(visualsApp)
#app.include_router(workflowManagementApp)
# app.include_router(workspaceApp)
app.include_router(cpcpApp)
# app.include_router(plantGptApp)

# # ---------------------------
# P2
# ---------------------------
app.include_router(p2App)

#  ---------------------------
# Cloud Setup
# ---------------------------

#app.include_router(cloudSetup)
# NOTE: the Swagger file-upload picker fix lives in p0
# (p0/api/main.py::_install_binary_file_schema_fix). It patches the
# shared get_openapi on import, so this gateway needs no schema post-processing.
