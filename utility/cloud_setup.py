import os
from cryptography.fernet import Fernet

from http import HTTPStatus
from fastapi import APIRouter, Cookie
from pydantic import BaseModel, ValidationInfo, field_validator
from fastapi.responses import JSONResponse

from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
MASTER_KEY = os.getenv("MASTER_KEY")

if not MASTER_KEY:
    raise ValueError("MASTER_KEY environment variable is not set.")

cipher = Fernet(MASTER_KEY.encode())

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_FILE = BASE_DIR / ".env.enc"

router = APIRouter(prefix="/cloudSetup", tags=["Cloud-Setup Overview"])

class DeploymentModeRequest(BaseModel):
    deployment_mode: str

    @field_validator("deployment_mode")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value

class PostgresConfigRequest(BaseModel):
    host: str
    port: int
    username: str
    password: str

    @field_validator("host", "port", "username", "password")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value

class IoTDBConfigRequest(BaseModel):
    host: str
    port: int
    username: str
    password: str
    db_name: str

    @field_validator("host", "port", "username", "password", "db_name")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value

class RustFSConfigRequest(BaseModel):
    endpoint: str
    access_key: str
    secret_key: str
    bucket_name: str

    @field_validator("endpoint", "access_key", "secret_key", "bucket_name")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value


def encrypt_data(**kwargs) -> dict:
    encrypted = {}

    for key, value in kwargs.items():
        if value is not None:
            encrypted[key] = cipher.encrypt(str(value).encode()).decode()

    return encrypted


def save_to_env(**kwargs):
    env = {}

    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                env[k] = v

    env.update({k: str(v) for k, v in kwargs.items()})

    with ENV_FILE.open("w") as f:
        for k, v in env.items():
            f.write(f"{k}={v}\n")


@router.post("/encryptDeploymentMode", status_code=HTTPStatus.OK)
async def encrypt_deployment_mode(
    request: DeploymentModeRequest,
    access_token: str = Cookie(None),
):
    # -------------------------------
    # Authentication
    # -------------------------------
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {
                        "field": "access_token",
                        "message": "Authentication token is missing.",
                    }
                ],
            },
        )

    try:
        encrypted_data = encrypt_data(deployment_mode=request.deployment_mode,)

        save_to_env(DEPLOYMENT_MODE=encrypted_data["deployment_mode"],)
        return {
            "success": True,
            "message": "Deployment mode encrypted and saved successfully.",
            "data": {},
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


@router.post("/encryptPostgresConfig", status_code=HTTPStatus.OK)
async def encrypt_postgres_config(
    request: PostgresConfigRequest,
    access_token: str = Cookie(None),
):
    # -------------------------------
    # Authentication
    # -------------------------------
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {
                        "field": "access_token",
                        "message": "Authentication token is missing.",
                    }
                ],
            },
        )

    try:
        encrypted_data = encrypt_data(
            host=request.host,
            port=request.port,
            username=request.username,
            password=request.password,
            db_name=request.db_name,
        )

        save_to_env(
            POSTGRES_HOST=encrypted_data["host"],
            POSTGRES_PORT=encrypted_data["port"],
            POSTGRES_USER=encrypted_data["username"],
            POSTGRES_PASSWORD=encrypted_data["password"],
            POSTGRES_DB_NAME=encrypted_data["db_name"],
        )

        return {
            "success": True,
            "message": "PostgreSQL configuration encrypted and saved successfully.",
            "data": {},
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


@router.post("/encryptIoTDBConfig", status_code=HTTPStatus.OK)
async def encrypt_iotdb_config(
    request: IoTDBConfigRequest,
    access_token: str = Cookie(None),
):
    # -------------------------------
    # Authentication
    # -------------------------------
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {
                        "field": "access_token",
                        "message": "Authentication token is missing.",
                    }
                ],
            },
        )

    try:
        encrypted_data = encrypt_data(
            host=request.host,
            port=request.port,
            username=request.username,
            password=request.password,
            db_name=request.db_name,
        )

        save_to_env(
            IOTDB_HOST=encrypted_data["host"],
            IOTDB_PORT=encrypted_data["port"],
            IOTDB_USER=encrypted_data["username"],
            IOTDB_PASSWORD=encrypted_data["password"],
            IOTDB_DB_NAME=encrypted_data["db_name"],
        )

        return {
            "success": True,
            "message": "IoTDB configuration encrypted and saved successfully.",
            "data": {},
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


@router.post("/encryptRustFSConfig", status_code=HTTPStatus.OK)
async def encrypt_rustfs_config(
    request: RustFSConfigRequest,
    access_token: str = Cookie(None),
):
    # -------------------------------
    # Authentication
    # -------------------------------
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {
                        "field": "access_token",
                        "message": "Authentication token is missing.",
                    }
                ],
            },
        )

    try:
        encrypted_data = encrypt_data(
            endpoint=request.endpoint,
            access_key=request.access_key,
            secret_key=request.secret_key,
            bucket_name=request.bucket_name,
        )

        save_to_env(
            RUSTFS_ENDPOINT=encrypted_data["endpoint"],
            RUSTFS_ACCESS_KEY=encrypted_data["access_key"],
            RUSTFS_SECRET_KEY=encrypted_data["secret_key"],
            RUSTFS_BUCKET_NAME=encrypted_data["bucket_name"],
        )

        return {
            "success": True,
            "message": "RustFS configuration encrypted and saved successfully.",
            "data": {},
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

