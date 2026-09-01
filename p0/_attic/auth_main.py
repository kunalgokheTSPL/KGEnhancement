"""
p0 Auth API — JWT-based authentication against PostgreSQL users table.

Endpoints exposed (all relative to where this app is mounted, e.g. /login):
    POST /login     — accept {username, password}, return {access_token, user}
    GET  /health    — liveness probe
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from jose import jwt
from passlib.context import CryptContext
from pydantic import BaseModel
from p0.driver import PostgresDriver

_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    load_dotenv(str(_env_path), override=False)

_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-this-before-deploying")
_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
_EXPIRE_HOURS = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_HOURS", "12"))


_pwd_ctx = CryptContext(schemes=["argon2"], deprecated="auto")

app = FastAPI(title="DecisionOps Auth", docs_url="/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class LoginRequest(BaseModel):
    username: str
    password: str


class UserProfile(BaseModel):
    id: str
    name: str
    email: str
    role: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "Bearer"
    user: UserProfile


def _get_user(email: str) -> dict | None:

    _driver = PostgresDriver()
    _driver.config["database"] = "decisionops_users"
    conn = _driver.connect(connect_timeout=5)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id::text, name, email, role, password_hash FROM users WHERE email = %s",
                (email,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


def _make_token(user: dict) -> str:
    payload = {
        "sub": user["id"],
        "email": user["email"],
        "role": user["role"],
        "exp": datetime.utcnow() + timedelta(hours=_EXPIRE_HOURS),
    }
    return jwt.encode(payload, _SECRET_KEY, algorithm=_ALGORITHM)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/login", response_model=LoginResponse)
def login(body: LoginRequest):
    try:
        user = _get_user(body.username)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database unavailable: {exc}",
        )

    if not user or not _pwd_ctx.verify(body.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    token = _make_token(user)
    return LoginResponse(
        access_token=token,
        user=UserProfile(
            id=user["id"],
            name=user["name"],
            email=user["email"],
            role=user["role"],
        ),
    )
