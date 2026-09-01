import asyncio

import httpx

from p0.Login.auth.providers import keycloak


def test_login_user_returns_dev_fallback_when_keycloak_is_unreachable(monkeypatch):
    class BrokenAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, *args, **kwargs):
            raise httpx.ConnectError("boom")

    monkeypatch.setattr(keycloak.httpx, "AsyncClient", BrokenAsyncClient)
    monkeypatch.setattr(keycloak, "AUTH_ENABLED", "true")
    monkeypatch.setattr(keycloak, "_get_user_by_email", lambda email: None)

    async def run_login():
        return await keycloak.login_user("dev@example.com", "secret")

    result = asyncio.run(run_login())

    assert result["access_token"] == "dev-token"
    assert result["user"]["email"] == "dev@example.com"
