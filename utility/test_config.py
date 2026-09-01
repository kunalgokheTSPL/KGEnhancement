from fastapi import FastAPI
from fastapi.testclient import TestClient
from app import app
import urllib.parse
from p0.Login.auth.session_manager import SessionManager

# Mock SessionManager.get_session to bypass Redis in tests
_original_get_session = SessionManager.get_session
PLANT_CODE = "Plant_1"

async def _mock_get_session(self, session_id: str):
    if session_id == "dev-session":
        return {
            "session_id": "dev-session",
            "uid": "dev-user",
            "token_id": "dev-token-id",
            "access_token": "dev-token",
            "email": "dev@example.com",
            "username": "dev-user",
            "tenant_id": "dev-tenant",
            "roles": ["admin"],
            "features": ["*"],
            "permissions": {
                PLANT_CODE: {
                    "modelfactory": ["v", "c", "m", "d"]
                }
            },
            "expires_at": 9999999999,
        }
    return await _original_get_session(self, session_id)

SessionManager.get_session = _mock_get_session


# Centralized application creation and mounting
base_client = TestClient(app)


def custom_request(method, url, *args, **kwargs):
    parsed = urllib.parse.urlparse(url)
    params = urllib.parse.parse_qs(parsed.query)
    if "plant_code_id" not in params:
        params["plant_code_id"] = [PLANT_CODE]
    new_query = urllib.parse.urlencode(params, doseq=True)
    url = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

    if "json" in kwargs and isinstance(kwargs["json"], dict):
        if "plant_code_id" not in kwargs["json"]:
            kwargs["json"]["plant_code_id"] = PLANT_CODE
    if "data" in kwargs and isinstance(kwargs["data"], dict):
        if "plant_code_id" not in kwargs["data"]:
            kwargs["data"]["plant_code_id"] = PLANT_CODE

    return base_client._old_request(method, url, *args, **kwargs)

base_client._old_request = base_client.request
base_client.request = custom_request
client = base_client

# Centralized credentials and header configurations
DEV_TOKEN = "dev-token"

def get_auth_headers():
    """
    Returns standard mock headers and cookies for authorization.
    Edit this dictionary if token, cookie, or header names change.
    """
    return {
        "Authorization": f"Bearer {DEV_TOKEN}",
        "Cookie": f"access_token={DEV_TOKEN}",
        "Content-Type": "application/json",
        "accept": "application/json",
        "X-PlantCodeId": PLANT_CODE,
    }


def assert_standard_response(response, status_code=200):
    assert response.status_code == status_code
    data = response.json()
    assert data.get("success") is True or data.get("Success") is True or data.get("statusCode") == 200
    assert any(k in data for k in ["message", "Message"])
    assert any(k in data for k in ["data", "Data"])
    return data

 
