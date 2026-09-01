import os
from pathlib import Path

import pytest
from dotenv import dotenv_values
from fastapi.testclient import TestClient

from p0.Login.main import app

client = TestClient(app)

# -------------------------------------------------------------------
# GLOBALS
# -------------------------------------------------------------------
CREATED_USER_ID = None
CREATED_USER_EMAIL = None
CREATED_USER_PASSWORD = None

# -------------------------------------------------------------------
# .env.enc LOADER
#
# Decrypts the Login service .env.enc file using the MASTER_KEY
# environment variable and returns all KEY=VALUE pairs as a dict.
#
# HOW TO RUN:
#   PowerShell:
#     $env:MASTER_KEY="your-fernet-master-key-here"
#     pytest p0/Login/Test/test.py -v
#
# The Login service stores .env.enc at p0/Login/.env.enc and keeps
# the file in KEY=ENCRYPTED_VALUE format, so each value is decrypted
# individually.
# -------------------------------------------------------------------


def _load_encrypted_env(enc_file: Path | None = None) -> dict[str, str]:
    """
    Decrypt the Login service .env.enc using the Fernet key stored in the
    MASTER_KEY environment variable and return all entries as a plain dict.
    """
    try:
        from cryptography.fernet import Fernet, InvalidToken
    except ImportError as exc:
        raise RuntimeError(
            "cryptography package is required to decrypt .env.enc.\n"
            "Run: pip install cryptography"
        ) from exc

    master_key = os.environ.get("MASTER_KEY")
    if not master_key:
        raise RuntimeError(
            "MASTER_KEY environment variable is not set.\n"
            "Set it before running pytest:\n"
            "  PowerShell: $env:MASTER_KEY='your-fernet-master-key-here'"
        )

    env_path = (
        Path(enc_file) if enc_file else Path(__file__).resolve().parents[1] / ".env.enc"
    )
    if not env_path.exists():
        raise FileNotFoundError(
            f".env.enc not found at '{env_path.resolve()}'.\n"
            "Make sure the Login service encrypted env file exists."
        )

    try:
        fernet = Fernet(master_key.encode())
    except ValueError as exc:
        raise RuntimeError("MASTER_KEY is not a valid Fernet key.") from exc

    encrypted_vars = dotenv_values(env_path)
    if not encrypted_vars:
        raise RuntimeError(f".env.enc is empty at '{env_path.resolve()}'.")

    env_vars: dict[str, str] = {}
    try:
        for key, encrypted_value in encrypted_vars.items():
            if not key or not encrypted_value:
                continue
            env_vars[key] = fernet.decrypt(encrypted_value.encode()).decode()
    except InvalidToken as exc:
        raise RuntimeError(
            "Decryption failed - MASTER_KEY is incorrect or the file is corrupted."
        ) from exc

    return env_vars


# Decrypt once at module load - all tests share the same values
_ENV = _load_encrypted_env()


def _require(key: str) -> str:
    """Return a decrypted env value or raise a clear error if missing."""
    value = _ENV.get(key)
    if not value:
        raise RuntimeError(
            f"Key '{key}' not found in .env.enc.\n"
            "Check the encrypted file contains this variable."
        )
    return value


def _optional(key: str) -> str | None:
    """Return a decrypted env value when present, otherwise None."""
    value = _ENV.get(key)
    return value if value else None


# -------------------------------------------------------------------
# CONFIG
#
# Update the five URLs below to match your router's mount prefix.
# Example: if your router is mounted at "/login", use "/login/login"
# instead of just "/login".
#
# NEW_USER_PAYLOAD is the dummy account created during signup tests.
# Change username / email / password if they conflict with an
# existing user in your Keycloak instance.
#
# Several tests pass "HR" as the group name.
# Replace it if your Keycloak realm uses a different group.
# -------------------------------------------------------------------

LOGIN_URL = "/login"
SIGNUP_URL = "/signup"
ME_URL = "/me"
ADMIN_URL = "/admin"
ASSIGN_GROUP_URL = "/admin/assign-group"

# Credentials are pulled automatically from .env.enc - do not hardcode here.
# `TEST_USERNAME` / `TEST_PASSWORD` are treated as the admin user for this suite.
# `TESTU_USERNAME` / `TESTU_PASSWORD` are treated as the regular user.
ADMIN_LOGIN_PAYLOAD = {
    "username": _require("TEST_USERNAME"),
    "password": _require("TEST_PASSWORD"),
}

USER_LOGIN_PAYLOAD = {
    "username": _require("TESTU_USERNAME"),
    "password": _require("TESTU_PASSWORD"),
}

HAS_REGULAR_USER = USER_LOGIN_PAYLOAD != ADMIN_LOGIN_PAYLOAD

# Dummy account created during the signup test suite
NEW_USER_PAYLOAD = {
    "username": "pytest_user",
    "email": "pytest_user@example.com",
    "password": "Pytest@123!",
    "role": "user",
    "group": None,
}

TEST_GROUP = "HR"


# ===================================================================
# FIXTURES
# ===================================================================


@pytest.fixture(scope="session")
def admin_access_token():
    """Login as admin and return the access token."""

    response = client.post(
        LOGIN_URL,
        json=ADMIN_LOGIN_PAYLOAD,
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    print("\nADMIN LOGIN STATUS:", response.status_code)
    print("ADMIN LOGIN RESPONSE:", response.text)

    assert response.status_code == 200, f"Admin login failed: {response.text}"

    data = response.json()
    token = data.get("access_token") or data.get("data", {}).get("access_token")

    assert token is not None, "No access_token found in admin login response"
    print("\nADMIN TOKEN:", token)
    return token


@pytest.fixture(scope="session")
def user_access_token():
    """Login as regular user and return the access token."""

    if not HAS_REGULAR_USER:
        pytest.skip("Set TESTU_USERNAME and TESTU_PASSWORD for regular-user tests.")

    response = client.post(
        LOGIN_URL,
        json=USER_LOGIN_PAYLOAD,
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    print("\nUSER LOGIN STATUS:", response.status_code)
    print("USER LOGIN RESPONSE:", response.text)

    assert response.status_code == 200, f"User login failed: {response.text}"

    data = response.json()
    token = data.get("access_token") or data.get("data", {}).get("access_token")

    assert token is not None, "No access_token found in user login response"
    return token


@pytest.fixture(scope="session")
def admin_auth_headers(admin_access_token):
    return {
        "Authorization": f"Bearer {admin_access_token}",
        "Content-Type": "application/json",
        "accept": "application/json",
    }


@pytest.fixture(scope="session")
def user_auth_headers(user_access_token):
    return {
        "Authorization": f"Bearer {user_access_token}",
        "Content-Type": "application/json",
        "accept": "application/json",
    }


# ===================================================================
# LOGIN API TESTS
# ===================================================================


def test_login_success_admin():
    """Admin can log in and receives token + user profile."""

    response = client.post(
        LOGIN_URL,
        json=ADMIN_LOGIN_PAYLOAD,
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 200, response.text

    data = response.json()

    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"].lower() == "bearer"
    assert isinstance(data["expires_in"], int)
    assert isinstance(data["refresh_expires_in"], int)
    assert data["expires_in"] > 0
    assert data["refresh_expires_in"] > 0

    user = data.get("user")
    if user is not None:
        assert "id" in user
        assert "email" in user
        assert "role" in user
        assert "name" in user


def test_login_success_regular_user():
    """Regular user can log in and receives a valid token."""

    if not HAS_REGULAR_USER:
        pytest.skip("Set TESTU_USERNAME and TESTU_PASSWORD for regular-user tests.")

    response = client.post(
        LOGIN_URL,
        json=USER_LOGIN_PAYLOAD,
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 200, response.text

    data = response.json()

    assert "access_token" in data
    assert len(data["access_token"]) > 0


def test_login_wrong_password():
    """Wrong password returns 401."""

    response = client.post(
        LOGIN_URL,
        json={
            "username": ADMIN_LOGIN_PAYLOAD["username"],
            "password": "WrongPassword@999",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 401, response.text


def test_login_nonexistent_user():
    """Non-existent user returns 401."""

    response = client.post(
        LOGIN_URL,
        json={
            "username": "ghost_user_xyz@notexist.com",
            "password": "Ghost@1234!",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 401, response.text


def test_login_empty_username():
    """Empty username returns 422."""

    response = client.post(
        LOGIN_URL,
        json={
            "username": "",
            "password": "SomePass@123",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


def test_login_empty_password():
    """Empty password returns 422."""

    response = client.post(
        LOGIN_URL,
        json={
            "username": ADMIN_LOGIN_PAYLOAD["username"],
            "password": "",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


def test_login_missing_fields():
    """Missing both fields returns 422."""

    response = client.post(
        LOGIN_URL,
        json={},
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


# ===================================================================
# SIGNUP API TESTS
# ===================================================================


def test_signup_success():
    """Create a new user - returns 201 with user_id, username, email, role."""

    global CREATED_USER_ID, CREATED_USER_EMAIL, CREATED_USER_PASSWORD

    CREATED_USER_EMAIL = NEW_USER_PAYLOAD["email"]
    CREATED_USER_PASSWORD = NEW_USER_PAYLOAD["password"]

    response = client.post(
        SIGNUP_URL,
        json=NEW_USER_PAYLOAD,
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    if response.status_code == 201:
        data = response.json()

        assert "user_id" in data
        assert data["username"] == NEW_USER_PAYLOAD["username"]
        assert data["email"] == NEW_USER_PAYLOAD["email"]
        assert data["role"] == NEW_USER_PAYLOAD["role"]

        CREATED_USER_ID = data["user_id"]

    elif response.status_code == 409:
        print("User already exists from a previous run - ID capture skipped")

    else:
        pytest.fail(f"Unexpected status {response.status_code}: {response.text}")


def test_signup_with_admin_role():
    """Create a user with role=admin explicitly."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "pytest_admin_user",
            "email": "pytest_admin@example.com",
            "password": "AdminTest@1!",
            "role": "admin",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code in [201, 409], response.text

    if response.status_code == 201:
        assert response.json()["role"] == "admin"


def test_signup_with_group():
    """Create a user and assign them to a group at signup."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "pytest_grouped_user",
            "email": "pytest_group@example.com",
            "password": "Group@Test1!",
            "role": "user",
            "group": TEST_GROUP,
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code in [201, 409], response.text


def test_signup_duplicate_email():
    """Re-submitting the same email returns 409."""

    response = client.post(
        SIGNUP_URL,
        json=NEW_USER_PAYLOAD,
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 409, response.text


def test_signup_password_too_short():
    """Password < 8 chars returns 422."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "short_pass_user",
            "email": "short@example.com",
            "password": "Ab1!",
            "role": "user",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


def test_signup_password_no_uppercase():
    """Password without uppercase returns 422."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "no_upper_user",
            "email": "noupper@example.com",
            "password": "alllower@123",
            "role": "user",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


def test_signup_password_no_special_char():
    """Password without a special character returns 422."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "no_special_user",
            "email": "nospecial@example.com",
            "password": "NoSpecial123",
            "role": "user",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


def test_signup_password_no_digit():
    """Password without a digit returns 422."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "no_digit_user",
            "email": "nodigit@example.com",
            "password": "NoDigit@Pass!",
            "role": "user",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


def test_signup_username_too_short():
    """Username < 3 chars returns 422."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "ab",
            "email": "short_user@example.com",
            "password": "Valid@123",
            "role": "user",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


def test_signup_username_special_characters():
    """Username with spaces or special chars (not underscore) returns 422."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "bad user!",
            "email": "baduser@example.com",
            "password": "Valid@123",
            "role": "user",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


def test_signup_invalid_role():
    """Role not in {user, admin} returns 422."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "pytest_bad_role",
            "email": "badrole@example.com",
            "password": "Valid@123",
            "role": "superuser",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


def test_signup_invalid_email():
    """Malformed email returns 422."""

    response = client.post(
        SIGNUP_URL,
        json={
            "username": "pytest_bademail",
            "email": "not-an-email",
            "password": "Valid@123",
            "role": "user",
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 422, response.text


# ===================================================================
# /me API TESTS
# ===================================================================


def test_me_as_admin(admin_auth_headers):
    """Admin token returns correct claims including admin role."""

    response = client.get(ME_URL, headers=admin_auth_headers)

    assert response.status_code == 200, response.text

    data = response.json()

    assert "sub" in data
    assert "username" in data
    assert "roles" in data
    assert isinstance(data["roles"], list)
    assert "admin" in data["roles"]


def test_me_as_regular_user(user_auth_headers):
    """Regular user token returns correct claims from /me."""

    response = client.get(ME_URL, headers=user_auth_headers)

    assert response.status_code == 200, response.text

    data = response.json()

    assert "sub" in data
    assert "username" in data
    assert isinstance(data["roles"], list)


def test_me_without_token():
    """/me without Authorization header returns 401 or 403."""

    response = client.get(ME_URL)

    assert response.status_code in [401, 403], response.text


def test_me_with_invalid_token():
    """/me with a garbage token returns 401."""

    response = client.get(
        ME_URL,
        headers={
            "Authorization": "Bearer this.is.not.a.valid.jwt",
            "accept": "application/json",
        },
    )

    assert response.status_code == 401, response.text


def test_me_with_expired_token():
    """/me with an expired token returns 401."""

    expired_token = (
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"
        ".eyJzdWIiOiIxMjM0NTY3ODkwIiwiZXhwIjoxfQ"
        ".invalidsignature"
    )

    response = client.get(
        ME_URL,
        headers={
            "Authorization": f"Bearer {expired_token}",
            "accept": "application/json",
        },
    )

    assert response.status_code == 401, response.text


# ===================================================================
# /admin API TESTS
# ===================================================================


def test_admin_panel_as_admin(admin_auth_headers):
    """Admin role can access /admin and gets the welcome payload."""

    response = client.get(ADMIN_URL, headers=admin_auth_headers)

    assert response.status_code == 200, response.text

    data = response.json()

    assert "message" in data
    assert "admin" in data["message"].lower()
    assert "admin_user" in data
    assert "roles" in data
    assert "admin" in data["roles"]


def test_admin_panel_as_regular_user(user_auth_headers):
    """Regular user (no admin role) gets 403 on /admin."""

    response = client.get(ADMIN_URL, headers=user_auth_headers)

    assert response.status_code == 403, response.text


def test_admin_panel_without_token():
    """/admin without a token returns 401 or 403."""

    response = client.get(ADMIN_URL)

    assert response.status_code in [401, 403], response.text


# ===================================================================
# /admin/assign-group API TESTS
# ===================================================================


def test_assign_group_as_admin(admin_auth_headers):
    """Admin can assign an existing user to a group."""

    response = client.post(
        ASSIGN_GROUP_URL,
        json={
            "email": USER_LOGIN_PAYLOAD["username"],
            "group": TEST_GROUP,
        },
        headers=admin_auth_headers,
    )

    assert response.status_code in [200, 404], response.text

    if response.status_code == 200:
        data = response.json()
        assert "message" in data
        assert "assigned_by" in data


def test_assign_group_as_regular_user(user_auth_headers):
    """Regular user cannot call assign-group - expects 403."""

    response = client.post(
        ASSIGN_GROUP_URL,
        json={
            "email": USER_LOGIN_PAYLOAD["username"],
            "group": TEST_GROUP,
        },
        headers=user_auth_headers,
    )

    assert response.status_code == 403, response.text


def test_assign_group_nonexistent_user(admin_auth_headers):
    """Assigning a group to a non-existent email returns 404."""

    response = client.post(
        ASSIGN_GROUP_URL,
        json={
            "email": "ghost99999@notexist.com",
            "group": TEST_GROUP,
        },
        headers=admin_auth_headers,
    )

    assert response.status_code == 404, response.text


def test_assign_group_invalid_email(admin_auth_headers):
    """Malformed email in assign-group returns 422."""

    response = client.post(
        ASSIGN_GROUP_URL,
        json={
            "email": "not-a-valid-email",
            "group": TEST_GROUP,
        },
        headers=admin_auth_headers,
    )

    assert response.status_code == 422, response.text


def test_assign_group_without_token():
    """assign-group without a token returns 401 or 403."""

    response = client.post(
        ASSIGN_GROUP_URL,
        json={
            "email": USER_LOGIN_PAYLOAD["username"],
            "group": TEST_GROUP,
        },
    )

    assert response.status_code in [401, 403], response.text


# ===================================================================
# END-TO-END: NEWLY CREATED USER FLOW
# ===================================================================


def test_new_user_can_login_after_signup():
    """User created via /signup can immediately log in."""

    if not CREATED_USER_EMAIL:
        pytest.skip("Signup test did not create a user - skipping flow test")

    response = client.post(
        LOGIN_URL,
        json={
            "username": CREATED_USER_EMAIL,
            "password": CREATED_USER_PASSWORD,
        },
        headers={
            "Content-Type": "application/json",
            "accept": "application/json",
        },
    )

    assert response.status_code == 200, (
        f"Newly created user could not log in: {response.text}"
    )

    data = response.json()
    assert "access_token" in data
    assert len(data["access_token"]) > 0


def test_new_user_me_endpoint():
    """Newly created user's token works on /me and has no admin role."""

    if not CREATED_USER_EMAIL:
        pytest.skip("Signup test did not create a user - skipping flow test")

    login_res = client.post(
        LOGIN_URL,
        json={
            "username": CREATED_USER_EMAIL,
            "password": CREATED_USER_PASSWORD,
        },
        headers={"Content-Type": "application/json"},
    )

    assert login_res.status_code == 200

    token = login_res.json().get("access_token")
    assert token

    me_res = client.get(
        ME_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "accept": "application/json",
        },
    )

    assert me_res.status_code == 200

    data = me_res.json()
    assert "sub" in data
    assert "admin" not in data.get("roles", [])


def test_new_user_cannot_access_admin():
    """Newly created user with role=user is denied /admin (403)."""

    if not CREATED_USER_EMAIL:
        pytest.skip("Signup test did not create a user - skipping flow test")

    login_res = client.post(
        LOGIN_URL,
        json={
            "username": CREATED_USER_EMAIL,
            "password": CREATED_USER_PASSWORD,
        },
        headers={"Content-Type": "application/json"},
    )

    assert login_res.status_code == 200

    token = login_res.json().get("access_token")

    admin_res = client.get(
        ADMIN_URL,
        headers={
            "Authorization": f"Bearer {token}",
            "accept": "application/json",
        },
    )

    assert admin_res.status_code == 403
