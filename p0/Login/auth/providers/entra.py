"""
Microsoft Entra ID Provider

This module replaces Keycloak authentication.

Responsibilities
----------------
1. Validate Microsoft Entra Access Token
2. Fetch Microsoft JWKS
3. Fetch User Security Groups from Microsoft Graph
4. Return normalized user information
"""

from __future__ import annotations
from typing import Dict, List
import time
import httpx
from jose import jwt
from jose.exceptions import JWTError

from p0.Login.utils.config import settings

class EntraProvider:

    def __init__(self):
        self._jwks = None
        self._jwks_expiry = 0

    # ------------------------------------------------------------------
    # JWKS
    # ------------------------------------------------------------------

    async def get_jwks(self) -> dict:

        if self._jwks and time.time() < self._jwks_expiry:
            return self._jwks

        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                settings.jwks_uri
            )

            response.raise_for_status()

        self._jwks = response.json()

        # Cache for 1 hour
        self._jwks_expiry = time.time() + 3600

        return self._jwks

    # ------------------------------------------------------------------
    # Token Validation
    # ------------------------------------------------------------------

    async def validate_access_token(
        self,
        access_token: str,
    ) -> Dict:

        try:
            header = jwt.get_unverified_header(
                access_token
            )

            kid = header["kid"]

            jwks = await self.get_jwks()

            key = next(
                (
                    key
                    for key in jwks["keys"]
                    if key["kid"] == kid
                ),
                None,
            )

            if key is None:
                raise ValueError(
                    "Matching signing key not found"
                )

            payload = jwt.decode(
                access_token,
                key,
                algorithms=["RS256"],
                issuer=settings.issuer,
                audience=settings.entra_client_id,
                options={
                    "verify_signature": False,
                    "verify_exp": True,
                    "verify_iss": True,
                    "verify_aud": False,
                },
            )

            return payload

        except Exception:
            raise

    # ------------------------------------------------------------------
    # Refresh Access Token
    # ------------------------------------------------------------------

    async def refresh_token(
        self,
        refresh_token: str,
    ) -> Dict:

        data = {
            "client_id": settings.entra_client_id,
            "client_secret": settings.entra_client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                settings.token_endpoint,
                data=data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "http://localhost:3000",
                },
            )

        if response.status_code != 200:
            try:
                error_data = response.json()
            except Exception:
                error_data = {
                    "error": response.text
                }

            raise ValueError(
                f"Entra token refresh failed: {error_data}"
            )

        token_data = response.json()

        if not token_data.get("access_token"):
            raise ValueError(
                "No access_token returned from Entra"
            )

        return token_data

    # ------------------------------------------------------------------
    # Graph
    # ------------------------------------------------------------------

    async def get_user_groups(
        self,
        access_token: str,
    ) -> List[Dict]:

        url = (
            f"{settings.graph_api}"
            "/me/memberOf/microsoft.graph.group"
            "?$select=id,displayName"
        )

        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        async with httpx.AsyncClient(
            timeout=30
        ) as client:

            response = await client.get(
                url,
                headers=headers,
            )

        response.raise_for_status()

        return response.json().get(
            "value",
            []
        )

    # ------------------------------------------------------------------
    # Build User
    # ------------------------------------------------------------------

    async def authenticate(
        self,
        access_token: str,
    ) -> Dict:

        claims = await self.validate_access_token(
            access_token
        )

        groups = await self.get_user_groups(
            access_token
        )

        return {
            "uid": claims.get("oid"),

            "sub": claims.get("oid"),

            "email": (
                claims.get("upn")
                or claims.get("unique_name")
                or claims.get("preferred_username")
            ),

            "username": claims.get("name"),

            "tenant_id": claims.get("tid"),

            "access_token": access_token,

            "token_id": claims.get("uti"),

            "expires_at": claims.get("exp"),

            "groups": groups,

            "claims": claims,
        }
    
    # ------------------------------------------------------------------
    # Get Names by Group
    # ------------------------------------------------------------------

    async def get_group_members_by_name(
        self,
        access_token: str,
        group_name: str,
    ) -> List[Dict]:
        """
        Fetch all users belonging to a specific Entra ID group by its displayName.
        """
        url = (
            f"{settings.graph_api}/groups"
            f"?$filter=displayName eq '{group_name}'"
            f"&$expand=members($select=id,displayName,userPrincipalName,mail)"
        )
 
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
 
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(url, headers=headers)
 
        response.raise_for_status()
        data = response.json()
 
        groups = data.get("value", [])
        if not groups:
            return []
 
        return groups[0].get("members", [])


entra_provider = EntraProvider()