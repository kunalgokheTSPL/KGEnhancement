"""
utils/config.py
───────────────
Microsoft Entra ID v1 configuration.

All settings are read from environment variables.
Never hard-code secrets in source code.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    # ==============================================================
    # Microsoft Entra ID
    # ==============================================================

    entra_tenant_id: str = Field(
        default="",
        validation_alias="AZURE_VAULT_TENANT_ID",
    )

    entra_client_id: str = Field(
        default="",
        validation_alias="AZURE_VAULT_CLIENT_ID",
    )

    entra_client_secret: str = Field(
        default="",
        validation_alias="AZURE_VAULT_CLIENT_SECRET",
    )
    # entra_client_secret: str = "b4fb62b8-bc98-47a4-b654-f132360dd959",

    # ==============================================================
    # JWT
    # ==============================================================

    jwt_algorithm: str = "RS256"

    # ==============================================================
    # Microsoft Graph
    # ==============================================================

    graph_api: str = (
        "https://graph.microsoft.com/v1.0"
    )

    # ==============================================================
    # RLM License
    # ==============================================================

    rlm_server: str = ""
    rlm_product: str = ""
    rlm_version: str = "1.0"

    # ==============================================================
    # Microsoft Entra v1 endpoints
    # ==============================================================

    @property
    def authority(self) -> str:
        return (
            f"https://login.microsoftonline.com/"
            f"{self.entra_tenant_id}"
        )

    @property
    def issuer(self) -> str:
        """
        Microsoft Entra ID v1 token issuer.
        """

        return (
            f"https://sts.windows.net/"
            f"{self.entra_tenant_id}/"
        )

    @property
    def openid_configuration(self) -> str:
        """
        Entra v1 OpenID configuration.
        """

        return (
            f"{self.authority}/"
            ".well-known/openid-configuration"
        )

    @property
    def token_endpoint(self) -> str:
        """
        OAuth 2.0 v1 token endpoint.
        """

        return (
            f"{self.authority}/oauth2/token"
        )

    @property
    def authorize_endpoint(self) -> str:
        """
        OAuth 2.0 v1 authorization endpoint.
        """

        return (
            f"{self.authority}/oauth2/authorize"
        )

    @property
    def jwks_uri(self) -> str:
        """
        Microsoft Entra v1 signing keys.
        """

        return (
            f"{self.authority}/discovery/keys"
        )

    # ==============================================================
    # Pydantic Settings
    # ==============================================================

    model_config = SettingsConfigDict(
        extra="ignore"
    )


# ==============================================================
# Single shared instance
# ==============================================================

settings = Settings()