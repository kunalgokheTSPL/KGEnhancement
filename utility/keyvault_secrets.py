"""
Fetches connection secrets from Azure Key Vault and exposes them as the same
environment variable names azure_driver.py expects (AZURE_PG_*, ADLS_*, AZURE_SQL_*).

Auth is handled by DefaultAzureCredential, which tries (in order):
  1. Environment variables (AZURE_CLIENT_ID / AZURE_TENANT_ID / AZURE_CLIENT_SECRET) - service principal
  2. Managed Identity - when running on Azure (AKS, VM, App Service)
  3. Azure CLI (`az login`) - convenient for local development

Nothing is written to disk; secrets only ever live in the process environment.
"""

from hvac.api import secrets_engines
import os
import hvac

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# The vault URL isn't a secret, so it's fine to default it here.
# Override by setting the AZURE_KEY_VAULT_URL env var if you use a different vault.


# env var name (as read by azure_driver.py) -> Key Vault secret name
# Key Vault secret names may only contain alphanumerics and hyphens (no underscores).

def load_secrets_from_azure_keyvault(vault_url: str | None = None) -> None:
    """
    Populate os.environ from Key Vault secrets. Call this before instantiating
    any driver in azure_driver.py.

    Args:
        vault_url: e.g. "https://<vault-name>.vault.azure.net/".
                   Falls back to the AZURE_KEY_VAULT_URL env var if omitted.
    """
    DEFAULT_AZURE_VAULT_URL = os.getenv("AZURE_KEY_VAULT_URL")
    vault_url = vault_url or os.getenv("AZURE_KEY_VAULT_URL") or DEFAULT_AZURE_VAULT_URL

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    secret = client.list_properties_of_secrets()

    missing = []
    for secret_property in secret:
        secret_name = secret_property.name
        try:
            secret_value = client.get_secret(secret_name).value
            os.environ[secret_name.replace("-", "_")] = secret_value
        except Exception:
            missing.append(secret_name)
    if missing:
        print(f"Warning: could not fetch {len(missing)} secret(s) from Key Vault: {', '.join(missing)}")


def load_secrets_from_onprem_keyvault(vault_url: str | None = None) -> None:
    ONPREM_VAULT_URL = os.getenv("ONPREM_VAULT_URL")
    ONPREM_VAULT_TOKEN = os.getenv("ONPREM_VAULT_TOKEN")
    
    vault_url = vault_url or ONPREM_VAULT_URL

    client = hvac.Client(
        url=vault_url,
        token=ONPREM_VAULT_TOKEN,
    )

    if not client.is_authenticated():
        raise RuntimeError("Authentication to on-prem Vault failed")


    secret = client.secrets.kv.v2.read_secret_version(
        mount_point="secret",
        path="secret",
    )

    db_config = secret["data"]["data"]

    for key, value in db_config.items():
        os.environ[key] = str(value)
