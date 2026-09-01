import os
from cryptography.fernet import Fernet
from dotenv import dotenv_values


def load_secrets():
    master_key = os.getenv("MASTER_KEY", "QTPOjHIt1xLQ7hWZXHXDJsIi5U3CJfnTpg1LIygnMJs=")

    if not master_key:
        raise ValueError("MASTER_KEY not found in environment")

    cipher = Fernet(master_key)

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
    # .env.enc.local (local-dev override) is loaded FIRST so its keys win; .env.enc
    # then fills in everything it doesn't set. Delete the .local file to fall back.
    paths = [
        p
        for p in (
            os.path.join(base_dir, ".env.enc.local"),
            os.path.join(base_dir, ".env.enc"),
        )
        if os.path.exists(p)
    ]
    # Optional: env may supply everything
    if paths:
        loaded_any = False
        for path in paths:
            for key, value in (dotenv_values(path) or {}).items():
                loaded_any = True
                os.environ[key] = cipher.decrypt(value.encode()).decode()

        if not loaded_any:
            raise ValueError(".env.enc is empty or not loaded")
    else:
        print(f"[OK] no .env.enc at {base_dir} — using environment variables")

    # Check if DEPLOYMENT_MODE became azure after decrypting .env.enc
    deployment_mode = os.getenv("DEPLOYMENT_MODE").lower()
    if deployment_mode == "azure":
        from utility.keyvault_secrets import load_secrets_from_azure_keyvault
        load_secrets_from_azure_keyvault()
        print("[OK] Secrets loaded from Azure Key Vault into environment variables")
        return
    elif deployment_mode == "on_prem":
        from utility.keyvault_secrets import load_secrets_from_onprem_keyvault
        load_secrets_from_onprem_keyvault()
        print("[OK] Secrets loaded from on-prem Vault into environment variables")
        return
    else:
        print("[OK] Loaded for .env.env")
        return

    # Automap localhost to host.docker.internal inside Docker containers
    is_docker = os.path.exists("/.dockerenv")
    if not is_docker:
        try:
            with open("/proc/self/cgroup", "r") as f:
                if "docker" in f.read():
                    is_docker = True
        except:
            pass

    if is_docker:
        for env_var in ["POSTGRES_HOST", "IOTDB_HOST", "RUSTFS_HOST"]:
            val = os.environ.get(env_var)
            if val in ["localhost", "127.0.0.1"]:
                os.environ[env_var] = "host.docker.internal"

    print("[OK] Secrets loaded into environment variables")


def deecrypt_value(encrypted_value):
    master_key = os.getenv("MASTER_KEY", "QTPOjHIt1xLQ7hWZXHXDJsIi5U3CJfnTpg1LIygnMJs=")

    if not master_key:
        raise ValueError("MASTER_KEY not found")

    cipher = Fernet(master_key)
    decrypted = cipher.decrypt(encrypted_value.encode()).decode()
    return decrypted
