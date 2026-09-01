"""Security regressions for the secret layer.

One property we want to keep (a wrong MASTER_KEY must never decrypt secrets), and one
gap we want to *catch* (secret_manager ships a hardcoded MASTER_KEY default, so repo
access alone can decrypt every env.enc). The gap is marked xfail with the exact fix —
remove the default in utility/secret_manager.py and rotate the key — so it flips to a
pass the moment that lands, and guards against the default being reintroduced later.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from cryptography.fernet import Fernet, InvalidToken

from utility import secret_manager
from utility.secret_manager import deecrypt_value


def test_a_wrong_master_key_cannot_decrypt_secrets(monkeypatch):
    # encrypt with one key, then attempt to decrypt with a different key
    token = Fernet(Fernet.generate_key()).encrypt(b"top-secret").decode()
    monkeypatch.setenv("MASTER_KEY", Fernet.generate_key().decode())
    with pytest.raises(InvalidToken):
        deecrypt_value(token)


@pytest.mark.xfail(
    reason="secret_manager.py:7,58 fall back to a hardcoded MASTER_KEY committed in "
    "the repo, so a missing key does NOT hard-fail. Remove the default (+ rotate the "
    "key) to close this; the test then passes.",
    strict=False,
)
def test_master_key_has_no_hardcoded_default():
    src = Path(secret_manager.__file__).read_text()
    # os.getenv("MASTER_KEY") must be called with no second (default) argument
    assert 'os.getenv("MASTER_KEY", ' not in src
    assert "os.getenv('MASTER_KEY', " not in src
