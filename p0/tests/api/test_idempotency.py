"""A replayed commit returns the original result instead of writing twice."""

from __future__ import annotations

import pathlib

import pytest

from p0.api.services import idempotency as idem

CONTEXT = (
    pathlib.Path(__file__).resolve().parents[2] / "api" / "routers" / "context.py"
).read_text()


@pytest.fixture(autouse=True)
def _clean_store():
    idem.reset()
    yield
    idem.reset()


BODY = {"plant_code_id": "BOKOR", "rows": [{"a": 1}]}
RESULT = {"success": True, "data": {"commit_id": 7}}


def test_first_use_is_not_a_replay():
    assert idem.lookup("BOKOR", "commit_docs", "key-1", BODY) == (None, None)


def test_replay_returns_the_original_result():
    idem.remember("BOKOR", "commit_docs", "key-1", BODY, RESULT)
    replayed, conflict = idem.lookup("BOKOR", "commit_docs", "key-1", BODY)
    assert replayed == RESULT
    assert conflict is None


def test_double_click_does_not_write_twice():
    """The exact scenario: two identical commits, one key."""
    replayed, _ = idem.lookup("BOKOR", "commit_docs", "key-1", BODY)
    assert replayed is None
    idem.remember("BOKOR", "commit_docs", "key-1", BODY, RESULT)
    replayed, _ = idem.lookup("BOKOR", "commit_docs", "key-1", BODY)
    assert replayed is RESULT


def test_same_key_with_a_different_body_is_a_conflict():
    idem.remember("BOKOR", "commit_docs", "key-1", BODY, RESULT)
    replayed, conflict = idem.lookup(
        "BOKOR", "commit_docs", "key-1", {"plant_code_id": "BOKOR", "rows": [{"a": 2}]}
    )
    assert replayed is None
    assert conflict == "body_mismatch"


def test_keys_are_scoped_per_plant():
    idem.remember("BOKOR", "commit_docs", "key-1", BODY, RESULT)
    assert idem.lookup("OTHER", "commit_docs", "key-1", BODY) == (None, None)


def test_keys_are_scoped_per_operation():
    idem.remember("BOKOR", "commit_docs", "key-1", BODY, RESULT)
    assert idem.lookup("BOKOR", "commit_sap", "key-1", BODY) == (None, None)


def test_no_key_means_no_idempotency():
    idem.remember("BOKOR", "commit_docs", None, BODY, RESULT)
    assert idem.lookup("BOKOR", "commit_docs", None, BODY) == (None, None)


def test_fingerprint_is_order_independent():
    assert idem.fingerprint({"a": 1, "b": 2}) == idem.fingerprint({"b": 2, "a": 1})


def test_fingerprint_differs_on_real_change():
    assert idem.fingerprint({"a": 1}) != idem.fingerprint({"a": 2})


def test_fingerprint_survives_unserialisable_payloads():
    assert idem.fingerprint({"when": object()})


def test_expired_entries_are_dropped(monkeypatch):
    idem.remember("BOKOR", "commit_docs", "key-1", BODY, RESULT)
    real_time = idem.time.time

    monkeypatch.setattr(idem.time, "time", lambda: real_time() + idem.TTL_SECONDS + 60)
    assert idem.lookup("BOKOR", "commit_docs", "key-1", BODY) == (None, None)


def test_every_commit_accepts_the_header():
    """Four per-connector commits plus the combined /context/commit."""
    assert CONTEXT.count('alias="Idempotency-Key"') == 6


def test_every_commit_checks_and_stores():
    assert CONTEXT.count("_idempotency.lookup(") == 5
    assert CONTEXT.count("_idempotency.remember(") == 5


def test_body_mismatch_returns_409():
    assert CONTEXT.count("This Idempotency-Key was already used with a different request body.") == 5
    assert "HTTPStatus.CONFLICT" in CONTEXT


def test_handler_mutating_the_body_does_not_break_the_replay():
    """Commit handlers stamp rows in place; the fingerprint must predate that."""
    body = {"plant_code_id": "BOKOR", "rows": [{"tag": "A"}]}
    taken_before = idem.fingerprint(body)

    body["rows"][0]["committed_at"] = "2026-08-12T00:00:00Z"
    idem.remember("BOKOR", "commit_ts", "key-1", body, RESULT, request_fingerprint=taken_before)

    replayed, conflict = idem.lookup(
        "BOKOR", "commit_ts", "key-1", {"plant_code_id": "BOKOR", "rows": [{"tag": "A"}]}
    )
    assert conflict is None, "an identical replay must not look like a body mismatch"
    assert replayed == RESULT


def test_a_genuinely_different_body_still_conflicts_with_a_precomputed_fingerprint():
    body = {"plant_code_id": "BOKOR", "rows": [{"tag": "A"}]}
    idem.remember(
        "BOKOR", "commit_ts", "key-2", body, RESULT,
        request_fingerprint=idem.fingerprint(body),
    )
    _, conflict = idem.lookup(
        "BOKOR", "commit_ts", "key-2", {"plant_code_id": "BOKOR", "rows": [{"tag": "B"}]}
    )
    assert conflict == "body_mismatch"


def test_every_commit_fingerprints_before_the_handler_runs():
    assert CONTEXT.count("_idem_fingerprint = _idempotency.fingerprint(body)") == 5
    assert CONTEXT.count("request_fingerprint=_idem_fingerprint") == 5
