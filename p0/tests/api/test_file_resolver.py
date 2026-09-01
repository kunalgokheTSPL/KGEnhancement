"""
GET /connectors/file?ref=s3://...

What we lock down:
  • The endpoint requires an ``s3://`` URI; anything else is 400.
  • The bucket portion is checked against the configured RustFS bucket so
    the endpoint can't be turned into an arbitrary-S3 prober.
  • A valid ``s3://<bucket>/<key>`` resolves to a 302 redirect — either to
    a presigned URL (when boto3 is installed and credentials work) or to
    the raw HTTP form ``{RUSTFS_ENDPOINT}/<bucket>/<key>`` as a fallback.
    Both shapes are acceptable for our use case — what matters is that the
    browser ends up at a URL that points at the same object.
"""

from __future__ import annotations


import pytest

from p0.drivers.database_driver import deployment_mode

from p0.tests._p0_test_base import client
from p0.api.config import RUSTFS_BUCKET, RUSTFS_ENDPOINT

# The /connectors/file resolver returns RustFS presigned / HTTP URLs (an S3/RustFS
# concept). ADLS uses SAS tokens, so this endpoint is on-prem-only until that path is
# implemented — skip on azure rather than fail on a not-yet-supported mode.
pytestmark = pytest.mark.skipif(
    deployment_mode() != "onprem",
    reason="RustFS-specific file resolver; ADLS SAS path not implemented yet",
)


def _do_get(client, ref: str):
    return client.get(
        f"/connectors/file?ref={ref}",
        follow_redirects=False,
    )


def test_resolver_rejects_non_s3(client):
    r = _do_get(client, "https://example.com/foo.pdf")
    assert r.status_code == 400


def test_resolver_rejects_missing_ref(client):
    r = client.get("/connectors/file", follow_redirects=False)
    assert r.status_code == 422


def test_resolver_rejects_s3_without_key(client):
    r = _do_get(client, f"s3://{RUSTFS_BUCKET}")
    assert r.status_code == 400


def test_resolver_rejects_other_buckets(client):
    """Bucket guard — the endpoint must not become a generic S3 proxy."""
    r = _do_get(client, "s3://some-other-bucket/whatever.pdf")
    assert r.status_code == 403


def test_resolver_redirects_for_valid_ref(client):
    """Valid s3:// URI under the configured bucket → 302 to an HTTP URL
    that addresses the same object."""
    r = _do_get(
        client, f"s3://{RUSTFS_BUCKET}/staging-pt/documents/plant_1_testcase/sop/test.pdf"
    )
    assert r.status_code == 302

    target = r.headers["location"]
    assert RUSTFS_BUCKET in target
    assert "staging-pt/documents/plant_1_testcase/sop/test.pdf" in target


def test_resolver_redirect_uses_configured_endpoint_on_fallback(client, monkeypatch):
    """When boto3 presigning fails, the endpoint falls back to a plain
    ``{RUSTFS_ENDPOINT}/<bucket>/<key>`` URL. We force the failure path
    by patching the boto3 import; the redirect target must then begin
    with the configured RUSTFS_ENDPOINT."""
    import p0.api.routers.connectors as conn_mod

    monkeypatch.setattr(
        conn_mod,
        "_log",
        type(
            "Q",
            (),
            {
                "warning": lambda *a, **kw: None,
                "info": lambda *a, **kw: None,
                "debug": lambda *a, **kw: None,
                "exception": lambda *a, **kw: None,
            },
        )(),
    )

    def _fail_client(*_a, **_kw):
        raise RuntimeError("simulated presign failure")

    monkeypatch.setattr(
        "p0.drivers.database_driver.get_rustfs_client",
        _fail_client,
    )

    r = _do_get(
        client, f"s3://{RUSTFS_BUCKET}/staging-pt/documents/plant_1_testcase/sop/test.pdf"
    )
    assert r.status_code == 302
    target = r.headers["location"]
    assert target.startswith(RUSTFS_ENDPOINT.rstrip("/"))
    assert "/staging-pt/documents/plant_1_testcase/sop/test.pdf" in target
