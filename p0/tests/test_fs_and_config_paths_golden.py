"""Golden tests for the object-store path helpers (utils/fs.py) and config resolution.

The fs helpers (scheme detection, split, join, basename) route every read/write between
local disk and the object store; a regression here silently sends data to the wrong
place. config_resolver maps a bare config name to its template path. Both are pure — no
network, no cluster. (fs triggers secret load at import, so this is the secrets tier.)
"""

from __future__ import annotations

from pathlib import Path

from p0.utils.config_resolver import resolve_config_path
from p0.utils.fs import basename, is_s3, path_join, split_s3, strip_scheme

_CONFIG_DIR = Path(__file__).resolve().parents[2] / "p0" / "config"


def test_is_s3_detects_object_store_schemes():
    assert is_s3("s3://b/k")
    assert is_s3("abfs://c/k")
    assert is_s3("az://c/k")
    assert not is_s3("/local/path")
    assert not is_s3("relative/path")


def test_strip_scheme_removes_only_object_store_prefixes():
    assert strip_scheme("s3://bucket/key") == "bucket/key"
    assert strip_scheme("abfs://container/key") == "container/key"
    assert strip_scheme("/local/path") == "/local/path"


def test_split_s3_returns_bucket_and_key():
    assert split_s3("s3://bucket/some/key.csv") == ("bucket", "some/key.csv")
    assert split_s3("s3://bucket") == ("bucket", "")
    assert split_s3("/local/path") == ("", "")


def test_path_join_normalizes_object_store_paths():
    assert path_join("s3://bucket", "a", "b") == "s3://bucket/a/b"


def test_basename_handles_local_and_object_store():
    assert basename("s3://b/dir/file.csv") == "file.csv"
    assert basename("/local/dir/file.csv") == "file.csv"


def test_resolve_config_path_maps_a_known_template():
    p = resolve_config_path(str(_CONFIG_DIR), "staging_quality_gate.yaml")
    assert p.endswith("templates/_common/staging_quality_gate.yaml")
    assert Path(p).exists()


def test_resolve_config_path_passes_through_an_unmapped_name():
    p = resolve_config_path("/some/dir", "not_a_known_template.yaml")
    assert p == "/some/dir/not_a_known_template.yaml"
