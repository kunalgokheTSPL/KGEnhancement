"""The P&ID reference must load from an object-store path, not only a local one."""

from __future__ import annotations

import pandas as pd

import p0.utils.fs as fs_module
import p0.utils.ts_asset_identity as ts_identity

CLOUD_REF = "abfs://container/plant/entities/equipment_pid.parquet"

_REFERENCE = pd.DataFrame(
    {
        "normalized_asset": ["K-2410A"],
        "equipment_id": ["K-2410A"],
        "asset_key": ["K-2410A"],
    }
)


def _wire_cloud(monkeypatch, seen: list[str]) -> None:
    """Make only the cloud path exist, and record what the loader asked for."""

    def fake_exists(path: str) -> bool:
        seen.append(path)
        return str(path) == CLOUD_REF

    monkeypatch.setattr(fs_module, "exists", fake_exists)
    monkeypatch.setattr(fs_module, "read_parquet", lambda p, **k: _REFERENCE.copy())


def test_a_reference_on_the_object_store_is_loaded(monkeypatch):
    seen: list[str] = []
    _wire_cloud(monkeypatch, seen)
    df = ts_identity._load_reference_df([CLOUD_REF])
    assert not df.empty
    assert list(df["equipment_id"]) == ["K-2410A"]


def test_the_loader_asks_the_filesystem_not_the_local_disk(monkeypatch):
    seen: list[str] = []
    _wire_cloud(monkeypatch, seen)
    ts_identity._load_reference_df([CLOUD_REF])
    assert seen == [CLOUD_REF]


def test_a_local_reference_still_loads(monkeypatch, tmp_path):
    local = tmp_path / "equipment_pid.parquet"
    _REFERENCE.to_parquet(local)
    df = ts_identity._load_reference_df([str(local)])
    assert list(df["equipment_id"]) == ["K-2410A"]
