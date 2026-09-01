"""P&ID normalized-asset lookup and matching, shared by the AIF and findings pipelines."""

from __future__ import annotations

from p0.utils.asset_identity import normalize_tag


def load_pnid_normalized_asset_map(plant_code_id: str, _fs) -> dict[str, str] | None:
    from p0.api.config import rustfs_processed_pnid

    path = f"{rustfs_processed_pnid(plant_code_id)}/equipment_pid.parquet"
    if not _fs.exists(path):
        return None
    try:
        df = _fs.read_parquet(path)
    except Exception:
        return None
    if df is None or df.empty or "normalized_asset" not in df.columns:
        return {}

    pnid_map: dict[str, str] = {}
    for value in df["normalized_asset"].tolist():
        original = str(value or "").strip()
        if not original:
            continue
        key = normalize_tag(original)
        if key and key not in pnid_map:
            pnid_map[key] = original
    return pnid_map


def match_against_pnid(equipment_id, pnid_map: dict[str, str]) -> tuple[bool, str, float | str]:
    if not pnid_map:
        return False, "", float("nan")

    eq_key = normalize_tag(equipment_id)
    if not eq_key:
        return False, "", float("nan")

    original = pnid_map.get(eq_key)
    if original is not None:
        return True, original, 1.0

    return False, "", float("nan")
