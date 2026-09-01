from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Sequence

import pandas as pd

_log = logging.getLogger(__name__)


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _norm(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in ("nan", "none", "nat"):
        return ""
    return s


PREFIX_MAP = {
    "equipment": "eq",
    "functional_location": "fl",
    "process_unit": "pu",
    "work_order": "wo",
    "notification": "no",
    "task_list": "tl",
    "maintenance_plan": "mp",
    "work_center": "wc",
    "material": "mat",
    "bom_item": "bom",
    "vendor": "ven",
    "purchase_order": "po",
    "goods_receipt": "gr",
    "invoice": "inv",
    "timeseries": "ts",
    "document": "doc",
    "equipment_pid": "pid",
    "equipment_connection": "conn",
    "equipment_sap": "equ",
    "timeseries_metadata": "tim",
    "material_item": "mit",
    "asset_strategy": "ast",
    "repair_action": "rac",
    "operating_state_log": "osl",
    "inspection_record": "ins",
    "proof_test": "prt",
}


def uid_prefix(entity_name: str) -> str:
    """The uid namespace for an entity, shared by every builder that mints one."""
    prefix = PREFIX_MAP.get(entity_name)
    if prefix:
        return prefix
    fallback = str(entity_name)[:3].lower()
    _log.warning(
        "[uid] entity %r has no declared uid prefix, falling back to %r — two "
        "entities sharing a fallback will mint colliding uids",
        entity_name,
        fallback,
    )
    return fallback


def build_uid_tables_from_canonical_entities(
    canonical_entities: Dict[str, pd.DataFrame],
    entities_cfg: dict,
    schema_cfg: dict,
) -> Dict[str, pd.DataFrame]:
    """Build UID lookup tables for ALL entities (template-driven)."""
    entities_meta = (entities_cfg or {}).get("entities", {}) or {}
    schema_tables = (
        (schema_cfg or {}).get("tables") or (schema_cfg or {}).get("schema") or {}
    )

    def canonical_table(entity: str) -> str:
        ent = entities_meta.get(entity, {})
        t = ent.get("canonical_table", entity)
        return t if isinstance(t, str) and t else entity

    def identity_keys(entity: str) -> List[str]:
        ent = entities_meta.get(entity, {})
        keys = ent.get("identity_keys", [])
        if not isinstance(keys, list) or not keys:
            raise ValueError(
                f"entities_frozen.yaml missing identity_keys for entity '{entity}'"
            )
        return keys

    def primary_key_col(entity: str) -> str:
        table = canonical_table(entity)
        tdef = schema_tables.get(table, {})
        pk = tdef.get("primary_key")
        if not isinstance(pk, str) or not pk:
            return f"{entity}_uid"
        return pk

    out: Dict[str, pd.DataFrame] = {}

    for entity_name in entities_meta.keys():
        table = canonical_table(entity_name)
        df = canonical_entities.get(table)
        if df is None or df.empty:
            continue

        keys = identity_keys(entity_name)
        keys = list(dict.fromkeys(keys))
        pk = primary_key_col(entity_name)

        missing = [k for k in keys if k not in df.columns]
        if missing:
            continue

        df_dedup = df.loc[:, ~df.columns.duplicated()]
        tmp = df_dedup[keys].copy()
        for k in keys:
            tmp[k] = tmp[k].astype(str).str.strip()

        tmp = tmp[tmp[keys].apply(lambda r: all(_norm(v) != "" for v in r), axis=1)]
        tmp = tmp.drop_duplicates(subset=keys)

        prefix = uid_prefix(entity_name)

        def make_uid(row: pd.Series) -> str:
            vals = [str(row[k]).strip() for k in keys]
            return f"{prefix}:{_sha1('|'.join(vals))}"

        tmp[pk] = tmp.apply(make_uid, axis=1)
        out_cols = [pk] + [
            k for k in keys if k != pk
        ]
        out[entity_name] = tmp[out_cols].copy()

    return out


def _is_blank(series: pd.Series) -> pd.Series:
    """True where a value carries no information, whatever dtype it is stored as."""
    if series.dtype == object or str(series.dtype).startswith("str"):
        text = series.astype("string").fillna("").str.strip()
        return text.eq("") | text.str.lower().isin(
            ["nan", "none", "nat", "<na>", "null"]
        )
    return series.isna()


def natural_key(pk_col: str | Sequence[str] | None) -> list[str]:
    """The key columns as a list, however the caller spelled them."""
    if pk_col is None:
        return []
    cols = [pk_col] if isinstance(pk_col, str) else list(pk_col)
    return [str(c) for c in cols if c]


def key_series(df: pd.DataFrame, key_cols: Sequence[str]) -> pd.Series:
    """One comparable key per row, joined from every column of the natural key."""
    parts = [df[c].astype("string").fillna("").str.strip() for c in key_cols]
    joined = parts[0]
    for part in parts[1:]:
        joined = joined.str.cat(part, sep="\x1f")
    return joined


def fill_blanks_from_previous(
    df_new: pd.DataFrame, previous: pd.DataFrame, pk_col: str | Sequence[str]
) -> pd.DataFrame:
    """Carry a value forward from an earlier upload wherever this one left it blank."""
    key_cols = natural_key(pk_col)
    if df_new.empty or previous.empty or not key_cols:
        return df_new
    if any(c not in previous.columns or c not in df_new.columns for c in key_cols):
        return df_new
    prev = previous.copy()
    prev_keys = key_series(prev, key_cols)
    prev = prev[~prev_keys.duplicated()]
    prev.index = key_series(prev, key_cols)
    keys = key_series(df_new, key_cols)
    out = df_new.copy()
    for col in out.columns:
        if col in key_cols or col not in prev.columns:
            continue
        blank = _is_blank(out[col])
        if not blank.any():
            continue
        carried = keys.map(prev[col])
        fillable = blank & carried.notna().values
        if not fillable.any():
            continue
        try:
            out.loc[fillable, col] = carried[fillable].values
        except (TypeError, ValueError):
            continue
    return out


def merge_entity_file_on_disk(
    df_new: pd.DataFrame,
    filepath: str,
    pk_col: str | Sequence[str] | None,
) -> pd.DataFrame:
    """Merge ``df_new`` with an existing canonical entity file on disk (Parquet or CSV)."""
    key_cols = natural_key(pk_col)
    if not key_cols or df_new is None:
        return df_new if df_new is not None else pd.DataFrame()

    from p0.utils import fs as _fs

    if not _fs.exists(filepath):
        return df_new

    if any(c not in df_new.columns for c in key_cols):
        return df_new

    new_pks = set(key_series(df_new, key_cols))

    try:
        if filepath.endswith(".parquet"):
            existing = _fs.read_parquet(filepath)
        else:
            existing = _fs.read_csv(
                filepath, low_memory=False, dtype=str, keep_default_na=False
            )

        if existing.empty or any(c not in existing.columns for c in key_cols):
            return df_new

        existing_keys = key_series(existing, key_cols)
        overlap = existing[existing_keys.isin(new_pks)]
        if not overlap.empty:
            df_new = fill_blanks_from_previous(df_new, overlap, key_cols)

        preserved = existing[~existing_keys.isin(new_pks)]
        if preserved.empty:
            return df_new

        preserved = preserved.copy()
        for c in df_new.columns:
            if c not in preserved.columns:
                preserved[c] = None
            if preserved[c].dtype != df_new[c].dtype:
                try:
                    preserved[c] = preserved[c].astype(df_new[c].dtype)
                except (TypeError, ValueError):
                    pass

        merged = pd.concat([df_new, preserved], ignore_index=True)
        return merged
    except Exception:
        return df_new
