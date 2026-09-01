from __future__ import annotations

from typing import Dict, List, Any, Optional, Tuple
import hashlib
import pandas as pd

from p0.utils.canonical_entities import uid_prefix


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _norm(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in ("nan", "none", "nat"):
        return ""
    return s


def _parse_ref(ref: str) -> Optional[Tuple[str, str]]:
    """
    Parse "sap_task_list.task_list_id" -> ("sap_task_list", "task_list_id")
    """
    if not isinstance(ref, str) or "." not in ref:
        return None
    src, col = ref.split(".", 1)
    src = src.strip()
    col = col.strip()
    if not src or not col:
        return None
    return src, col


def _pick_first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Return first existing column from candidates (case-insensitive).
    """
    if df is None or df.empty:
        return None
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if not c:
            continue
        if c in df.columns:
            return c
        hit = lower_map.get(str(c).lower())
        if hit:
            return hit
    return None


def _ensure_primary_key(
    df: pd.DataFrame, entity_name: str, identity_keys: List[str], pk_col: str
) -> pd.DataFrame:
    """Template-driven PK generation:"""
    if df is None or df.empty:
        return df

    if pk_col not in df.columns:
        df[pk_col] = ""

    missing_mask = df[pk_col].isna() | (df[pk_col].astype(str).str.strip() == "")
    if not missing_mask.any():
        return df

    prefix = uid_prefix(entity_name)

    def make_pk(row: pd.Series) -> str:
        vals = [str(row.get(k, "")).strip() for k in identity_keys]
        return f"{prefix}:{_sha1('|'.join(vals))}"

    df.loc[missing_mask, pk_col] = df.loc[missing_mask].apply(make_pk, axis=1)
    return df


def build_asset_cross_reference(
    sap_equipment_df: pd.DataFrame,
    pid_equipment_df: pd.DataFrame,
    plant_code_id: str,
) -> pd.DataFrame:
    """Build asset_cross_reference by matching SAP equipment to P&ID tags."""
    rows: list[dict] = []

    if sap_equipment_df is None or sap_equipment_df.empty:
        return pd.DataFrame()
    if pid_equipment_df is None or pid_equipment_df.empty:
        return pd.DataFrame()

    pid_norm_map: dict[str, str] = {}
    pid_tag_map: dict[str, str] = {}

    for _, r in pid_equipment_df.iterrows():
        pid_tag = _norm(r.get("equipment_tag", r.get("normalized_asset", "")))
        norm = _norm(r.get("normalized_asset", "")).upper()
        tag = _norm(r.get("equipment_tag", "")).upper()
        if pid_tag:
            if norm:
                pid_norm_map[norm] = pid_tag
            if tag:
                pid_tag_map[tag] = pid_tag

    seen: set[tuple] = set()

    for _, r in sap_equipment_df.iterrows():
        sap_id = _norm(r.get("equipment_id", r.get("sap_equipment_id", "")))
        sap_norm = _norm(r.get("normalized_asset", "")).upper()

        if not sap_id:
            continue

        pid_tag = None
        confidence = 0.0
        method = ""

        if sap_norm and sap_norm in pid_norm_map:
            pid_tag = pid_norm_map[sap_norm]
            confidence = 1.0
            method = "exact_normalized_asset"

        elif sap_norm and sap_norm in pid_tag_map:
            pid_tag = pid_tag_map[sap_norm]
            confidence = 1.0
            method = "exact_pid_tag"

        else:
            for pid_key, p_tag in pid_norm_map.items():
                if pid_key and pid_key in sap_norm:
                    pid_tag = p_tag
                    confidence = 0.8
                    method = "contains_pid_tag"
                    break

        if pid_tag:
            key = (plant_code_id, _norm(pid_tag).upper(), sap_id.upper())
            if key not in seen:
                seen.add(key)
                rows.append(
                    {
                        "plant_code_id": plant_code_id,
                        "pid_tag": pid_tag,
                        "sap_equipment_id": sap_id,
                        "normalized_asset": sap_norm,
                        "match_method": method,
                        "confidence": confidence,
                        "source_system": "SAP",
                        "is_active": 1,
                    }
                )

    df = (
        pd.DataFrame(rows)
        if rows
        else pd.DataFrame(
            columns=[
                "plant_code_id",
                "pid_tag",
                "sap_equipment_id",
                "normalized_asset",
                "match_method",
                "confidence",
                "source_system",
                "is_active",
            ]
        )
    )
    if not df.empty:
        df = df.drop_duplicates(subset=["plant_code_id", "pid_tag", "sap_equipment_id"])
        df.insert(0, "xref_uid", range(1, len(df) + 1))
    return df.reset_index(drop=True)


def build_canonical_entities_from_sap_template(
    post_sources: Dict[str, pd.DataFrame],
    entities_cfg: dict,
    schema_cfg: dict,
    plant_code_id: str,
    primary_columns: Optional[Dict[str, set]] = None,
) -> Dict[str, pd.DataFrame]:
    """FULL template-driven canonical entity builder."""
    entities_meta = (entities_cfg or {}).get("entities", {}) or {}
    from p0.utils.schema_apply import _resolve_tables

    schema_tables = _resolve_tables(schema_cfg or {}) or {}

    out: Dict[str, pd.DataFrame] = {}

    for entity_name, ent in entities_meta.items():
        if not isinstance(ent, dict):
            continue

        canonical_table = ent.get("canonical_table", entity_name)
        if not isinstance(canonical_table, str) or not canonical_table:
            canonical_table = entity_name

        table_def = schema_tables.get(canonical_table)
        if not isinstance(table_def, dict):
            continue

        schema_cols = list((table_def.get("columns") or {}).keys())
        pk_col = table_def.get("primary_key")
        if not isinstance(pk_col, str) or not pk_col:
            pk_col = f"{entity_name}_uid"

        identity_keys = ent.get("identity_keys", [])
        if not isinstance(identity_keys, list) or not identity_keys:
            continue
        identity_keys = list(
            dict.fromkeys(identity_keys)
        )

        sources = ent.get("sources", [])
        if not isinstance(sources, list) or not sources:
            continue

        attributes = ent.get("attributes", {}) or {}
        if not isinstance(attributes, dict) or not attributes:
            continue

        src_frames: Dict[str, pd.DataFrame] = {}
        for s in sources:
            if isinstance(s, str) and s in post_sources:
                df = post_sources[s]
                if df is not None and not df.empty:
                    df = df.copy()
                    if "plant_code_id" in df.columns:
                        df["plant_code_id"] = plant_code_id
                    src_frames[s] = df

        if not src_frames:
            continue

        base_src = list(src_frames.keys())[0]
        base_df = src_frames[base_src]
        out_df = pd.DataFrame(index=base_df.index)

        required_cols = set()

        for col in schema_cols:
            spec = attributes.get(col)
            if isinstance(spec, dict):
                from_list = spec.get("from", []) or []
                required = bool(spec.get("required", False))
                if required:
                    required_cols.add(col)

                series = None
                if isinstance(from_list, list) and from_list:
                    for ref in from_list:
                        parsed = _parse_ref(ref)
                        if not parsed:
                            continue
                        src_name, src_col = parsed
                        sdf = src_frames.get(src_name)
                        if sdf is None or sdf.empty:
                            continue
                        actual = _pick_first_existing_col(sdf, [src_col])
                        if actual:
                            series = sdf[actual]
                            if len(series) != len(out_df.index):
                                series = series.reindex(out_df.index)
                            break

                out_df[col] = series if series is not None else None
            else:
                out_df[col] = None

        for k in identity_keys:
            if k not in out_df.columns:
                spec = attributes.get(k, {})
                from_list = spec.get("from", []) if isinstance(spec, dict) else []
                series = None
                if isinstance(from_list, list):
                    for ref in from_list:
                        parsed = _parse_ref(ref)
                        if not parsed:
                            continue
                        src_name, src_col = parsed
                        sdf = src_frames.get(src_name)
                        if sdf is None or sdf.empty:
                            continue
                        actual = _pick_first_existing_col(sdf, [src_col])
                        if actual:
                            series = sdf[actual]
                            if len(series) != len(out_df.index):
                                series = series.reindex(out_df.index)
                            break
                out_df[k] = series if series is not None else ""

        if "plant_code_id" in out_df.columns:
            out_df["plant_code_id"] = plant_code_id

        for c in set(identity_keys) | required_cols:
            if c in out_df.columns:
                out_df[c] = out_df[c].map(_norm)

        before_count = len(out_df)
        out_df = out_df[
            out_df[identity_keys].apply(lambda r: all(v != "" for v in r), axis=1)
        ]
        dropped = before_count - len(out_df)
        if dropped > 0:
            print(
                f"  [canonical_builder_sap] '{entity_name}': dropped {dropped} rows with blank identity keys {identity_keys}"
            )

        existing_cols = list(out_df.columns)
        allowed_primary = (primary_columns or {}).get(base_src)
        if allowed_primary is not None:
            extra_cols = [
                c
                for c in base_df.columns
                if c not in existing_cols and c in allowed_primary
            ]
        else:
            extra_cols = [c for c in base_df.columns if c not in existing_cols]
        for col in extra_cols:
            out_df[col] = base_df[col]

        out_df = out_df.drop_duplicates()

        out_df = out_df[existing_cols + extra_cols]

        out_df = _ensure_primary_key(out_df, entity_name, identity_keys, pk_col)

        if "confidence" in out_df.columns:
            out_df["confidence"] = out_df["confidence"].replace("", None).fillna(1.0)
        if "is_active" in out_df.columns:
            out_df["is_active"] = (
                out_df["is_active"].replace("", None).fillna(1).astype(int)
            )

        out[canonical_table] = out_df.reset_index(drop=True)

    return out
