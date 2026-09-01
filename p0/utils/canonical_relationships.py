from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.lower() in ("nan", "none", "nat"):
        return ""
    return s


def _col(df: pd.DataFrame, name: str) -> Optional[str]:
    return name if name in df.columns else None


def _build_lookup(
    entity_df: pd.DataFrame, key_cols: List[str], uid_col: str
) -> Dict[Tuple[str, ...], str]:
    if entity_df is None or entity_df.empty:
        return {}
    missing = [c for c in key_cols + [uid_col] if c not in entity_df.columns]
    if missing:
        return {}

    tmp = entity_df[key_cols + [uid_col]].dropna().copy()
    for c in key_cols:
        tmp[c] = tmp[c].astype(str).str.strip()
    tmp = tmp[tmp[key_cols].apply(lambda r: all(v != "" for v in r), axis=1)]
    return {tuple(row[key_cols]): row[uid_col] for _, row in tmp.iterrows()}


def _make_source_record_id(prefix: str, values: List[str]) -> str:
    values = [str(v).strip() for v in values]
    return f"{prefix}:{_sha1('|'.join(values))}"


def _parse_ref(ref: str) -> Optional[Tuple[str, str]]:
    if not isinstance(ref, str) or "." not in ref:
        return None
    src, col = ref.split(".", 1)
    src, col = src.strip(), col.strip()
    if not src or not col:
        return None
    return src, col


def build_asset_relationships(
    post_sources: Dict[str, pd.DataFrame],
    relationships_cfg: dict,
    entities_cfg: dict,
    schema_cfg: dict,
    entity_uid_tables: Dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Template-driven relationship builder with attribute fallback:"""
    rel_defs = (relationships_cfg or {}).get("relationships", {}) or {}
    entities_meta = (entities_cfg or {}).get("entities", {}) or {}
    schema_tables = (
        (schema_cfg or {}).get("tables") or (schema_cfg or {}).get("schema") or {}
    )

    def identity_keys(entity: str) -> List[str]:
        ent = entities_meta.get(entity, {})
        keys = ent.get("identity_keys", [])
        if not isinstance(keys, list) or not keys:
            raise ValueError(
                f"entities_frozen.yaml missing identity_keys for entity '{entity}'"
            )
        return list(dict.fromkeys(keys))

    def canonical_table(entity: str) -> str:
        ent = entities_meta.get(entity, {})
        t = ent.get("canonical_table", entity)
        return t if isinstance(t, str) and t else entity

    def uid_col(entity: str) -> str:
        table = canonical_table(entity)
        tdef = schema_tables.get(table, {})
        pk = tdef.get("primary_key")
        return pk if isinstance(pk, str) and pk else f"{entity}_uid"

    lookup_cache: Dict[str, Dict[Tuple[str, ...], str]] = {}

    def get_lookup(entity: str) -> Dict[Tuple[str, ...], str]:
        if entity in lookup_cache:
            return lookup_cache[entity]
        keys = identity_keys(entity)
        pk = uid_col(entity)
        df = entity_uid_tables.get(entity)
        lu = _build_lookup(df, keys, pk) if df is not None else {}
        lookup_cache[entity] = lu
        return lu

    def resolve_key_value(
        df: pd.DataFrame, row: pd.Series, entity: str, key: str, src_name: str
    ) -> str:
        """Resolve a key value for relationship-building."""
        if key in df.columns:
            return _norm(row.get(key))

        ent = entities_meta.get(entity, {})
        attrs = ent.get("attributes", {}) if isinstance(ent, dict) else {}
        spec = attrs.get(key) if isinstance(attrs, dict) else None
        if not isinstance(spec, dict):
            return ""

        from_list = spec.get("from", []) or []
        if not isinstance(from_list, list):
            return ""

        for ref in from_list:
            parsed = _parse_ref(ref)
            if not parsed:
                continue
            sname, scol = parsed
            if sname != src_name:
                continue
            if scol in df.columns:
                return _norm(row.get(scol))
        return ""

    now = _now_utc()
    rows: List[dict] = []

    def emit(
        plant: str,
        rel_type: str,
        parent_ref: str,
        child_ref: str,
        source_system: str,
        source_record_id: str,
        confidence: float,
    ):
        relationship_uid = (
            f"rel:{_sha1('|'.join([plant, rel_type, parent_ref, child_ref]))}"
        )
        rows.append(
            {
                "relationship_uid": relationship_uid,
                "plant_code_id": plant,
                "parent_ref": parent_ref,
                "child_ref": child_ref,
                "relationship_type": rel_type,
                "source_system": source_system,
                "source_record_id": source_record_id,
                "confidence": confidence,
                "updated_at": now,
                "is_active": 1,
            }
        )

    for rel_name, rcfg in rel_defs.items():
        if not isinstance(rcfg, dict):
            continue

        src = rcfg.get("source")
        rel_type = rcfg.get("type")
        from_entity = rcfg.get("from_entity")
        to_entity = rcfg.get("to_entity")
        dedupe_keys = rcfg.get("dedupe_keys", []) or []

        if not (src and rel_type and from_entity and to_entity):
            continue
        if src not in post_sources:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "[relationships] skip %s (%s): source '%s' not among %s",
                rel_name,
                rel_type,
                src,
                sorted(post_sources),
            )
            continue

        if from_entity not in entities_meta or to_entity not in entities_meta:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "[relationships] skip %s (%s→%s): entity not defined in entities config",
                rel_name,
                from_entity,
                to_entity,
            )
            continue

        df = post_sources[src]
        if df is None or df.empty:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "[relationships] skip %s (%s): source '%s' is present but empty",
                rel_name,
                rel_type,
                src,
            )
            continue
        df = df.copy()

        srcsys_c = _col(df, "source_system")
        srid_c = _col(df, "source_record_id")
        conf_c = _col(df, "confidence")

        dedupe_cols = [k for k in dedupe_keys if k in df.columns]
        keep = list(
            dict.fromkeys(
                dedupe_cols
                + ([srcsys_c] if srcsys_c else [])
                + ([srid_c] if srid_c else [])
                + ([conf_c] if conf_c else [])
                + ["plant_code_id"]
            )
        )
        keep = [c for c in keep if c in df.columns]
        if keep:
            df = df[keep + [c for c in df.columns if c not in keep]].copy()

        for c in df.columns:
            if df[c].dtype == object:
                df[c] = df[c].astype(str).str.strip()

        if dedupe_cols:
            df = df.drop_duplicates(subset=dedupe_cols)

        from_keys = identity_keys(from_entity)
        to_keys = identity_keys(to_entity)
        from_lu = get_lookup(from_entity)
        to_lu = get_lookup(to_entity)

        for _, r in df.iterrows():
            from_tuple = tuple(
                resolve_key_value(df, r, from_entity, k, src) for k in from_keys
            )
            to_tuple = tuple(
                resolve_key_value(df, r, to_entity, k, src) for k in to_keys
            )

            if any(v == "" for v in from_tuple) or any(v == "" for v in to_tuple):
                continue

            parent_uid = from_lu.get(from_tuple)
            child_uid = to_lu.get(to_tuple)
            if not parent_uid or not child_uid:
                continue

            plant = (
                _norm(r.get("plant_code_id", ""))
                if "plant_code_id" in df.columns
                else _norm(from_tuple[0])
            )
            if not plant:
                continue

            source_system = _norm(r.get(srcsys_c, "SAP")) if srcsys_c else "SAP"
            confidence = float(r.get(conf_c, 0.8)) if conf_c else 0.8

            source_record_id = _norm(r.get(srid_c, "")) if srid_c else ""
            if not source_record_id:
                vals = (
                    [_norm(r.get(k, "")) for k in dedupe_keys]
                    if dedupe_keys
                    else [plant, rel_name] + list(from_tuple) + list(to_tuple)
                )
                source_record_id = _make_source_record_id(f"{src}_{rel_name}", vals)

            emit(
                plant,
                rel_type,
                parent_uid,
                child_uid,
                source_system,
                source_record_id,
                confidence,
            )

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(
            columns=[
                "relationship_uid",
                "plant_code_id",
                "parent_ref",
                "child_ref",
                "relationship_type",
                "source_system",
                "source_record_id",
                "confidence",
                "updated_at",
                "is_active",
            ]
        )
    return out.drop_duplicates()
