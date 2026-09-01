from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher
from typing import Iterable

import pandas as pd

from p0.utils import fs as _fs

_log = logging.getLogger("p0.ts_asset_identity")


def _norm_text(value: object) -> str:
    s = "" if value is None else str(value)
    s = s.strip().upper()
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


_DEFAULT_POLICY = {
    "separator_insensitive": True,
    "candidate_columns": [
        "normalized_asset",
        "equipment_tag",
        "equipment_id",
        "functional_location",
        "floc",
        "canonical_code",
    ],
    "equipment_id_columns": [
        "equipment_id",
        "equipment_tag",
        "normalized_asset",
        "sap_equipment_id",
    ],
    "identity_column": "normalized_asset",
    "fuzzy_threshold": 0.85,
    "min_match_key_length": 3,
    "attribute_separators": [".", ":"],
    "path_separators": ["!", "\\", "/"],
    "duplicate_suffix": "#",
    "aggregate_suffixes": ["ENERGY", "COUNT", "COUNTER", "TOTAL", "TOT"],
    "instrument_suffixes": ["IZ", "SZ", "FZ", "LZ", "TZ", "VZ", "ZZ", "PZ"],
}


def matching_policy(identity_cfg: dict | None, dataset_name: str = "timeseries") -> dict:
    """Read the cross reference policy out of the identity template."""
    policy = {k: (list(v) if isinstance(v, list) else v) for k, v in _DEFAULT_POLICY.items()}
    cfg = identity_cfg or {}
    normalization = cfg.get("normalization") or {}
    if "separator_insensitive" in normalization:
        policy["separator_insensitive"] = bool(normalization["separator_insensitive"])
    xref = (cfg.get("cross_reference") or {}).get(dataset_name) or {}
    if not xref:
        _log.warning(
            "[ts] identity template declares no cross_reference for %s — "
            "falling back to built in matching policy",
            dataset_name,
        )
    for key in (
        "candidate_columns",
        "equipment_id_columns",
        "identity_column",
        "attribute_separators",
        "path_separators",
        "duplicate_suffix",
        "aggregate_suffixes",
        "instrument_suffixes",
    ):
        if xref.get(key):
            policy[key] = xref[key]
    if xref.get("min_match_key_length") is not None:
        policy["min_match_key_length"] = int(xref["min_match_key_length"])
    if xref.get("fuzzy_threshold") is not None:
        policy["fuzzy_threshold"] = float(xref["fuzzy_threshold"])
    return policy


def _match_key(value: object, separator_insensitive: bool = True) -> str:
    """Join key that optionally ignores separators, so K2410A and K-2410A are one equipment."""
    s = "" if value is None else str(value)
    s = s.strip().upper()
    if separator_insensitive:
        return re.sub(r"[^A-Z0-9]+", "", s)
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9]+", " ", s)).strip()


_FUZZY_CANDIDATE_CAP = 2000

_NULL_TOKENS = frozenset({"", "nan", "none", "nat", "null", "<na>", "n/a", "na"})


def _is_null_token(value: object) -> bool:
    """True for a tag that carries no name, however the source spelled its blank."""
    if value is None:
        return True
    if isinstance(value, float) and value != value:
        return True
    return str(value).strip().lower() in _NULL_TOKENS


def _suffix_pattern(suffixes: list, allow_digits: bool) -> str:
    """One anchored alternation over the suffixes the template declares."""
    tail = r"\d*" if allow_digits else ""
    parts = [re.escape(str(x).upper()) + tail for x in suffixes if str(x).strip()]
    return r"(?:" + "|".join(parts) + r")$"


def tag_asset_part(tag_name: object, policy: dict | None = None) -> str:
    """The part of a historian tag that names the asset, before its attribute."""
    policy = policy or _DEFAULT_POLICY
    if _is_null_token(tag_name):
        return ""
    raw = str(tag_name).strip()
    if not raw:
        return ""
    dup = str(policy.get("duplicate_suffix") or "")
    if dup and dup in raw:
        raw = raw.split(dup, 1)[0]
    for sep in policy.get("path_separators") or []:
        if sep and sep in raw:
            raw = raw.rsplit(sep, 1)[-1]
    for sep in policy.get("attribute_separators") or []:
        if sep and sep in raw:
            raw = raw.split(sep, 1)[0]
    return raw.strip()


def _extract_tag_asset_key(tag_name: object, policy: dict | None = None) -> str:
    """Convert a raw TS tag to a stable asset-like key candidate."""
    policy = policy or _DEFAULT_POLICY
    if _is_null_token(tag_name):
        return ""
    asset_part = tag_asset_part(tag_name, policy)
    tag = _norm_text(asset_part) or _norm_text(tag_name)
    if not tag:
        return ""

    aggregates = policy.get("aggregate_suffixes") or []
    if aggregates:
        tag = re.sub(_suffix_pattern(aggregates, False), "", tag).strip()

    upper = asset_part.upper()
    primary = re.split(r"[_\s\-]+", upper)[0]
    primary = re.sub(r"[^A-Z0-9]", "", primary)

    if aggregates:
        primary = re.sub(_suffix_pattern(aggregates, False), "", primary)

    instruments = policy.get("instrument_suffixes") or []
    if instruments:
        primary = re.sub(_suffix_pattern(instruments, True), "", primary)

    candidates = re.findall(r"[0-9]+[A-Z]+[A-Z0-9]*|[A-Z]+[0-9]+[A-Z0-9]*", primary)
    if candidates:
        return candidates[0]

    whole = re.sub(r"[^A-Z0-9]", "", upper)
    minimum = int(policy.get("min_match_key_length") or 3)
    if len(whole) >= minimum:
        return whole

    chunks = re.findall(r"[A-Z0-9]+", tag)
    return chunks[0] if chunks else ""


def _build_reference_assets(
    reference_df: pd.DataFrame, policy: dict | None = None
) -> pd.DataFrame:
    """Build a lookup table from the P&ID entity reference the pnid stage writes."""
    policy = policy or _DEFAULT_POLICY
    separator_insensitive = bool(policy.get("separator_insensitive", True))
    rows: list[dict] = []
    if reference_df is None or reference_df.empty:
        return pd.DataFrame(
            columns=[
                "asset_key",
                "match_key",
                "normalized_asset",
                "identity_asset",
                "equipment_id",
                "source",
            ]
        )

    possible_asset_cols = list(policy.get("candidate_columns") or [])
    possible_eq_cols = list(policy.get("equipment_id_columns") or [])

    available_asset_cols = [c for c in possible_asset_cols if c in reference_df.columns]
    if not available_asset_cols:
        return pd.DataFrame(
            columns=["asset_key", "normalized_asset", "equipment_id", "source"]
        )

    available_eq_cols = [c for c in possible_eq_cols if c in reference_df.columns]
    if len(available_eq_cols) > 1:
        populated = [
            c
            for c in available_eq_cols
            if reference_df[c].astype("string").fillna("").str.strip().ne("").any()
        ]
        if populated and populated[0] != available_eq_cols[0]:
            _log.warning(
                "[ts] P&ID reference column %r is empty — using %r for equipment_id",
                available_eq_cols[0],
                populated[0],
            )
        available_eq_cols = populated or available_eq_cols
    identity_col = policy.get("identity_column")
    has_identity_col = bool(identity_col) and identity_col in reference_df.columns

    for _, row in reference_df.iterrows():
        equipment_id = ""
        for _eq_c in available_eq_cols:
            _v = row.get(_eq_c)
            _v = str(_v).strip() if pd.notna(_v) else ""
            if _v:
                equipment_id = _v
                break
        identity_raw = row.get(identity_col) if has_identity_col else None
        identity_asset = (
            str(identity_raw).strip() if pd.notna(identity_raw) and identity_raw is not None else ""
        )
        for c in available_asset_cols:
            raw_val = row.get(c)
            asset_val = str(raw_val).strip() if pd.notna(raw_val) else ""
            if not asset_val:
                continue
            rows.append(
                {
                    "asset_key": _norm_text(asset_val),
                    "match_key": _match_key(asset_val, separator_insensitive),
                    "normalized_asset": asset_val,
                    "identity_asset": identity_asset or asset_val,
                    "equipment_id": equipment_id,
                    "source": c,
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "asset_key",
                "match_key",
                "normalized_asset",
                "identity_asset",
                "equipment_id",
                "source",
            ]
        )

    ref = pd.DataFrame(rows)
    ref = ref[ref["asset_key"] != ""].drop_duplicates(
        subset=["asset_key", "normalized_asset", "equipment_id"]
    )
    return ref


def _lookup_by_match_key(
    value: object, ref_df: pd.DataFrame, policy: dict | None = None
) -> tuple[str, str]:
    """Resolve a value to the reference's own spelling under the template policy."""
    policy = policy or _DEFAULT_POLICY
    key = _match_key(value, bool(policy.get("separator_insensitive", True)))
    if not key or ref_df is None or ref_df.empty or "match_key" not in ref_df.columns:
        return "", ""
    hit = ref_df[ref_df["match_key"] == key]
    if hit.empty:
        return "", ""
    if "source" in hit.columns:
        identity = hit[hit["source"] == policy.get("identity_column")]
        if not identity.empty:
            hit = identity
    r = hit.iloc[0]
    identity = r["identity_asset"] if "identity_asset" in hit.columns else r["normalized_asset"]
    return str(identity), str(r["equipment_id"])


def _load_reference_df(reference_candidates: Iterable[str]) -> pd.DataFrame:
    """Read the P&ID entity reference, which the pipeline writes as parquet."""
    tried = []
    for p in reference_candidates:
        if not p:
            continue
        tried.append(p)
        if not _fs.exists(p):
            continue
        try:
            if str(p).lower().endswith(".parquet"):
                df = _fs.read_parquet(p)
            else:
                df = _fs.read_csv(p, low_memory=False)
        except Exception as exc:
            _log.warning("[ts] P&ID reference %s could not be read: %s", p, exc)
            continue
        if df.empty:
            _log.warning(
                "[ts] P&ID reference %s has no rows — trying the next candidate", p
            )
            continue
        return df
    if tried:
        _log.warning(
            "[ts] no P&ID equipment reference loaded from %s — "
            "tags will not be linked to equipment",
            ", ".join(tried),
        )
    return pd.DataFrame()


def _numeric_parts(key: object) -> tuple[str, ...]:
    """The numeric runs of a tag, leading zeros dropped, in the order they appear."""
    digits = re.findall(r"\d+", str(key).upper())
    return tuple(d.lstrip("0") or "0" for d in digits)


def _same_asset_number(tag_key: str, candidate: str) -> bool:
    """Two tags can only name one asset when every number in them agrees."""
    return _numeric_parts(tag_key) == _numeric_parts(candidate)


def _sibling_suffix(key: object) -> str:
    """The letters trailing the last digit, which name a sibling rather than a type."""
    text = str(key).upper()
    if not re.search(r"[0-9]", text):
        return ""
    return re.sub(r"^.*[0-9]", "", text)


def _same_asset_identity(tag_key: str, candidate: str) -> bool:
    """One asset only when both the numbers and the sibling letters agree."""
    if not _same_asset_number(tag_key, candidate):
        return False
    return _sibling_suffix(tag_key) == _sibling_suffix(candidate)


def _pick_best_reference(
    tag_key: str, ref_df: pd.DataFrame, min_key_length: int = 3
) -> tuple[str, str, float, str]:
    """
    Returns (normalized_asset, equipment_id, confidence, method).
    """
    if not tag_key or ref_df is None or ref_df.empty:
        return "", "", 0.0, "none"

    key_col = "match_key" if "match_key" in ref_df.columns else "asset_key"

    exact = ref_df[ref_df[key_col] == tag_key]
    if not exact.empty:
        r = exact.iloc[0]
        return str(r["normalized_asset"]), str(r["equipment_id"]), 1.0, "exact"

    if len(tag_key) < min_key_length:
        return "", "", 0.0, "key_too_short"

    def _substantial(other: object) -> bool:
        other = str(other)
        if len(other) < min_key_length:
            return False
        if not (tag_key in other or other in tag_key):
            return False
        return _same_asset_identity(tag_key, other)

    contains = ref_df[ref_df[key_col].apply(_substantial)]
    if not contains.empty:
        contains = contains.copy()
        contains["_gap"] = (contains[key_col].str.len() - len(tag_key)).abs()
        r = contains.sort_values(["_gap", key_col]).iloc[0]
        return str(r["normalized_asset"]), str(r["equipment_id"]), 0.92, "contains"

    cand_df = ref_df
    if len(tag_key) >= 3:
        narrowed = ref_df[ref_df[key_col].str.startswith(tag_key[:3])]
        if not narrowed.empty:
            cand_df = narrowed

    cand_df = cand_df[cand_df[key_col].apply(lambda o: _same_asset_identity(tag_key, o))]
    if cand_df.empty:
        return "", "", 0.0, "none"

    if len(cand_df) > _FUZZY_CANDIDATE_CAP:
        _log.warning(
            "[ts] %r has %d candidates; scoring only the first %d",
            tag_key,
            len(cand_df),
            _FUZZY_CANDIDATE_CAP,
        )
        cand_df = cand_df.head(_FUZZY_CANDIDATE_CAP)

    best_score = 0.0
    best_row = None
    for _, r in cand_df.iterrows():
        score = SequenceMatcher(None, tag_key, str(r[key_col])).ratio()
        if score > best_score:
            best_score = score
            best_row = r

    if best_row is not None and best_score >= 0.84:
        return (
            str(best_row["normalized_asset"]),
            str(best_row["equipment_id"]),
            float(best_score),
            "fuzzy",
        )

    return "", "", 0.0, "none"


def enrich_timeseries_asset_identity(
    df: pd.DataFrame,
    *,
    reference_candidates: list[str] | None = None,
    identity_cfg: dict | None = None,
    dataset_name: str = "timeseries",
) -> pd.DataFrame:
    """Resolve ``normalized_asset`` and ``equipment_id`` for TS tags using tag naming convention"""
    if df is None or df.empty:
        return df

    out = df.copy()

    if "tag_name" not in out.columns:
        if "tagname" in out.columns:
            out["tag_name"] = out["tagname"]
        else:
            out["tag_name"] = ""

    if reference_candidates is None:
        reference_candidates = []

    policy = matching_policy(identity_cfg, dataset_name)
    reference_df = _load_reference_df(reference_candidates)
    ref_assets = _build_reference_assets(reference_df, policy)
    has_reference = not ref_assets.empty
    if not has_reference and reference_df is not None and reference_df.empty:
        _log.warning(
            "[ts] the P&ID equipment reference is present but empty — "
            "the pnid stage wrote no equipment, so no tag will be linked"
        )
    elif not has_reference:
        _log.warning(
            "[ts] the P&ID equipment reference has none of the columns %s — "
            "no tag will be linked",
            policy.get("candidate_columns"),
        )

    if "normalized_asset" not in out.columns:
        out["normalized_asset"] = ""
    if "equipment_id" not in out.columns:
        out["equipment_id"] = ""

    methods: list[str] = []
    confidences: list[float] = []
    keys: list[str] = []

    for idx, row in out.iterrows():
        tag_name = row.get("tag_name", "")
        tag_key = _extract_tag_asset_key(tag_name)
        keys.append(tag_key)

        source_asset = str(row.get("normalized_asset") or "").strip()
        if source_asset:
            methods.append("source_asset")
            confidences.append(0.90)
        elif tag_key:
            out.at[idx, "normalized_asset"] = tag_key
            methods.append("tag_pattern")
            confidences.append(0.90)
        else:
            out.at[idx, "normalized_asset"] = str(tag_name).strip()
            methods.append("tag_name_fallback")
            confidences.append(0.50)

        if has_reference:
            existing = str(out.at[idx, "equipment_id"] or "").strip()
            if existing:
                ref_asset, ref_eq = _lookup_by_match_key(existing, ref_assets, policy)
                if ref_eq:
                    out.at[idx, "equipment_id"] = ref_eq
                    if ref_asset:
                        out.at[idx, "normalized_asset"] = ref_asset
                    methods[-1] = (
                        "source_verified_against_pid"
                        if ref_eq == existing
                        else "source_relinked_to_pid"
                    )
                    confidences[-1] = 1.0
                else:
                    methods[-1] = "source_only_no_pid_match"
                    confidences[-1] = 0.40
            else:
                search_key = _norm_text(tag_key or tag_name)
                ref_asset, ref_eq = _lookup_by_match_key(tag_key or tag_name, ref_assets, policy)
                if ref_eq:
                    out.at[idx, "equipment_id"] = ref_eq
                    if ref_asset:
                        out.at[idx, "normalized_asset"] = ref_asset
                    methods[-1] = "pid_match_key"
                    confidences[-1] = 1.0
                else:
                    _, matched_eq, score, how = _pick_best_reference(
                        search_key,
                        ref_assets,
                        int(policy.get("min_match_key_length") or 3),
                    )
                    if matched_eq and score >= float(policy["fuzzy_threshold"]):
                        out.at[idx, "equipment_id"] = matched_eq
                        methods[-1] = f"pid_{how}"
                        confidences[-1] = score
                    else:
                        methods[-1] = f"{methods[-1]}_no_pid_match"
                        confidences[-1] = 0.40

    out["asset_match_method"] = methods
    out["asset_match_confidence"] = confidences
    out["asset_match_key"] = keys

    return out
