"""Derive tag identity and equipment from the PI and PROTEAN exports."""

from __future__ import annotations

import pandas as pd

from p0.utils.row_identity import assign_stable_keys


def _blank(value: object) -> bool:
    """Whether a cell carries nothing worth keeping."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() in ("", "nan", "NaT", "None")


def split_hierarchy(value: object, separator: str) -> list[str]:
    """One hierarchy path as its segments, blanks removed."""
    if _blank(value):
        return []
    return [p for p in str(value).strip().split(separator) if p.strip()]


def equipment_at_depth(value: object, separator: str, depth: int) -> str | None:
    """The last segment, but only on paths deep enough to end at equipment."""
    parts = split_hierarchy(value, separator)
    return parts[-1].strip() if len(parts) >= depth else None


def equipment_at_index(value: object, separator: str, index: int) -> str | None:
    """The segment that always names the equipment, wherever the path ends."""
    parts = split_hierarchy(value, separator)
    return parts[index].strip() if len(parts) > index else None


def hierarchy_levels(value: object, separator: str, levels: list) -> dict:
    """Name every segment of a hierarchy path from the template's level list."""
    parts = split_hierarchy(value, separator)
    out: dict[str, str] = {}
    for level in levels or []:
        if not isinstance(level, dict):
            continue
        field = level.get("field")
        index = level.get("index")
        if not field or index is None:
            continue
        i = int(index)
        if 0 <= i < len(parts) and parts[i].strip():
            out[str(field)] = parts[i].strip()
    return out


def deepest_link(named: dict, preference: list) -> str:
    """The most specific level the template will accept a link at."""
    for field in preference or []:
        if named.get(str(field)):
            return str(field)
    return ""


def split_config_string(value: object, separator: str) -> tuple[str | None, str | None]:
    """A point reference as its tag id and its point attribute."""
    if _blank(value):
        return None, None
    parts = str(value).strip().split(separator)
    if len(parts) < 2:
        return str(value).strip(), None
    return parts[-2].strip() or None, parts[-1].strip() or None


def _attach_hierarchy_levels(
    frame: pd.DataFrame, spec: dict, separator: str, hierarchy_column: str
) -> pd.DataFrame:
    """Name every declared level of the path and record where the tag can link."""
    levels = spec.get("levels") or []
    if not levels or hierarchy_column not in frame.columns:
        return frame
    preference = spec.get("link_preference") or []
    confidence = spec.get("level_confidence") or {}
    named = frame[hierarchy_column].map(
        lambda v: hierarchy_levels(v, separator, levels)
    )
    for level in levels:
        if not isinstance(level, dict) or not level.get("field"):
            continue
        field = str(level["field"])
        column = f"hier_{field}"
        frame[column] = [n.get(field, "") for n in named]
    linked = [deepest_link(n, preference) for n in named]
    frame["asset_link_level"] = linked
    frame["asset_link_confidence"] = [
        float(confidence.get(lvl, 0.0)) if lvl else 0.0 for lvl in linked
    ]
    return frame


def parse_pi_tags(frame: pd.DataFrame, spec: dict) -> tuple[pd.DataFrame, dict]:
    """PI rows carrying a tag id, a point attribute, and an equipment where known."""
    if frame is None or frame.empty:
        return frame, {"rows": 0}

    out = frame.copy()
    separator = spec.get("hierarchy_separator") or "\\"
    depth = int(spec.get("equipment_at_depth") or 0)
    hierarchy_column = spec.get("hierarchy_column") or "Parent"
    config_column = spec.get("config_column") or "AttributeConfigString"
    config_separator = spec.get("config_separator") or "."

    if hierarchy_column in out.columns and depth:
        out["equipment_id"] = out[hierarchy_column].map(
            lambda v: equipment_at_depth(v, separator, depth)
        )

    if config_column in out.columns:
        split = out[config_column].map(lambda v: split_config_string(v, config_separator))
        out["pi_tag_id"] = [s[0] for s in split]
        out["pi_state"] = [s[1] for s in split]
        out["_base_key"] = [
            f"{s[0]}{config_separator}{s[1]}" if s[0] and s[1] else (s[0] or "")
            for s in split
        ]

    fallback_column = spec.get("name_fallback_column")
    if fallback_column and fallback_column in out.columns and "_base_key" in out.columns:
        recovered = 0
        for i in out.index:
            if str(out.at[i, "_base_key"] or "").strip():
                continue
            raw = "" if _blank(out.at[i, fallback_column]) else str(out.at[i, fallback_column]).strip()
            if raw:
                out.at[i, "_base_key"] = raw
                recovered += 1
        out.attrs["identity_from_name_fallback"] = recovered

    out = _attach_hierarchy_levels(out, spec, separator, hierarchy_column)

    out, report = assign_stable_keys(out, "_base_key", target_column="tag_name")
    report["equipment_resolved"] = (
        int(out["equipment_id"].notna().sum()) if "equipment_id" in out.columns else 0
    )
    report["identity_from_name_fallback"] = out.attrs.get(
        "identity_from_name_fallback", 0
    )
    if "asset_link_level" in out.columns:
        report["link_levels"] = {
            str(k): int(v) for k, v in out["asset_link_level"].value_counts().items()
        }
    return out.drop(columns=["_base_key"]), report


def parse_protean(frames: list[pd.DataFrame], spec: dict) -> tuple[pd.DataFrame, dict]:
    """PROTEAN monitored parameters from every sheet, calculations excluded."""
    usable = [f for f in (frames or []) if f is not None and not f.empty]
    if not usable:
        return pd.DataFrame(), {"rows": 0}

    frame = pd.concat(usable, ignore_index=True)
    separator = spec.get("hierarchy_separator") or "\\"
    index = int(spec.get("equipment_segment_index") or 0)
    hierarchy_column = spec.get("hierarchy_column") or "Parent"
    name_column = spec.get("name_column") or "Name"
    excluded = {str(t).strip().lower() for t in (spec.get("exclude_object_types") or [])}

    dropped = 0
    if "ObjectType" in frame.columns and excluded:
        keep = ~frame["ObjectType"].map(lambda v: str(v).strip().lower() in excluded)
        dropped = int((~keep).sum())
        frame = frame[keep].reset_index(drop=True)

    if hierarchy_column in frame.columns:
        frame["equipment_id"] = frame[hierarchy_column].map(
            lambda v: equipment_at_index(v, separator, index)
        )

    names = frame[name_column] if name_column in frame.columns else pd.Series([""] * len(frame))
    blank_rows = [
        i for i in range(len(frame))
        if _blank(frame.iloc[i].get(hierarchy_column)) and _blank(frame.iloc[i].get(name_column))
    ]
    if blank_rows:
        frame = frame.drop(frame.index[blank_rows]).reset_index(drop=True)
        names = frame[name_column] if name_column in frame.columns else pd.Series([""] * len(frame))

    owners = (
        frame["equipment_id"] if "equipment_id" in frame.columns
        else pd.Series([""] * len(frame))
    )
    frame["_base_key"] = [
        ".".join(p for p in (str(a).strip(), str(b).strip()) if p and p != "nan")
        for a, b in zip(owners, names)
    ]

    frame = _attach_hierarchy_levels(frame, spec, separator, hierarchy_column)

    frame, report = assign_stable_keys(frame, "_base_key", target_column="tag_name")
    report["analysis_rows_dropped"] = dropped
    report["blank_rows_dropped"] = len(blank_rows)
    report["equipment_resolved"] = (
        int(frame["equipment_id"].notna().sum()) if "equipment_id" in frame.columns else 0
    )
    if "asset_link_level" in frame.columns:
        report["link_levels"] = {
            str(k): int(v) for k, v in frame["asset_link_level"].value_counts().items()
        }
    return frame.drop(columns=["_base_key"]), report
