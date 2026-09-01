"""Fold rows finer than one-per-tag into one column per declared code."""

from __future__ import annotations

import pandas as pd


class UnknownTransposeKeyError(ValueError):
    """A row carries a code the template declares no column for."""


class DuplicateTransposeKeyError(ValueError):
    """One group carries the same code twice, so a value would be lost."""


def _after_first_dot(value: object) -> str:
    """What follows the first dot, which is where a boundary label keeps its code."""
    text = str(value)
    return text.split(".", 1)[1] if "." in text else ""


KEY_TRANSFORMS = {
    "as_is": lambda v: str(v).strip(),
    "after_first_dot": lambda v: _after_first_dot(v).strip(),
}


def _blank(value: object) -> bool:
    """Whether a cell carries nothing worth keeping."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() in ("", "nan", "NaT", "None")


def transpose_columns(spec: dict) -> list[str]:
    """Every target column a transpose spec can produce."""
    out: list[str] = []
    for template in (spec.get("value_columns") or {}).values():
        for code in sorted(set((spec.get("key_map") or {}).values())):
            out.append(str(template).format(key=code))
    return out


def apply_transpose(df: pd.DataFrame, spec: dict | None, verbose: bool = False) -> pd.DataFrame:
    """One row per group, each row's code folded into columns of its own."""
    if df is None or df.empty or not spec:
        return df

    key_col = spec.get("key_column")
    group_by = [c for c in (spec.get("group_by") or []) if c in df.columns]
    value_columns = spec.get("value_columns") or {}
    key_map = spec.get("key_map") or {}
    transform = KEY_TRANSFORMS[spec.get("key_transform") or "as_is"]

    if not key_col or key_col not in df.columns or not group_by:
        return df

    consumed = {key_col, *value_columns}
    carried = [c for c in df.columns if c not in consumed]

    produced = transpose_columns(spec)
    rows: dict[tuple, dict] = {}
    order: list[tuple] = []

    for record in df.to_dict("records"):
        group = tuple(str(record.get(c, "")).strip() for c in group_by)
        raw_key = transform(record.get(key_col))
        code = key_map.get(raw_key)
        if code is None:
            raise UnknownTransposeKeyError(
                f"{key_col}={raw_key!r} has no column declared for it. Add it to "
                f"key_map, or the value is dropped without anyone noticing. "
                f"declared: {sorted(set(key_map))}"
            )

        row = rows.get(group)
        if row is None:
            row = {c: None for c in produced}
            for c in carried:
                row[c] = record.get(c)
            rows[group] = row
            order.append(group)
        else:
            for c in carried:
                if _blank(row.get(c)) and not _blank(record.get(c)):
                    row[c] = record.get(c)

        for source_col, target_template in value_columns.items():
            target = str(target_template).format(key=code)
            value = record.get(source_col)
            if _blank(value):
                continue
            if not _blank(row.get(target)):
                raise DuplicateTransposeKeyError(
                    f"group {group} carries {key_col}={raw_key!r} twice; "
                    f"{target} already holds a value and the second would be lost"
                )
            row[target] = value

    out = pd.DataFrame([rows[g] for g in order], columns=carried + produced)
    if verbose:
        print(f"  [transpose] {len(df)} rows -> {len(out)} rows, +{len(produced)} columns")
    return out
