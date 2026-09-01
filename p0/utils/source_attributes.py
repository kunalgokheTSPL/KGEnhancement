"""Per-source attributes packed into one JSON column."""

from __future__ import annotations

import json

import pandas as pd

DEFAULT_PREFIXES = ["pi_", "alerts_prime_", "protean_"]


def _blank(value: object) -> bool:
    """True when a cell carries no information."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    text = str(value).strip()
    return text == "" or text.lower() in ("nan", "none", "nat", "<na>", "null")


def source_columns(columns: list, prefixes: list) -> dict:
    """Map each declared prefix to the columns it owns."""
    owned = {}
    for prefix in prefixes:
        hits = [c for c in columns if c.startswith(prefix)]
        if hits:
            owned[prefix.rstrip("_")] = hits
    return owned


def pack_source_attributes(
    df: pd.DataFrame, prefixes: list | None = None
) -> pd.Series:
    """One JSON document per row holding every source specific field it actually has."""
    prefixes = list(prefixes or DEFAULT_PREFIXES)
    owned = source_columns(list(df.columns), prefixes)
    if not owned:
        return pd.Series([""] * len(df), index=df.index, dtype="object")

    packed = []
    records = df.to_dict("records")
    for row in records:
        doc = {}
        for source, cols in owned.items():
            block = {}
            for col in cols:
                value = row.get(col)
                if _blank(value):
                    continue
                key = col[len(source) + 1 :] if col.startswith(source + "_") else col
                block[key] = str(value).strip()
            if block:
                doc[source] = block
        packed.append(json.dumps(doc, sort_keys=True) if doc else "")
    return pd.Series(packed, index=df.index, dtype="object")
