"""Give every source row a stable identity, including sources with no natural key."""

from __future__ import annotations

import hashlib

import pandas as pd

SEPARATOR = "#"


def _blank(value: object) -> bool:
    """Whether a cell carries nothing worth keeping."""
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() in ("", "nan", "NaT", "None")


def _text(value: object) -> str:
    """One cell as the digest sees it."""
    return "" if _blank(value) else str(value).strip()


def content_digest(record: dict, columns: list[str]) -> str:
    """A short digest of one row's content, stable across runs and machines."""
    parts = [f"{col}={_text(record.get(col))}" for col in sorted(columns)]
    joined = "\x1f".join(parts).encode("utf-8")
    return hashlib.blake2s(joined, digest_size=4).hexdigest()


def assign_stable_keys(
    df: pd.DataFrame,
    base_column: str,
    target_column: str = "tag_name",
    discriminators: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """The base key where it is unique, and base plus a content digest where it is not."""
    if df is None or df.empty or base_column not in df.columns:
        return df, {"rows": 0, "ambiguous_bases": 0, "disambiguated": 0, "collapsed": 0}

    df = df.copy()
    bases = df[base_column].map(_text)
    counts = bases.value_counts()
    ambiguous = {b for b, n in counts.items() if n > 1 and b}

    cols = discriminators if discriminators is not None else [
        c for c in df.columns if c != target_column
    ]

    keys: list[str] = []
    digests_by_base: dict[str, set[str]] = {}
    for (_, record), base in zip(df.iterrows(), bases):
        if base and base in ambiguous:
            digest = content_digest(record.to_dict(), cols)
            digests_by_base.setdefault(base, set()).add(digest)
            keys.append(f"{base}{SEPARATOR}{digest}")
        else:
            keys.append(base)

    df[target_column] = keys

    distinct_after = sum(len(v) for v in digests_by_base.values())
    rows_ambiguous = int(sum(counts[b] for b in ambiguous))
    report = {
        "rows": int(len(df)),
        "ambiguous_bases": len(ambiguous),
        "rows_with_ambiguous_base": rows_ambiguous,
        "disambiguated": distinct_after,
        "collapsed": rows_ambiguous - distinct_after,
    }
    return df, report
