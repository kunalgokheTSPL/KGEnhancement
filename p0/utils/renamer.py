from __future__ import annotations
import pandas as pd
import re


def _norm(s: str) -> str:
    """Header key that ignores separator style, so Equipment_Tag matches Equipment Tag."""
    return re.sub(r"\s+", " ", re.sub(r"[_\-./]+", " ", str(s).strip().upper())).strip()


def apply_column_rename(
    df: pd.DataFrame,
    rename_cfg: dict,
    source_name: str | None = None,
    verbose: bool = True,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    sources = rename_cfg.get("sources", {}) or {}

    union = {}
    for _, src in sources.items():
        m = (src or {}).get("mappings", {}) or {}
        for k, v in m.items():
            if isinstance(k, str) and isinstance(v, str) and v.strip():
                union[_norm(k)] = v.strip()

    if source_name and source_name in sources:
        m = (sources[source_name] or {}).get("mappings", {}) or {}
        for k, v in m.items():
            if isinstance(k, str) and isinstance(v, str) and v.strip():
                union[_norm(k)] = v.strip()

    rename_pairs = {}
    for col in df.columns:
        key = _norm(col)

        if key in union:
            rename_pairs[col] = union[key]
            continue

    if not rename_pairs:
        if verbose:
            print(f"[rename] source='{source_name}': 0 columns renamed")
        return df

    priority_cfg = {}
    if source_name and source_name in sources:
        priority_cfg = (sources[source_name] or {}).get("target_priority", {}) or {}

    targets: dict[str, list[str]] = {}
    for src_col, tgt in rename_pairs.items():
        targets.setdefault(tgt, []).append(src_col)

    for tgt, src_cols in targets.items():
        if len(src_cols) < 2:
            continue
        pref = priority_cfg.get(tgt)
        if pref:
            pref_norm = [_norm(p) for p in pref]

            def _rank(c: str) -> int:
                k = _norm(c)
                return pref_norm.index(k) if k in pref_norm else len(pref_norm)

            winner = min(src_cols, key=_rank)
        else:
            winner = src_cols[0]
        for c in src_cols:
            if c != winner:
                del rename_pairs[c]
                if verbose:
                    print(
                        f"[rename] target '{tgt}': '{winner}' wins over '{c}' (dropped)"
                    )

    out = df.rename(columns=rename_pairs)
    out = out.loc[:, ~out.columns.duplicated()]
    if verbose:
        print(f"[rename] source='{source_name}': renamed {len(rename_pairs)} columns")
        print("[rename] sample:", list(rename_pairs.items())[:15])

    return out


def apply_copies(df: pd.DataFrame, copies: dict | None) -> pd.DataFrame:
    """Duplicate a canonical column into the source-specific alias a template names."""
    if df is None or df.empty or not copies:
        return df
    df = df.copy()
    for source, target in copies.items():
        if source in df.columns and target not in df.columns:
            df[target] = df[source]
    return df
