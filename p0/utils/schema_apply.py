from __future__ import annotations
import pandas as pd


def _resolve_tables(schema_cfg: dict) -> dict:
    """Normalize schema config: accept both 'tables' and 'schema' as the top key."""
    return schema_cfg.get("tables") or schema_cfg.get("schema") or {}


def apply_schema(df: pd.DataFrame, schema_cfg: dict, table_name: str) -> pd.DataFrame:
    """
    Align dataframe to schema_frozen.yaml table definition.
    """

    if df is None:
        return None
    if df.empty:
        return df.iloc[:0].copy()

    tables = _resolve_tables(schema_cfg)
    if table_name not in tables:
        print(f"[schema] table '{table_name}' not found in schema")
        return df.copy()

    table_def = tables[table_name]
    col_defs = table_def.get("columns", {})

    schema_cols = list(col_defs.keys())

    df = df.copy()

    for col in schema_cols:
        if col not in df.columns:
            df[col] = None

    df = df[schema_cols]

    for c, t in col_defs.items():
        if "TIMESTAMP" in str(t).upper() and c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    return df


def apply_schema_keep_extra(
    df: pd.DataFrame, schema_cfg: dict, table_name: str
) -> pd.DataFrame:
    """Same as apply_schema but appends extra columns (present in df but not in schema)"""
    if df is None or df.empty:
        return df

    tables = _resolve_tables(schema_cfg)
    if table_name not in tables:
        return df

    schema_cols = list(tables[table_name].get("columns", {}).keys())
    extra_cols = [c for c in df.columns if c not in schema_cols]

    df_schema = apply_schema(df, schema_cfg, table_name)

    if extra_cols:
        df_schema = pd.concat(
            [df_schema.reset_index(drop=True), df[extra_cols].reset_index(drop=True)],
            axis=1,
        )

    return df_schema
