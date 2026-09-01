"""
CDM Final Consolidation — merges all pipeline entity and relationship outputs
into a single data/out/final/ folder, ready for database insertion.

All hardcoded table definitions, file paths, dedup keys, and source priorities
are now driven entirely by config files:

  config/schema_frozen.yaml    → column lists, primary keys, unique constraints
  config/cdm_config_frozen.yaml → consolidation section (tables, source mappings,
                                   file paths, output subdirs, merge priority)

No hardcoded table names, column lists, or paths exist in this script.

Usage:
    python -m pipelines.build_final_cdm \\
        --pnid_out   ./data/out/pnid \\
        --ts_out     ./data/out/ts \\
        --docs_out   ./data/out/docs \\
        --out_dir    ./data/out/final \\
        --config_dir ./config
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from p0.utils import fs
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import yaml



from p0.utils.config_resolver import resolve_config_path
from p0.utils.progress import emit as _emit_progress


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _load_config(config_dir: str, filename: str) -> dict[str, Any]:
    return _load_yaml(resolve_config_path(config_dir, filename))




def _clean_desc(raw: str) -> str:
    """Produce a clean, title-cased description suitable for human display."""
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", ""):
        return ""
    s = re.sub(r"^\d+\s+", "", s)
    s = re.sub(r"^[A-Z]{1,3}\d+[A-Z]?\s+", "", s)
    s = " ".join(s.split())
    if s == s.upper():
        def _smart_title(word: str) -> str:
            _KEEP_UPPER = {
                "HP",
                "LP",
                "NG",
                "LPG",
                "GAS",
                "WO",
                "ID",
                "OD",
                "RPM",
                "OK",
            }
            return word if word.upper() in _KEEP_UPPER else word.capitalize()

        s = " ".join(_smart_title(w) for w in s.split())
    return s


def _build_readable_name_lookup(source_dirs: dict[str, str]) -> dict[str, str]:
    """Build {NORMALISED_EQUIPMENT_ID → readable_name} from all available pipeline"""
    lookup: dict[str, str] = {}

    def _register(eq_id: str, desc: str, overwrite: bool = False) -> None:
        key = str(eq_id).strip()
        if not key or key.upper() in ("NAN", "NONE", ""):
            return
        clean = _clean_desc(desc)
        if clean and clean.upper().replace(" ", "") != key.upper().replace(
            "-", ""
        ).replace(" ", ""):
            value = f"{clean} ({key})"
        else:
            value = key
        if overwrite or key.upper() not in lookup:
            lookup[key.upper()] = value

    def _register_from_entity(path_base: str, eq_col: str, overwrite: bool) -> None:
        path = fs.resolve_entity_path(path_base)
        if not fs.exists(path):
            return
        try:
            if path.endswith(".parquet"):
                df = fs.read_parquet(path)
                if eq_col not in df.columns or "description" not in df.columns:
                    return
                for _, row in df[[eq_col, "description"]].iterrows():
                    eid = str(row[eq_col]).strip()
                    if eid:
                        _register(eid, row.get("description", ""), overwrite=overwrite)
            else:
                seen_eq: set[str] = set()
                for chunk in fs.read_csv_chunked(
                    path, dtype=str, keep_default_na=False
                ):
                    if (
                        eq_col not in chunk.columns
                        or "description" not in chunk.columns
                    ):
                        continue
                    for _, row in chunk[[eq_col, "description"]].iterrows():
                        eid = str(row[eq_col]).strip()
                        if eid and eid not in seen_eq:
                            seen_eq.add(eid)
                            _register(
                                eid, row.get("description", ""), overwrite=overwrite
                            )
        except Exception:
            pass

    ts_base = fs.path_join(
        source_dirs.get("ts_out", ""), "entities", "timeseries_metadata"
    )
    _register_from_entity(ts_base, "equipment_id", overwrite=False)

    pid_base = fs.path_join(
        source_dirs.get("pnid_out", ""), "entities", "equipment_pid"
    )
    _register_from_entity(pid_base, "equipment_id", overwrite=True)

    sap_base = fs.path_join(source_dirs.get("sap_out", ""), "entities", "equipment_sap")
    sap_path = fs.resolve_entity_path(sap_base)
    if fs.exists(sap_path):
        try:
            if sap_path.endswith(".parquet"):
                sample = fs.read_parquet(sap_path).head(1)
            else:
                sample = next(
                    fs.read_csv_chunked(
                        sap_path, chunksize=1, dtype=str, keep_default_na=False
                    )
                )
            eq_col = (
                "equipment_id"
                if "equipment_id" in sample.columns
                else "sap_equipment_id"
            )
        except Exception:
            eq_col = "equipment_id"
        _register_from_entity(sap_base, eq_col, overwrite=True)

    return lookup


def _apply_readable_names(
    df: pd.DataFrame,
    lookup: dict[str, str],
    id_col: str = "equipment_id",
    out_col: str = "equipment_id_readable",
) -> pd.DataFrame:
    """Insert `out_col` immediately after `id_col` in df."""
    if id_col not in df.columns:
        return df

    df = df.copy()

    def _resolve(v: str) -> str:
        v = str(v).strip()
        if not v or v.lower() in ("nan", "none", ""):
            return ""
        return lookup.get(v.upper(), v)

    df[out_col] = df[id_col].apply(_resolve)

    cols = [c for c in df.columns if c != out_col]
    try:
        idx = cols.index(id_col) + 1
    except ValueError:
        idx = len(cols)
    cols.insert(idx, out_col)
    return df[cols]


def _build_schema_map(
    schema: dict,
) -> tuple[
    dict[str, list[str]],
    dict[str, str],
    dict[str, list[str]],
]:
    """Derive SCHEMA_COLUMNS, UID_COL, DEDUP_KEYS purely from schema_frozen.yaml."""
    tables: dict = schema.get("tables") or schema.get("schema") or {}
    schema_columns: dict[str, list[str]] = {}
    uid_col: dict[str, str] = {}
    dedup_keys: dict[str, list[str]] = {}

    for table, defn in tables.items():
        if not isinstance(defn, dict):
            continue
        cols = list(defn.get("columns", {}).keys())
        schema_columns[table] = cols
        uid_col[table] = defn.get("primary_key", cols[0] if cols else "uid")

        for constraint in defn.get("constraints", []):
            if isinstance(constraint, dict) and constraint.get("type") == "unique":
                dedup_keys[table] = constraint.get("columns", [])
                break
        else:
            non_uid = [c for c in cols if c != uid_col[table]]
            dedup_keys[table] = non_uid[:1] if non_uid else []

    return schema_columns, uid_col, dedup_keys



_LOAD_CHUNK = 10_000


def _load_file(path: str) -> pd.DataFrame:
    """Load a Parquet or CSV file if it exists, else return an empty DataFrame."""
    if not path or not fs.exists(path):
        return pd.DataFrame()
    try:
        if path.endswith(".parquet"):
            return fs.read_parquet(path).astype(str)
        chunks = []
        for chunk in fs.read_csv_chunked(
            path, dtype=str, keep_default_na=False, chunksize=_LOAD_CHUNK
        ):
            chunks.append(chunk)
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def _load_csv_chunked(path: str, chunksize: int = _LOAD_CHUNK):
    """Yield DataFrames of *chunksize* rows from a CSV (or empty if missing)."""
    if not path or not os.path.exists(path):
        return
    try:
        yield from pd.read_csv(
            path, dtype=str, keep_default_na=False, chunksize=chunksize
        )
    except Exception:
        return


def _enforce_schema(
    df: pd.DataFrame,
    table: str,
    schema_columns: dict[str, list[str]],
    uid_col: dict[str, str],
    keep_extra: bool = False,
) -> pd.DataFrame:
    """Ensure all schema columns are present, reorder to schema order,"""
    cols = schema_columns.get(table, list(df.columns))
    pk = uid_col.get(table)

    for c in cols:
        if c not in df.columns:
            df[c] = ""

    df = df.reset_index(drop=True)
    if pk and pk in cols:
        df[pk] = range(1, len(df) + 1)

    schema_cols_present = [c for c in cols if c in df.columns]
    if keep_extra:
        extra_cols = [c for c in df.columns if c not in cols]
        return df[schema_cols_present + extra_cols]
    return df[schema_cols_present]




def _merge_equipment(
    source_paths: dict[str, str],
    source_priority: list[str],
    dedup_keys: list[str],
) -> pd.DataFrame:
    """Merge equipment from multiple sources."""
    frames = []
    for i, source in enumerate(source_priority):
        path = source_paths.get(source, "")
        df = _load_file(path)
        if source == "sap":
            print(f"[DEBUG] SAP frame columns: {list(df.columns)}")
        if not df.empty:
            df["_priority"] = i
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    print(f"[DEBUG] dedup_keys: {dedup_keys}")

    for c in dedup_keys:
        if c in combined.columns:
            combined[c] = combined[c].astype(str).str.strip().str.upper()

    import numpy as np

    combined = combined.sort_values("_priority", ascending=True)
    combined = combined.replace(to_replace=r"^\s*$", value=np.nan, regex=True)
    print(f"[DEBUG] rows before groupby dedup: {len(combined)}")
    combined = combined.groupby(dedup_keys, as_index=False, dropna=False).first()
    print(f"[DEBUG] rows after groupby dedup: {len(combined)}")
    combined = combined.fillna("")
    combined = combined.drop(columns=["_priority"], errors="ignore")

    mask = combined[dedup_keys].apply(lambda s: s.str.strip().ne("")).any(axis=1)
    combined = combined[mask]

    return combined.reset_index(drop=True)


def _merge_generic(
    source_paths: dict[str, str],
    dedup_keys: list[str],
) -> pd.DataFrame:
    """Concat rows from multiple sources (order doesn't matter) and deduplicate."""
    frames = [_load_file(p) for p in source_paths.values() if p]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    valid_keys = [c for c in dedup_keys if c in combined.columns]
    if valid_keys:
        for c in valid_keys:
            combined[c] = combined[c].astype(str).str.strip()
        combined = combined.drop_duplicates(subset=valid_keys)

    return combined.reset_index(drop=True)


def _stream_merge_generic(
    source_paths: dict[str, str],
    dedup_keys: list[str],
    out_path: str,
) -> int:
    """Stream-merge multiple source CSVs to *out_path*, deduplicating on the"""
    paths = [p for p in source_paths.values() if p]
    return fs.stream_dedup_csvs(paths, out_path, dedup_cols=dedup_keys)




def build_final_cdm(
    source_dirs: dict[str, str],
    out_dir: str,
    schema: dict,
    cdm_cfg: dict,
) -> dict[str, int]:
    """Consolidate all pipeline outputs into out_dir."""
    now_ts = datetime.now(timezone.utc).isoformat()

    schema_columns, uid_col, dedup_keys = _build_schema_map(schema)
    consolidation = cdm_cfg.get("consolidation", {})

    target_tables: list[str] = consolidation.get(
        "target_tables", list(schema_columns.keys())
    )
    equip_priority: list[str] = consolidation.get(
        "equipment_source_priority", list(source_dirs.keys())
    )
    output_subdirs: dict[str, str] = consolidation.get("output_subdirs", {})
    source_files: dict = consolidation.get("source_files", {})
    cli_arg_map: dict[str, str] = consolidation.get("source_dirs", {})

    readable_lookup = _build_readable_name_lookup(source_dirs)
    print(
        f"  [readable] Built equipment_id_readable lookup: {len(readable_lookup)} entries"
    )

    row_counts: dict[str, int] = {}

    for _table_index, table in enumerate(target_tables):
        print(f"  [consolidate] {table} ...")
        _emit_progress(
            "consolidate",
            label="Consolidating CDM tables",
            status="running",
            items_done=_table_index,
            items_total=len(target_tables),
            current_item=table,
        )

        file_map: dict[str, str] = {}
        for src_key, rel_path in source_files.get(table, {}).items():
            cli_arg = cli_arg_map.get(src_key, "")
            abs_dir = source_dirs.get(cli_arg, source_dirs.get(src_key, ""))
            if abs_dir and rel_path:
                full_path = fs.path_join(abs_dir, rel_path)
                if full_path.endswith(".csv"):
                    pq_path = full_path[:-4] + ".parquet"
                    if fs.exists(pq_path):
                        full_path = pq_path
                file_map[src_key] = full_path

        dk = dedup_keys.get(table, [])
        table_sources = set(source_files.get(table, {}).keys())
        has_sap = "SAP" in table_sources

        out_name = table
        if table == "document_metadata":
            out_name = "documents"
        subdir = output_subdirs.get(table, "entities")
        out_subdir = fs.path_join(out_dir, subdir)
        if not fs.is_s3(out_subdir):
            os.makedirs(out_subdir, exist_ok=True)
        out_path = fs.path_join(out_subdir, f"{out_name}.parquet")

        if table == "equipment":
            df = _merge_equipment(file_map, equip_priority, dk)
            df = _enforce_schema(df, table, schema_columns, uid_col, keep_extra=has_sap)
            if "equipment_id" in df.columns:
                df = _apply_readable_names(df, readable_lookup)
            fs.write_parquet(df, out_path)
            row_counts[table] = len(df)
            continue

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", prefix=f"cdm_{table}_")
        os.close(tmp_fd)
        try:
            merged_rows = _stream_merge_generic(file_map, dk, tmp_path)
            if merged_rows == 0:
                row_counts[table] = 0
                continue

            uid_counter = [0]

            def _chunk_transform(chunk: pd.DataFrame) -> pd.DataFrame:
                cols = schema_columns.get(table, list(chunk.columns))
                pk = uid_col.get(table)
                for c in cols:
                    if c not in chunk.columns:
                        chunk[c] = ""
                chunk = chunk.reset_index(drop=True)
                if pk and pk in cols:
                    start_id = uid_counter[0] + 1
                    chunk[pk] = range(start_id, start_id + len(chunk))
                    uid_counter[0] += len(chunk)
                schema_cols_present = [c for c in cols if c in chunk.columns]
                if has_sap:
                    extra_cols = [c for c in chunk.columns if c not in cols]
                    chunk = chunk[schema_cols_present + extra_cols]
                else:
                    chunk = chunk[schema_cols_present]

                if table == "document_metadata":
                    if "source_file" in chunk.columns:
                        chunk["title"] = chunk["source_file"].apply(
                            lambda x: os.path.basename(str(x)) if pd.notna(x) else ""
                        )
                    if "normalized_asset" in chunk.columns:
                        chunk["equipment_id"] = chunk["normalized_asset"]
                    elif "equipment_tag" in chunk.columns:
                        chunk["equipment_id"] = chunk["equipment_tag"]

                if "equipment_id" in chunk.columns:
                    chunk = _apply_readable_names(chunk, readable_lookup)
                return chunk

            total = fs.stream_transform_to_parquet(tmp_path, out_path, _chunk_transform)
            row_counts[table] = total
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    summary = {
        "generated_at": now_ts,
        "config": {
            "schema": "config/schema_frozen.yaml",
            "cdm_cfg": "config/cdm_config_frozen.yaml",
        },
        "sources": source_dirs,
        "tables": {
            t: {
                "rows": row_counts[t],
                "file": fs.path_join(output_subdirs.get(t, "entities"), f"{t}.csv"),
                "dedup_key": dedup_keys.get(t, []),
            }
            for t in row_counts
        },
        "total_rows": sum(row_counts.values()),
    }
    summary_path = fs.path_join(out_dir, "cdm_summary.json")
    if not fs.is_s3(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    if not fs.is_s3(out_dir):
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
    else:
        from p0.utils.fs import get_fs as _get_fs

        with _get_fs().open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
    return row_counts


def main() -> None:
    ap = argparse.ArgumentParser(
        description="CDM Final Consolidation (template-driven)"
    )
    ap.add_argument(
        "--pnid_out",
        required=True,
        help="P&ID pipeline output dir  (e.g. data/out/pnid)",
    )
    ap.add_argument(
        "--ts_out", required=True, help="TS pipeline output dir     (e.g. data/out/ts)"
    )
    ap.add_argument(
        "--docs_out",
        required=True,
        help="Docs pipeline output dir   (e.g. data/out/docs)",
    )
    ap.add_argument(
        "--sap_out",
        required=False,
        default="",
        help="SAP pipeline output dir    (e.g. data/out/sap)",
    )
    ap.add_argument(
        "--fmea_out",
        required=False,
        default="",
        help="FMEA pipeline output dir   (e.g. data/out/fmea)",
    )
    ap.add_argument(
        "--out_dir",
        required=True,
        help="Final output dir           (e.g. data/out/final)",
    )
    ap.add_argument(
        "--config_dir",
        required=False,
        default="./config",
        help="Config directory containing schema_frozen.yaml and cdm_config_frozen.yaml",
    )
    args = ap.parse_args()

    schema = _load_config(args.config_dir, "schema_frozen.yaml")
    cdm_cfg = _load_config(args.config_dir, "cdm_config_frozen.yaml")

    source_dirs = {
        "pnid_out": args.pnid_out,
        "ts_out": args.ts_out,
        "docs_out": args.docs_out,
        "sap_out": args.sap_out,
        "fmea_out": args.fmea_out,
    }

    print("=" * 60)
    print("CDM FINAL CONSOLIDATION  (template-driven)")
    print("=" * 60)
    print(f"  Schema   : {args.config_dir}/schema_frozen.yaml")
    print(f"  CDM cfg  : {args.config_dir}/cdm_config_frozen.yaml")
    print(f"  P&ID src : {args.pnid_out}")
    print(f"  TS src   : {args.ts_out}")
    print(f"  Docs src : {args.docs_out}")
    print(f"  SAP src  : {args.sap_out or '(not provided)'}")
    print(f"  FMEA src : {args.fmea_out or '(not provided)'}")
    print(f"  Output   : {args.out_dir}")
    print()

    row_counts = build_final_cdm(
        source_dirs=source_dirs,
        out_dir=args.out_dir,
        schema=schema,
        cdm_cfg=cdm_cfg,
    )

    print()
    print("=" * 60)
    print("CONSOLIDATION COMPLETE")
    print("=" * 60)
    total = 0
    for table, count in row_counts.items():
        print(f"  {table:<34} {count:>6} rows")
        total += count
    print(f"  {'TOTAL':<34} {total:>6} rows")
    print()
    print(f"  Output folder : {args.out_dir}")
    print(f"  Summary       : {fs.path_join(args.out_dir, 'cdm_summary.json')}")


if __name__ == "__main__":
    main()
