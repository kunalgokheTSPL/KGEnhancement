from __future__ import annotations

import os
import re
import pandas as pd
from p0.utils import fs

import logging

_TS_CHUNK = 10_000

log = logging.getLogger("ts_pipeline")


_TS_KNOWN_HEADER_TOKENS = {
    "tagname",
    "tag",
    "tagid",
    "pitag",
    "pitagname",
    "parametername",
    "parameter",
    "equipment",
    "equipmentid",
    "equipmenttag",
    "assetid",
    "lowerlimit",
    "upperlimit",
    "lllimit",
    "llimit",
    "hlimit",
    "hhlimit",
    "opll",
    "opl",
    "oph",
    "ophh",
    "operatingll",
    "operatingl",
    "operatingh",
    "operatinghh",
    "oplimitll",
    "oplimitl",
    "oplimith",
    "oplimithh",
    "safetyll",
    "safetyl",
    "safetyh",
    "safetyhh",
    "safetylimitll",
    "safetylimitl",
    "safetylimith",
    "safetylimithh",
    "tripll",
    "tripl",
    "triph",
    "triphh",
    "triplimitll",
    "triplimitl",
    "triplimith",
    "triplimithh",
    "minoperatingrange",
    "maxoperatingrange",
    "minoperating",
    "maxoperating",
    "lowalarm",
    "highalarm",
    "minimum",
    "maximum",
    "description",
    "unit",
    "uom",
    "datatype",
    "type",
    "plant",
    "plantcode",
    "subsystem",
    "template",
}

from p0.source_processing.ts.source_detect import detect_ts_source
from p0.source_processing.ts.source_reader import read_detected_source

_HEADER_SCAN_MAX = 15


def _norm_token(s: object) -> str:
    """Lowercase + strip non-alnum so 'Tag Name', 'tag_name', 'TAG-NAME'"""
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def _score_columns(cols: list) -> int:
    """How many columns match one of the known TS header tokens."""
    return sum(1 for c in cols if _norm_token(c) in _TS_KNOWN_HEADER_TOKENS)


def _smart_read_excel(path: str) -> tuple[pd.DataFrame, int]:
    """Read an xlsx and try multiple header rows. Returns (df, header_row_used)."""
    best_df: pd.DataFrame | None = None
    best_header = 0
    best_score = -1
    for hdr in range(_HEADER_SCAN_MAX):
        try:
            df = fs.read_excel(path, dtype=str, header=hdr)
        except Exception as exc:
            log.debug("[ts] header=%d read failed: %s", hdr, exc)
            continue
        score = _score_columns(list(df.columns))
        log.debug(
            "[ts] header=%d → %d recognized cols (of %d)", hdr, score, len(df.columns)
        )
        if score > best_score:
            best_score, best_df, best_header = score, df, hdr
            if score >= 3:
                break
    if best_df is None:
        best_df = fs.read_excel(path, dtype=str)
        best_header = 0
    return best_df, best_header


def _source_parse_spec(source: str) -> dict:
    """The parse rules a detected source declares in the rename template."""
    try:
        import yaml

        from p0.api.config import COMMON_DIR

        path = COMMON_DIR / "column_rename" / "timeseries_column_rename.yaml"
        with open(path, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh) or {}
        return ((cfg.get("sources") or {}).get(source) or {}).get("parse") or {}
    except Exception as exc:
        raise RuntimeError(f"cannot read parse rules for {source}: {exc}") from exc


def _write_merged(df: pd.DataFrame, work_dir: str) -> None:
    """Persist the merged frame where the rest of the pipeline expects it."""
    if fs.is_s3(work_dir):
        df.to_csv(
            fs.path_join(work_dir, "timeseries_merged.csv"),
            index=False,
            storage_options=fs.get_storage_options(),
        )
    else:
        df.to_csv(os.path.join(work_dir, "timeseries_merged.csv"), index=False)


def _validate_ts_schema(df: pd.DataFrame, source_path: str) -> None:
    """Fail fast (and helpfully) if the file isn't a TS metadata file."""
    norm = {_norm_token(c) for c in df.columns}
    tag_aliases = {"tagname", "tag", "tagid", "pitag", "pitagname", "parametername"}
    if not (norm & tag_aliases):
        unnamed = sum(1 for c in df.columns if _norm_token(c).startswith("unnamed"))
        raise ValueError(
            "TS pipeline aborted: uploaded file does not look like a timeseries "
            "tag-metadata file.\n"
            f"  file:        {fs.basename(source_path)}\n"
            f"  columns:     {list(df.columns)[:10]}{' ...' if len(df.columns) > 10 else ''}\n"
            f"  unnamed:     {unnamed}/{len(df.columns)}\n"
            "  expected:    a column called one of: tag_name, Tag Name, tagname, Tag, tag_id,\n"
            "               PI Tag, Parameter Name\n"
            "  fix:         upload the canonical timeseries_tag_metadata.{csv,xlsx,parquet}\n"
            "               (see config/templates/_common/column_rename/timeseries_column_rename.yaml\n"
            "                for the full list of accepted header variants)"
        )


def _detect_id_vars(sample: pd.DataFrame) -> list[str]:
    """Detect id_vars (non-value columns) from a sample chunk."""
    id_vars = ["Timestamp"]
    for col in sample.columns:
        if col != "Timestamp" and (
            sample[col].nunique() < len(sample) / 2 or sample[col].dtype != "object"
        ):
            if col not in id_vars:
                id_vars.append(col)
    return id_vars


def _clean_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace and normalize NaN strings in a chunk."""
    chunk.columns = [str(c).strip() for c in chunk.columns]
    for col in chunk.columns:
        if chunk[col].dtype == object:
            chunk[col] = chunk[col].astype(str).str.strip()
            chunk[col] = chunk[col].replace({"nan": "", "None": "", "NaT": ""})
    return chunk


def run_ts_source_processing(
    ts_in: str, work_dir: str, target_file: str | None = None
) -> dict[str, pd.DataFrame]:
    if not fs.is_s3(work_dir):
        os.makedirs(work_dir, exist_ok=True)

    _FMT_BY_EXT = {
        "xlsx": "xlsx",
        "xls": "xlsx",
        "parquet": "parquet",
        "csv": "csv",
        "tsv": "csv",
    }

    ts_path = ts_fmt = None

    if target_file:
        want = str(target_file).strip().replace("\\", "/").rsplit("/", 1)[-1]
        for p in fs.glob_files(fs.path_join(ts_in, "*")):
            if fs.basename(p) == want:
                ext = want.rsplit(".", 1)[-1].lower() if "." in want else ""
                ts_path, ts_fmt = p, _FMT_BY_EXT.get(ext, "csv")
                log.info("  [ts] processing user-selected file: %s", want)
                break
        if ts_path is None:
            log.warning(
                "  [ts] selected file %r not found in staging dir; "
                "falling back to auto-resolve",
                want,
            )

    if ts_path is None:
        try:
            from p0.utils.io import _resolve_table_path

            ts_path, ts_fmt = _resolve_table_path(ts_in, "timeseries_tag_metadata")
        except FileNotFoundError:
            candidates = []
            for pat, fmt in (
                ("*.xlsx", "xlsx"),
                ("*.parquet", "parquet"),
                ("*.csv", "csv"),
            ):
                for p in fs.glob_files(fs.path_join(ts_in, pat)):
                    candidates.append((p, fmt))
            if not candidates:
                raise FileNotFoundError(
                    f"No timeseries_tag_metadata.csv/.parquet and no fallback CSV/XLSX in: {ts_in}"
                )
            candidates.sort(
                key=lambda pf: fs.mtime(pf[0]), reverse=True
            )

            def _looks_like_metadata(path: str, fmt: str) -> bool:
                try:
                    if fmt == "xlsx":
                        if detect_ts_source(path):
                            return True
                        hdr = _smart_read_excel(path)[0].columns.tolist()
                    elif fmt == "parquet":
                        hdr = fs.read_parquet(path).columns.tolist()
                    else:
                        hdr = fs.read_csv(path, nrows=0).columns.tolist()
                    return _score_columns(hdr) >= 2
                except Exception:
                    return False

            ts_path = ts_fmt = None
            for p, fmt in candidates:
                if _looks_like_metadata(p, fmt):
                    ts_path, ts_fmt = p, fmt
                    break
            if ts_path is None:
                ts_path, ts_fmt = candidates[0]
                log.warning(
                    "  [ts] no file scored as metadata; using newest: %s",
                    fs.basename(ts_path),
                )

    _source_file = fs.basename(ts_path)
    log.info(f"  [ts] Source: {_source_file}  format={ts_fmt}")

    if ts_fmt == "xlsx":
        detected = detect_ts_source(ts_path)
        if detected:
            source = detected["source"]
            log.info("  [ts] recognised as %s", detected.get("label") or source)
            df, parse_report = read_detected_source(
                ts_path, detected, _source_parse_spec(source)
            )
            df = _clean_chunk(df)
            df["source_file"] = _source_file
            df["source_system"] = source
            log.info("  [ts] %s: %d rows, parse=%s", source, len(df), parse_report)
            _write_merged(df, work_dir)
            return {source: df}

        df, header_row = _smart_read_excel(ts_path)
        if header_row != 0:
            log.info(
                f"  [ts] Auto-detected header at row {header_row} "
                f"(row 0 had no recognizable TS columns)"
            )
        df = _clean_chunk(df)
        _validate_ts_schema(df, ts_path)
        df["source_file"] = _source_file
        out_path = os.path.join(work_dir, "timeseries_merged.csv")
        if fs.is_s3(work_dir):
            df.to_csv(
                fs.path_join(work_dir, "timeseries_merged.csv"),
                index=False,
                storage_options=fs.get_storage_options(),
            )
        else:
            df.to_csv(out_path, index=False)
        log.info(f"  [ts] Timeseries: {len(df)} rows, columns: {list(df.columns)}")
        return {"timeseries": df}

    out_path = (
        fs.path_join(work_dir, "timeseries_merged.csv")
        if fs.is_s3(work_dir)
        else os.path.join(work_dir, "timeseries_merged.csv")
    )

    needs_melt = False
    id_vars: list[str] = []
    value_vars: list[str] = []
    first_write = True
    total_rows = 0
    result_cols: list[str] = []

    if ts_fmt == "parquet":
        from p0.utils.fs import read_parquet

        sample = read_parquet(ts_path, engine="pyarrow").head(_TS_CHUNK)
    else:
        sample = next(
            fs.read_csv_chunked(ts_path, chunksize=_TS_CHUNK, low_memory=False)
        )

    sample.columns = [str(c).strip() for c in sample.columns]
    is_wide_ts = "Timestamp" in sample.columns and len(sample.columns) > 2
    if not is_wide_ts:
        _validate_ts_schema(sample, ts_path)
    if is_wide_ts:
        id_vars = _detect_id_vars(sample)
        value_vars = [c for c in sample.columns if c not in id_vars]
        if value_vars:
            needs_melt = True
            log.info(
                f"  [ts] Wide format detected — {len(value_vars)} value columns. Melting in chunks."
            )

    if ts_fmt == "parquet":
        from p0.utils.fs import read_parquet as _rp

        _full = _rp(ts_path, engine="pyarrow")
        _full.columns = [str(c).strip() for c in _full.columns]
        chunk_iter = (
            _full.iloc[s : s + _TS_CHUNK].copy()
            for s in range(0, len(_full), _TS_CHUNK)
        )
    else:
        chunk_iter = fs.read_csv_chunked(ts_path, chunksize=_TS_CHUNK, low_memory=False)

    for chunk in chunk_iter:
        chunk.columns = [str(c).strip() for c in chunk.columns]

        if needs_melt:
            chunk = chunk.melt(
                id_vars=id_vars,
                value_vars=value_vars,
                var_name="tag_name",
                value_name="value",
            )

        chunk = _clean_chunk(chunk)
        chunk["source_file"] = _source_file
        total_rows += len(chunk)

        if first_write:
            result_cols = list(chunk.columns)
            if fs.is_s3(out_path):
                chunk.to_csv(
                    out_path, index=False, storage_options=fs.get_storage_options()
                )
            else:
                chunk.to_csv(out_path, index=False)
            first_write = False
        else:
            if fs.is_s3(out_path):
                chunk.to_csv(
                    out_path,
                    mode="a",
                    header=False,
                    index=False,
                    storage_options=fs.get_storage_options(),
                )
            else:
                chunk.to_csv(out_path, mode="a", header=False, index=False)

    log.info(f"  [ts] Timeseries: {total_rows} rows, columns: {result_cols}")

    if total_rows > _TS_CHUNK:
        sentinel = pd.read_csv(out_path, nrows=0)
        sentinel.attrs["__rows"] = total_rows
        sentinel.attrs["__path"] = out_path
        return {"timeseries": sentinel}
    else:
        return {"timeseries": pd.read_csv(out_path, low_memory=False)}
