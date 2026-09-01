"""Findings end-to-end pipeline — AIF, GLOC and LOPC."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import datetime, timezone

import pandas as pd

from p0.api.services import findings as _findings
from p0.utils import fs as _fs
from p0.utils.asset_identity import assign_equipment_uid
from p0.utils.pnid_match import load_pnid_normalized_asset_map, match_against_pnid
from p0.utils.progress import emit as _emit_progress, emit_file as _emit_file

log = logging.getLogger("p0.findings.pipeline")

_READERS = {".csv": "csv", ".xlsx": "excel", ".xls": "excel"}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="AIF / GLOC / LOPC end-to-end pipeline")
    p.add_argument("--connector", required=True, choices=list(_findings.CONNECTORS))
    p.add_argument("--findings_in", required=True)
    p.add_argument("--work_dir", default="")
    p.add_argument("--out_dir", required=True)
    p.add_argument("--processed_out", required=True)
    p.add_argument("--plant_code_id", required=True)
    p.add_argument("--pid_out", default="")
    p.add_argument("--findings_files", default="")
    p.add_argument("--upload_batch_id", default="")
    p.add_argument("--config_dir", default="")
    return p.parse_args()


def _list_source_files(staging: str, wanted: list[str]) -> list[str]:
    """Every readable source file in staging, filtered to the requested names."""
    found: list[str] = []
    for extension in _READERS:
        try:
            found.extend(_fs.glob_files(_fs.path_join(staging, f"*{extension}")))
        except Exception as exc:
            log.warning("[findings] listing %s%s failed: %s", staging, extension, exc)
    found = [f for f in found if not _fs.basename(f).startswith((".", "_"))]
    if wanted:
        names = {w.strip() for w in wanted if w.strip()}
        found = [f for f in found if _fs.basename(f) in names]
    return sorted(set(found))


def _read_source(path: str, connector: str) -> pd.DataFrame:
    """Read one source file, choosing the data sheet by the template's hints."""
    extension = os.path.splitext(path)[1].lower()
    if _READERS.get(extension) == "csv":
        return _fs.read_csv(path)
    book_sheets = _fs.read_excel(path, sheet_name=None)
    if isinstance(book_sheets, dict):
        chosen = _findings.pick_sheet(connector, list(book_sheets.keys()))
        return book_sheets[chosen] if chosen else pd.DataFrame()
    return book_sheets


def _known_assets(pid_out: str) -> set[str]:
    """Assets already committed from P&ID or SAP, for the mapping step."""
    if not pid_out:
        return set()
    assets: set[str] = set()
    for name in ("equipment.parquet", "equipment_pid.parquet"):
        path = _fs.path_join(pid_out, name)
        try:
            if _fs.exists(path):
                frame = _fs.read_parquet(path)
                for column in ("normalized_asset", "equipment_tag"):
                    if column in frame.columns:
                        assets.update(str(v).strip().upper() for v in frame[column].dropna())
        except Exception as exc:
            log.warning("[findings] could not read %s: %s", path, exc)
    return {a for a in assets if a}


def _map_to_known(frame: pd.DataFrame, known: set[str]) -> pd.DataFrame:
    """Flag which resolved assets exist in the canonical equipment set."""
    if frame.empty:
        return frame
    if not known:
        frame["asset_mapped"] = False
        return frame
    frame["asset_mapped"] = [
        bool(str(value).strip().upper() in known) for value in frame["normalized_asset"]
    ]
    return frame


def main() -> int:
    args = _parse_args()
    connector = args.connector
    spec = _findings.spec_for(connector)
    now = datetime.now(timezone.utc)

    _emit_progress("validate", label="Reading source files", status="running")
    wanted = [f for f in (args.findings_files or "").split(",") if f.strip()]
    sources = _list_source_files(args.findings_in, wanted)
    if not sources:
        print(f"[{connector}] No source data found under {args.findings_in}", flush=True)
        _emit_progress("validate", label="Reading source files", status="failed")
        return 1
    _emit_progress(
        "validate", label="Reading source files", status="succeeded",
        items_done=len(sources), items_total=len(sources),
    )

    known = _known_assets(args.pid_out)
    log.info("[%s] %d known asset(s) available for mapping", connector, len(known))

    _emit_progress(
        "extract", label="Extracting findings", status="running",
        items_done=0, items_total=len(sources),
    )
    frames: list[pd.DataFrame] = []
    processed = 0
    for path in sources:
        name = _fs.basename(path)
        try:
            raw = _read_source(path, connector)
            if raw is None or raw.empty:
                _emit_file(name, "skipped")
                continue
            frame, mapping = _findings.read_frame(connector, raw)
            missing = _findings.missing_required(connector, mapping)
            if missing:
                _emit_file(name, "failed", error=f"missing required column(s): {', '.join(missing)}")
                continue
            frame = _map_to_known(frame, known)
            records = frame.to_dict("records")
            frame["source_record_id"] = _findings.assign_record_ids(connector, records)
            frame["source_file"] = name
            frame["plant_code_id"] = args.plant_code_id
            frame["source_system"] = connector.upper()
            frame["upload_batch_id"] = args.upload_batch_id or ""
            frame["confidence"] = frame["asset_match_confidence"]
            frame["updated_at"] = now
            frame["is_active"] = True
            frames.append(frame)
            processed += 1
            _emit_file(name, "processed", rows=int(len(frame)))
        except Exception as exc:
            log.exception("[%s] failed on %s", connector, name)
            _emit_file(name, "failed", error=str(exc))
        _emit_progress(
            "extract", label="Extracting findings", status="running",
            items_done=processed, items_total=len(sources), current_item=name,
        )

    if not frames:
        print(f"[{connector}] No source data found - every file was skipped or failed", flush=True)
        _emit_progress("extract", label="Extracting findings", status="failed")
        return 1

    combined = pd.concat(frames, ignore_index=True)
    combined = assign_equipment_uid(combined, plant_code_id=args.plant_code_id)

    if spec.get("pnid_equipment_match"):
        pnid_map = load_pnid_normalized_asset_map(args.plant_code_id, _fs)
        if pnid_map is None:
            log.info(
                "[%s] no P&ID equipment_pid.parquet found -- equipment_id left blank",
                connector,
            )
            combined["equipment_id"] = ""
        else:
            matches = [
                match_against_pnid(v, pnid_map) for v in combined["normalized_asset"]
            ]
            combined["equipment_id"] = [m[1] if m[0] else "" for m in matches]
            combined["normalized_asset"] = [
                m[1] if m[0] else asset
                for asset, m in zip(combined["normalized_asset"], matches)
            ]
            matched_count = sum(1 for m in matches if m[0])
            log.info(
                "[%s] P&ID equipment_id match: %d/%d row(s) matched",
                connector, matched_count, len(combined),
            )

    _emit_progress(
        "load", label="Writing canonical output", status="running",
        items_done=0, items_total=1,
    )
    table = spec.get("target_table") or f"{connector}_findings"
    for destination in (args.out_dir, args.processed_out):
        if not destination:
            continue
        if not _fs.is_s3(destination):
            os.makedirs(destination, exist_ok=True)
        _fs.write_parquet(combined, _fs.path_join(destination, f"{table}.parquet"))
    _emit_progress(
        "load", label="Writing canonical output", status="succeeded",
        items_done=1, items_total=1,
    )

    matched = int((combined["normalized_asset"].astype(str).str.len() > 0).sum())
    mapped = int(combined.get("asset_mapped", pd.Series([], dtype=bool)).sum())
    print(
        f"[{connector}] {len(combined)} finding(s) from {processed} file(s) — "
        f"{matched} with an equipment identity, {mapped} matched to known equipment",
        flush=True,
    )
    _emit_progress(
        "finalize", label="Finalising", status="succeeded", items_done=1, items_total=1
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
