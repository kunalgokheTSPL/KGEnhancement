"""
FMEA End-to-End Pipeline

Reads Failure_Data_FMEA.xlsx from staging/fmea/ and produces CDM-compatible
CSV outputs for failure_mode, failure_cause, and failure_effect tables.

All 16 input columns are stored across the three tables:
  - failure_mode:   risk scoring (S/O/D/RPN), detection, controls, actions
  - failure_cause:  equipment context
  - failure_effect: equipment context

Usage:
    python -m pipelines.run_fmea_end_to_end \
        --config_dir ./config \
        --fmea_in    ./data/staging/fmea \
        --work_dir   ./data/work/fmea \
        --out_dir    ./data/out/fmea \
        --plant_code_id CEMENT_PLANT_1
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from p0.utils import fs


_FMEA_SHEET = "FMEA"
_FMEA_COLS = {
    "Equipment_ID": "equipment_id",
    "Equipment": "equipment_name",
    "Sub-system / Component": "subsystem_component",
    "Function": "function",
    "Failure Mode": "failure_mode_text",
    "Failure Cause": "failure_cause_text",
    "Failure Effect": "failure_effect_text",
    "Severity (S)": "severity",
    "Occurrence (O)": "occurrence",
    "Detectability (D)": "detectability",
    "Risk Priority Number (RPN)=S*O*D": "rpn",
    "Risk Category": "risk_category",
    "Detection Parameters": "detection_parameters",
    "Current Controls": "current_controls",
    "Recommended Action": "recommended_action",
}


def _now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_str(val) -> str:
    s = str(val).strip()
    return "" if s.lower() in ("nan", "none", "nat") else s


def _safe_int(val) -> int | None:
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def _read_fmea(fmea_dir: str) -> pd.DataFrame:
    """Find and read FMEA xlsx from fmea_dir (local or S3)."""
    if fs.is_s3(fmea_dir):
        files = fs.glob_files(fmea_dir.rstrip("/") + "/*.xlsx")
    else:
        files = [str(p) for p in Path(fmea_dir).glob("*.xlsx")]

    if not files:
        raise FileNotFoundError(f"No .xlsx files found in {fmea_dir}")

    fmea_file = next((f for f in files if "fmea" in f.lower()), files[0])

    if fs.is_s3(fmea_file):
        import io

        fso = fs.get_fs()
        with fso.open(fmea_file, "rb") as fh:
            raw = io.BytesIO(fh.read())
        df = pd.read_excel(raw, sheet_name=_FMEA_SHEET)
    else:
        df = pd.read_excel(fmea_file, sheet_name=_FMEA_SHEET)

    print(f"  [fmea] Loaded {len(df)} rows from {fmea_file}")
    return df


def run_fmea_end_to_end(
    config_dir: str,
    fmea_in: str,
    work_dir: str,
    out_dir: str,
    plant_code_id: str,
) -> dict[str, int]:
    """Process FMEA data → CDM failure_mode / cause / effect tables."""
    entities_dir = fs.path_join(out_dir, "entities")
    if not fs.is_s3(out_dir):
        os.makedirs(entities_dir, exist_ok=True)
        os.makedirs(work_dir, exist_ok=True)

    df_raw = _read_fmea(fmea_in)

    df = df_raw.rename(
        columns={k: v for k, v in _FMEA_COLS.items() if k in df_raw.columns}
    )

    required = ["failure_mode_text", "failure_cause_text", "failure_effect_text"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"FMEA file missing required columns: {missing}. Found: {list(df.columns)}"
        )

    now = _now_ts()
    eq_col = "equipment_id" if "equipment_id" in df.columns else None

    fm_rows = []
    for i, row in df.iterrows():
        text = _safe_str(row["failure_mode_text"])
        if not text:
            continue
        eq_id = _safe_str(row[eq_col]) if eq_col else ""
        code = f"FM_{eq_id}_{i + 1:03d}"[:60] if eq_id else f"FM_{i + 1:03d}"
        fm_rows.append(
            {
                "plant_code_id": plant_code_id,
                "failure_mode_code": code,
                "failure_mode_text": text,
                "equipment_id": eq_id,
                "equipment_name": _safe_str(row.get("equipment_name", "")),
                "subsystem_component": _safe_str(row.get("subsystem_component", "")),
                "function": _safe_str(row.get("function", "")),
                "severity": _safe_int(row.get("severity")),
                "occurrence": _safe_int(row.get("occurrence")),
                "detectability": _safe_int(row.get("detectability")),
                "rpn": _safe_int(row.get("rpn")),
                "risk_category": _safe_str(row.get("risk_category", "")),
                "detection_parameters": _safe_str(row.get("detection_parameters", "")),
                "current_controls": _safe_str(row.get("current_controls", "")),
                "recommended_action": _safe_str(row.get("recommended_action", "")),
                "source_system": "FMEA",
                "source_record_id": str(i),
                "confidence": 1.0,
                "updated_at": now,
                "is_active": True,
            }
        )
    df_fm = pd.DataFrame(fm_rows).drop_duplicates(
        subset=["plant_code_id", "failure_mode_code"]
    )

    fc_rows = []
    for i, row in df.iterrows():
        text = _safe_str(row["failure_cause_text"])
        if not text:
            continue
        eq_id = _safe_str(row[eq_col]) if eq_col else ""
        code = f"FC_{eq_id}_{i + 1:03d}"[:60] if eq_id else f"FC_{i + 1:03d}"
        fc_rows.append(
            {
                "plant_code_id": plant_code_id,
                "failure_cause_code": code,
                "failure_cause_text": text,
                "equipment_id": eq_id,
                "equipment_name": _safe_str(row.get("equipment_name", "")),
                "subsystem_component": _safe_str(row.get("subsystem_component", "")),
                "source_system": "FMEA",
                "source_record_id": str(i),
                "confidence": 1.0,
                "updated_at": now,
                "is_active": True,
            }
        )
    df_fc = pd.DataFrame(fc_rows).drop_duplicates(
        subset=["plant_code_id", "failure_cause_code"]
    )

    fe_rows = []
    for i, row in df.iterrows():
        text = _safe_str(row["failure_effect_text"])
        if not text:
            continue
        eq_id = _safe_str(row[eq_col]) if eq_col else ""
        code = f"FE_{eq_id}_{i + 1:03d}"[:60] if eq_id else f"FE_{i + 1:03d}"
        fe_rows.append(
            {
                "plant_code_id": plant_code_id,
                "failure_effect_code": code,
                "failure_effect_text": text,
                "equipment_id": eq_id,
                "equipment_name": _safe_str(row.get("equipment_name", "")),
                "subsystem_component": _safe_str(row.get("subsystem_component", "")),
                "source_system": "FMEA",
                "source_record_id": str(i),
                "confidence": 1.0,
                "updated_at": now,
                "is_active": True,
            }
        )
    df_fe = pd.DataFrame(fe_rows).drop_duplicates(
        subset=["plant_code_id", "failure_effect_code"]
    )

    def _write(df_out: pd.DataFrame, name: str) -> None:
        path = fs.path_join(entities_dir, f"{name}.csv")
        if fs.is_s3(path):
            fso = fs.get_fs()
            with fso.open(path, "w") as fh:
                df_out.to_csv(fh, index=False)
        else:
            df_out.to_csv(path, index=False)
        print(f"  [fmea] {name}: {len(df_out)} rows -> {path}")

    _write(df_fm, "failure_mode")
    _write(df_fc, "failure_cause")
    _write(df_fe, "failure_effect")

    return {
        "failure_mode": len(df_fm),
        "failure_cause": len(df_fc),
        "failure_effect": len(df_fe),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="FMEA End-to-End CDM Pipeline")
    ap.add_argument("--config_dir", required=False, default="./config")
    ap.add_argument("--fmea_in", required=True, help="Dir with Failure_Data_FMEA.xlsx")
    ap.add_argument("--work_dir", required=True)
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--plant_code_id", required=True)
    args = ap.parse_args()

    print("=" * 60)
    print("FMEA END-TO-END PIPELINE")
    print("=" * 60)
    print(f"  Input     : {args.fmea_in}")
    print(f"  Output    : {args.out_dir}")
    print(f"  Plant     : {args.plant_code_id}")
    print()

    counts = run_fmea_end_to_end(
        config_dir=args.config_dir,
        fmea_in=args.fmea_in,
        work_dir=args.work_dir,
        out_dir=args.out_dir,
        plant_code_id=args.plant_code_id,
    )

    print()
    print("=" * 60)
    print("FMEA PIPELINE COMPLETE")
    print("=" * 60)
    for table, cnt in counts.items():
        print(f"  {table:<30} {cnt:>6} rows")
    print()


if __name__ == "__main__":
    main()
