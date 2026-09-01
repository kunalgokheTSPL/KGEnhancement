"""Inject a synthetic equipment_pid.parquet into RustFS for a given plant.

Use this to seed P&ID equipment data so the sap_mapping pipeline has something
to match against when no real P&ID pipeline has run yet.

Usage:
    MASTER_KEY=... PYTHONPATH=$PWD .venv/bin/python p0/scripts/write_pnid_parquet.py \\
        --plant_code_id SAP_TEST_3 \\
        --tags "V-101,P-101,E-201,HE-301"

The parquet is written to:
    s3://<BUCKET>/p0/processed_data/<plant>/pnid/equipment_pid.parquet

Exit code is non-zero if the write fails.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

BACKEND = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND))

from utility.secret_manager import load_secrets  # noqa: E402

load_secrets()

from p0.api.config import rustfs_processed_pnid  # noqa: E402
from p0.utils import fs as _fs  # noqa: E402


def _normalize(tag: str) -> str:
    """Strip non-alphanumeric chars and uppercase — mirrors sap_asset_identity._norm_text."""
    return re.sub(r"[^A-Za-z0-9]+", " ", tag).strip().upper()


def build_equipment_pid(plant_code_id: str, tags: list[str]) -> pd.DataFrame:
    rows = []
    for tag in tags:
        tag = tag.strip()
        if not tag:
            continue
        rows.append(
            {
                "equipment_tag": tag,
                "normalized_asset": _normalize(tag),
                "plant_code_id": plant_code_id,
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Write a synthetic equipment_pid.parquet to RustFS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python p0/scripts/write_pnid_parquet.py \\
      --plant_code_id SAP_TEST_3 \\
      --tags "V-101,P-101,E-201"
""",
    )
    ap.add_argument("--plant_code_id", required=True, help="Plant code ID (e.g. SAP_TEST_3)")
    ap.add_argument(
        "--tags",
        required=True,
        help="Comma-separated equipment tags to inject (e.g. 'V-101,P-101,E-201')",
    )
    args = ap.parse_args()

    tag_list = [t.strip() for t in args.tags.split(",") if t.strip()]
    if not tag_list:
        print("ERROR: --tags produced an empty list. Provide at least one tag.")
        return 1

    df = build_equipment_pid(args.plant_code_id, tag_list)
    print(f"  Built {len(df)} row(s) for plant '{args.plant_code_id}':")
    print(df.to_string(index=False))

    pnid_dir = rustfs_processed_pnid(args.plant_code_id)
    dest = f"{pnid_dir.rstrip('/')}/equipment_pid.parquet"
    print(f"\n  Writing -> {dest}")

    try:
        _fs.write_parquet(df, dest)
        print("  Done.")
        return 0
    except Exception as exc:
        print(f"  ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
