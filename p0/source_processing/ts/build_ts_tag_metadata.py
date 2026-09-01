from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

TS_DIR: Path
PID_DIR: Path
OUT_CSV: Path
VERIFY_CSV: Path


MEASUREMENT_MAP = {
    "IZ": "current",
    "JZ": "power",
    "SZ": "speed",
    "TZ": "temperature",
    "PZ": "pressure",
    "LZ": "level",
    "FZ": "flow",
    "ZZ": "position",
    "BW": "weight",
}

DEFAULT_UOM_BY_MEASUREMENT = {
    "IZ": "Amps",
    "JZ": "kW",
    "SZ": "RPM",
    "TZ": "°C",
    "PZ": "mmWC",
    "LZ": "%",
    "FZ": "TPH",
    "ZZ": "%",
    "BW": "Tonnes",
}

SECTION_KEYWORDS = {
    "preheater": ["pre-heater", "pre heater", "PRE HEATER"],
    "rawmill": ["rawmill", "Rawmill"],
    "slagmill1": ["SLAG MILL-1", "SLAGMILL-1", "Rp2"],
    "slagmill2": ["SLAG MILL-2", "SLAGMILL-2", "RP-2"],
    "cementmill": ["CEMENT MILL", "Cement Mill"],
}


@dataclass
class PidComponent:
    tag: str
    type: str
    description: str
    file_name: str


def normalize_text(v: object) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", s)


def normalize_tag(tag: str) -> str:
    t = normalize_text(tag).upper()
    t = t.replace(" ", "")
    t = t.lstrip("_")
    return t


def parse_section_name(file_name: str, sheet_name: str) -> str:
    text = f"{file_name} {sheet_name}".lower()
    for section, keys in SECTION_KEYWORDS.items():
        if any(k.lower() in text for k in keys):
            return section
    return "unknown"


def find_header_row(df: pd.DataFrame) -> Optional[int]:
    for i in range(min(40, len(df))):
        row_vals = [normalize_text(x).upper() for x in df.iloc[i, :6].tolist()]
        if any("TAG NAME" in x for x in row_vals):
            return i
    return None


def extract_base_tag(tag: str) -> str:
    t = normalize_tag(tag)
    return t.split("_")[0] if "_" in t else t


def extract_measurement_code(tag: str) -> str:
    t = normalize_tag(tag)
    if "_" in t:
        right = t.split("_", 1)[1]
    else:
        right = t
    m = re.search(r"([A-Z]{2})", right)
    return m.group(1) if m else ""


def clean_numeric(s: pd.Series) -> pd.Series:
    out = pd.to_numeric(s, errors="coerce")
    return out[np.isfinite(out)]


def load_pid_components() -> list[PidComponent]:
    items: list[PidComponent] = []
    for f in sorted(PID_DIR.glob("*.xlsx")):
        comp = pd.read_excel(f, sheet_name="Components")
        for _, r in comp.iterrows():
            tag = normalize_tag(r.get("Tag", ""))
            if not tag:
                continue
            items.append(
                PidComponent(
                    tag=tag,
                    type=normalize_text(r.get("Type", "")),
                    description=normalize_text(r.get("Description", "")),
                    file_name=f.name,
                )
            )
    return items


def link_asset_name(
    ts_tag: str, section: str, pid_components: list[PidComponent]
) -> tuple[str, str, float]:
    ntag = normalize_tag(ts_tag)
    base = extract_base_tag(ntag)

    for comp in pid_components:
        if comp.tag == ntag:
            return (comp.description or comp.tag, "exact_tag", 1.0)

    for comp in pid_components:
        if comp.tag == base:
            return (comp.description or comp.tag, "exact_base", 0.95)

    candidates = [
        c for c in pid_components if c.tag.startswith(base) or base.startswith(c.tag)
    ]
    if candidates:
        candidates.sort(
            key=lambda c: (0 if c.type.lower() == "equipment" else 1, len(c.tag))
        )
        return (candidates[0].description or candidates[0].tag, "prefix_contains", 0.8)

    m = re.match(r"(\d{3})", base)
    if m:
        fam = m.group(1)
        fam_candidates = [c for c in pid_components if c.tag.startswith(fam)]
        if fam_candidates:
            fam_candidates.sort(
                key=lambda c: (0 if c.type.lower() == "equipment" else 1, len(c.tag))
            )
            return (
                fam_candidates[0].description or fam_candidates[0].tag,
                "family_3digit",
                0.6,
            )

    return (base, "fallback_base_tag", 0.4)


def infer_uom(tag: str, description: str, raw_uom: str) -> str:
    uom = normalize_text(raw_uom)
    if uom:
        return uom

    desc = normalize_text(description).lower()
    if "count" in desc:
        return "count"
    if "total" in desc or "tot" in desc:
        return "total"

    code = extract_measurement_code(tag)
    return DEFAULT_UOM_BY_MEASUREMENT.get(code, "unitless")


def build_description(tag: str, raw_description: str, linked_asset: str) -> str:
    desc = normalize_text(raw_description)
    if desc:
        return desc

    measure_code = extract_measurement_code(tag)
    measure = MEASUREMENT_MAP.get(measure_code, "process value")

    if linked_asset:
        return f"{linked_asset} {measure}".strip()

    base = extract_base_tag(tag)
    return f"{base} {measure}".strip()


def extract_metadata_rows() -> pd.DataFrame:
    pid_components = load_pid_components()
    records: list[dict] = []

    for xlsx in sorted(TS_DIR.glob("*.xlsx")):
        xls = pd.ExcelFile(xlsx)
        for sheet in xls.sheet_names:
            df = pd.read_excel(xlsx, sheet_name=sheet, header=None)
            header_row = find_header_row(df)
            if header_row is None:
                continue

            desc_row = header_row + 1
            unit_row = header_row + 2
            data_row = header_row + 4
            section = parse_section_name(xlsx.name, sheet)

            ncols = df.shape[1]
            for col in range(1, ncols):
                raw_tag = normalize_text(df.iat[header_row, col] if col < ncols else "")
                tag = normalize_tag(raw_tag)
                if not tag:
                    continue

                raw_desc = normalize_text(
                    df.iat[desc_row, col] if desc_row < len(df) else ""
                )
                values = (
                    clean_numeric(df.iloc[data_row:, col])
                    if data_row < len(df)
                    else pd.Series(dtype=float)
                )
                lower = float(values.min()) if not values.empty else np.nan
                upper = float(values.max()) if not values.empty else np.nan

                linked_asset, match_method, link_confidence = link_asset_name(
                    tag, section, pid_components
                )
                final_desc = build_description(tag, raw_desc, linked_asset)
                uom = infer_uom(
                    tag, final_desc, df.iat[unit_row, col] if unit_row < len(df) else ""
                )

                records.append(
                    {
                        "tagname": tag,
                        "description": final_desc,
                        "uom": uom,
                        "lower limit": lower,
                        "upper limit": upper,
                        "linked asset name": linked_asset,
                        "linked_asset_match_method": match_method,
                        "linked_asset_confidence": link_confidence,
                        "source_file": xlsx.name,
                        "source_sheet": sheet,
                        "section": section,
                    }
                )

    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=out.columns)

    out = (
        out.sort_values(["tagname", "source_file", "source_sheet"])
        .groupby("tagname", as_index=False)
        .agg(
            {
                "description": "first",
                "uom": lambda s: next((x for x in s if normalize_text(x)), ""),
                "lower limit": "min",
                "upper limit": "max",
                "linked asset name": lambda s: next(
                    (x for x in s if normalize_text(x)), ""
                ),
                "linked_asset_match_method": "first",
                "linked_asset_confidence": "max",
                "source_file": lambda s: " | ".join(sorted(set(s))),
                "source_sheet": lambda s: " | ".join(sorted(set(s))),
                "section": lambda s: " | ".join(sorted(set(s))),
            }
        )
    )
    return out


def build_verification(ts_meta: pd.DataFrame) -> pd.DataFrame:
    raw_tags: set[str] = set()
    for xlsx in sorted(TS_DIR.glob("*.xlsx")):
        for sheet in pd.ExcelFile(xlsx).sheet_names:
            df = pd.read_excel(xlsx, sheet_name=sheet, header=None)
            h = find_header_row(df)
            if h is None:
                continue
            for col in range(1, df.shape[1]):
                tag = normalize_tag(df.iat[h, col])
                if tag:
                    raw_tags.add(tag)

    meta_tags = set(ts_meta["tagname"].tolist())
    missing_in_meta = sorted(raw_tags - meta_tags)
    extra_in_meta = sorted(meta_tags - raw_tags)

    checks = [
        {"check": "raw_tag_count", "value": len(raw_tags)},
        {"check": "metadata_tag_count", "value": len(meta_tags)},
        {"check": "missing_in_metadata_count", "value": len(missing_in_meta)},
        {"check": "extra_in_metadata_count", "value": len(extra_in_meta)},
        {
            "check": "blank_description_count",
            "value": int((ts_meta["description"].str.strip() == "").sum()),
        },
        {
            "check": "blank_uom_count",
            "value": int((ts_meta["uom"].str.strip() == "").sum()),
        },
        {
            "check": "blank_linked_asset_count",
            "value": int(
                (ts_meta["linked asset name"].fillna("").str.strip() == "").sum()
            ),
        },
    ]

    if missing_in_meta:
        checks.append(
            {
                "check": "missing_in_metadata_sample",
                "value": " | ".join(missing_in_meta[:15]),
            }
        )
    if extra_in_meta:
        checks.append(
            {
                "check": "extra_in_metadata_sample",
                "value": " | ".join(extra_in_meta[:15]),
            }
        )

    return pd.DataFrame(checks)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Extract TS tag metadata from raw workbooks and write a staging CSV."
    )
    ap.add_argument(
        "--ts_dir", required=True, help="Folder containing TS Excel workbooks"
    )
    ap.add_argument(
        "--pid_dir", required=True, help="Folder containing parsed P&ID Excel files"
    )
    ap.add_argument(
        "--out_dir", required=True, help="Output directory for generated CSVs"
    )
    args = ap.parse_args()

    global TS_DIR, PID_DIR, OUT_CSV, VERIFY_CSV
    TS_DIR = Path(args.ts_dir)
    PID_DIR = Path(args.pid_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    OUT_CSV = out_dir / "timeseries_tag_metadata.csv"
    VERIFY_CSV = out_dir / "timeseries_tag_metadata_verification.csv"

    ts_meta = extract_metadata_rows()
    if ts_meta.empty:
        raise SystemExit("No metadata could be extracted from TS workbooks.")

    final_cols = [
        "tagname",
        "description",
        "uom",
        "lower limit",
        "upper limit",
        "linked asset name",
    ]
    ts_meta[final_cols].to_csv(OUT_CSV, index=False)

    extended_out = out_dir / "timeseries_tag_metadata_with_confidence.csv"
    ts_meta[
        [
            "tagname",
            "description",
            "uom",
            "lower limit",
            "upper limit",
            "linked asset name",
            "linked_asset_match_method",
            "linked_asset_confidence",
            "source_file",
            "source_sheet",
            "section",
        ]
    ].to_csv(extended_out, index=False)

    low_conf = ts_meta[ts_meta["linked_asset_confidence"] < 0.8].copy()
    low_conf_out = out_dir / "timeseries_tag_metadata_low_confidence_review.csv"
    low_conf[
        [
            "tagname",
            "description",
            "uom",
            "linked asset name",
            "linked_asset_match_method",
            "linked_asset_confidence",
            "source_file",
            "source_sheet",
            "section",
        ]
    ].to_csv(low_conf_out, index=False)

    verification = build_verification(ts_meta)
    verification.to_csv(VERIFY_CSV, index=False)

    print(f"Created metadata: {OUT_CSV}")
    print(f"Created metadata (extended): {extended_out}")
    print(f"Created low-confidence review: {low_conf_out}")
    print(f"Created verification: {VERIFY_CSV}")
    print(f"Total unique tags: {len(ts_meta)}")
    print(f"Low-confidence links (<0.8): {len(low_conf)}")


if __name__ == "__main__":
    main()
