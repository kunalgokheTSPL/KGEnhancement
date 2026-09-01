from __future__ import annotations

import argparse
import glob
import os
import yaml
import pandas as pd

from p0.utils.renamer import apply_column_rename
from p0.utils.derived import apply_derived_fields
from p0.utils.identity import apply_identity_rules
from p0.utils.schema_apply import apply_schema
from p0.utils.validate import run_validations, report_to_json

from p0.utils.canonical_entities import (
    build_uid_tables_from_canonical_entities,
)
from p0.utils.canonical_relationships import (
    build_asset_relationships,
)


def _load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _norm_upper(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip().str.upper()
    x = x.str.replace(r"\s+", " ", regex=True)
    x = x.replace({"NAN": "", "NONE": "", "NAT": ""})
    return x


def _first_non_empty(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    out = pd.Series([""] * len(df), index=df.index)
    for c in cols:
        if c in df.columns:
            v = df[c].astype(str).str.strip()
            mask = (out == "") & (v != "") & (v.str.lower() != "nan")
            out = out.mask(mask, v)
    return out


def _build_entity_from_entities_yaml(
    post_pid: pd.DataFrame, entities_cfg: dict, entity_name: str, schema_cfg: dict
) -> pd.DataFrame:
    ent = (entities_cfg.get("entities", {}) or {}).get(entity_name, {}) or {}
    attrs = ent.get("attributes", {}) or {}

    out = pd.DataFrame(index=post_pid.index)
    for attr, spec in attrs.items():
        from_list = (spec or {}).get("from", []) or []
        pid_cols = []
        for ref in from_list:
            if isinstance(ref, str) and ref.startswith("pid."):
                pid_cols.append(ref.split(".", 1)[1])
        out[attr] = _first_non_empty(post_pid, pid_cols) if pid_cols else ""

    table = ent.get("canonical_table", entity_name) or entity_name
    out = apply_schema(out, schema_cfg, table_name=table)
    return out


def main():
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--pnid_dir", required=True, help="Folder containing multiple PnID excel files"
    )
    ap.add_argument(
        "--pattern",
        default="*.xlsx",
        help="Glob pattern inside pnid_dir (default *.xlsx)",
    )
    ap.add_argument("--equipment_sheet", default="equipment")
    ap.add_argument("--connectivity_sheet", default="connectivity")
    ap.add_argument("--out_dir", required=True)

    ap.add_argument("--cdm_config", default="/mnt/data/cdm_config_frozen.yaml")
    ap.add_argument("--pnid_rename", default="/mnt/data/pnid_column_rename.yaml")
    ap.add_argument("--derived_fields", default="/mnt/data/derived_fields.yaml")
    ap.add_argument("--identity", default="/mnt/data/identity_frozen.yaml")
    ap.add_argument("--entities", default="/mnt/data/entities_frozen.yaml")
    ap.add_argument("--relationships", default="/mnt/data/relationships_frozen.yaml")
    ap.add_argument("--schema", default="/mnt/data/schema_frozen.yaml")
    ap.add_argument("--validation", default="/mnt/data/validation_contracts.yaml")

    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    cdm_cfg = _load_yaml(args.cdm_config)
    rename_cfg = _load_yaml(args.pnid_rename)
    derived_cfg = _load_yaml(args.derived_fields)
    identity_cfg = _load_yaml(args.identity)
    entities_cfg = _load_yaml(args.entities)
    relationships_cfg = _load_yaml(args.relationships)
    schema_cfg = _load_yaml(args.schema)
    validation_cfg = _load_yaml(args.validation)

    plant_code_id = ((cdm_cfg.get("cdm", {}) or {}).get("plant_code_id") or "UNKNOWN").strip()

    files = sorted(glob.glob(os.path.join(args.pnid_dir, args.pattern)))
    if not files:
        raise SystemExit(f"No files found in {args.pnid_dir} matching {args.pattern}")

    eq_all = []
    cn_all = []

    for fp in files:
        eq_raw = pd.read_excel(fp, sheet_name=args.equipment_sheet)
        cn_raw = pd.read_excel(fp, sheet_name=args.connectivity_sheet)

        eq_raw["source_file"] = os.path.basename(fp)
        cn_raw["source_file"] = os.path.basename(fp)

        eq = apply_column_rename(eq_raw, rename_cfg, source_name="pid", verbose=False)
        cn = apply_column_rename(cn_raw, rename_cfg, source_name="pid", verbose=False)

        eq = apply_derived_fields(
            eq,
            derived_cfg,
            dataset_name="pid_equipment",
            source_name="pid",
            plant_code_id=plant_code_id,
        )
        cn = apply_derived_fields(
            cn,
            derived_cfg,
            dataset_name="pid_connectivity",
            source_name="pid",
            plant_code_id=plant_code_id,
        )
        eq["source_system"] = "PID"
        cn["source_system"] = "PID"

        eq = apply_identity_rules(eq, identity_cfg, dataset_name="pid")

        if "equipment_tag" in eq.columns:
            eq["equipment_tag"] = _norm_upper(eq["equipment_tag"])
        if "normalized_asset" not in eq.columns:
            eq["normalized_asset"] = ""
        eq["normalized_asset"] = _norm_upper(eq["normalized_asset"])
        if "equipment_tag" in eq.columns:
            eq.loc[eq["normalized_asset"] == "", "normalized_asset"] = eq[
                "equipment_tag"
            ]

        if "from_equipment_tag" in cn.columns:
            cn["from_equipment_tag"] = _norm_upper(cn["from_equipment_tag"])
            cn["from_normalized_asset"] = cn["from_equipment_tag"]
        else:
            cn["from_normalized_asset"] = ""

        if "to_equipment_tag" in cn.columns:
            cn["to_equipment_tag"] = _norm_upper(cn["to_equipment_tag"])
            cn["to_normalized_asset"] = cn["to_equipment_tag"]
        else:
            cn["to_normalized_asset"] = ""

        if "connection_type" not in cn.columns:
            cn["connection_type"] = "FLOW"
        cn["connection_type"] = (
            cn["connection_type"].astype(str).str.strip().replace({"": "FLOW"})
        )

        eq_all.append(eq)
        cn_all.append(cn)

    eq_post = pd.concat(eq_all, ignore_index=True, sort=False)
    cn_post = pd.concat(cn_all, ignore_index=True, sort=False)

    eq_post.to_csv(os.path.join(args.out_dir, "pid_post_equipment.csv"), index=False)
    cn_post.to_csv(os.path.join(args.out_dir, "pid_post_connectivity.csv"), index=False)

    equipment = _build_entity_from_entities_yaml(
        eq_post, entities_cfg, entity_name="equipment", schema_cfg=schema_cfg
    )
    equipment_conn = _build_entity_from_entities_yaml(
        cn_post, entities_cfg, entity_name="equipment_connection", schema_cfg=schema_cfg
    )

    equipment.to_csv(os.path.join(args.out_dir, "equipment.csv"), index=False)
    equipment_conn.to_csv(
        os.path.join(args.out_dir, "equipment_connectivity.csv"), index=False
    )

    equipment_pid = _build_entity_from_entities_yaml(
        eq_post, entities_cfg, entity_name="equipment_pid", schema_cfg=schema_cfg
    )
    equipment_pid.to_csv(os.path.join(args.out_dir, "equipment_pid.csv"), index=False)

    canonical_entities = {
        "equipment": equipment,
        "equipment_connectivity": equipment_conn,
        "equipment_pid": equipment_pid,
    }
    uid_tables = build_uid_tables_from_canonical_entities(
        canonical_entities, entities_cfg, schema_cfg
    )

    asset_relationship = build_asset_relationships(
        post_sources={
            "pid": cn_post
        },
        relationships_cfg=relationships_cfg,
        entities_cfg=entities_cfg,
        schema_cfg=schema_cfg,
        entity_uid_tables=uid_tables,
    )
    asset_relationship = apply_schema(
        asset_relationship, schema_cfg, table_name="asset_relationship"
    )
    asset_relationship.to_csv(
        os.path.join(args.out_dir, "asset_relationship.csv"), index=False
    )

    report = run_validations(
        datasets={
            "pid_post_equipment": eq_post,
            "pid_post_connectivity": cn_post,
            "equipment": equipment,
            "equipment_connectivity": equipment_conn,
            "asset_relationship": asset_relationship,
        },
        validation_cfg=validation_cfg,
    )
    with open(
        os.path.join(args.out_dir, "validation_report.json"), "w", encoding="utf-8"
    ) as f:
        f.write(report_to_json(report))

    print("DONE")
    print(f"Files processed: {len(files)}")
    print("Outputs:", args.out_dir)
    print(report_to_json(report))


if __name__ == "__main__":
    main()
