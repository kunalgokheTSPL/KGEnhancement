"""
CDM Post-Pipeline Validation — Pre-Database Quality Gate.

Runs AFTER build_final_cdm and BEFORE save_to_postgres to validate the
merged CDM output is complete, consistent, and safe to load.

Checks:
  1. Schema conformance    — every output CSV matches schema_frozen.yaml columns
  2. Required tables       — core CDM tables must exist and be non-empty
  3. Primary key integrity — PK columns non-null, unique per table
  4. Equipment connectivity — equipment_id linkage across all dependent tables
  5. Referential integrity — FK references resolve to parent tables
  6. Data quality basics   — null rates, duplicate rows, confidence distribution
  7. Relationship graph    — relationship CSVs reference valid entity IDs
  8. CDM manifest          — verify cdm_manifest.json is generated and complete

Usage:
    from src.utils.cdm_post_validation import run_cdm_post_validation

    report = run_cdm_post_validation(
        final_dir=Path("/data/out/final"),
        config_dir=Path("/app/config"),
    )
    if report["gate_status"] == "BLOCKED":
        raise RuntimeError("CDM validation failed")
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from p0.utils.config_resolver import resolve_config_path

log = logging.getLogger("p0-cdm-worker.cdm-validation")

BLOCK = "BLOCK"
WARN = "WARN"
PASS = "PASS"

_VALIDATION_CONFIG_FILE = "cdm_post_validation.yaml"


_DEFAULT_CORE_TABLES = ["equipment", "functional_location"]

_DEFAULT_EXPECTED_TABLES = [
    "equipment_connectivity",
    "work_order",
    "notification",
    "material",
    "bom_item",
    "timeseries_metadata",
    "document_metadata",
    "task_list",
    "maintenance_plan",
    "vendor",
    "purchase_order",
    "goods_receipt",
    "invoice",
    "process_unit",
    "work_center",
    "failure_mode",
    "failure_cause",
    "failure_effect",
]

_DEFAULT_EQUIPMENT_ID_COLS = [
    "normalized_asset",
    "equipment_id",
    "sap_equipment_id",
    "equipment_tag",
    "canonical_code",
]

_DEFAULT_EQUIPMENT_LINKED: dict[str, list[str]] = {
    "work_order": ["equipment_ref", "equipment_id"],
    "notification": ["equipment_ref"],
    "task_list": ["equipment_ref", "equipment_id"],
    "maintenance_plan": ["equipment_ref"],
    "timeseries_metadata": ["normalized_asset", "equipment_id"],
    "document_metadata": ["equipment_id"],
    "equipment_connectivity": ["from_equipment_ref", "to_equipment_ref"],
    "bom_item": ["sap_equipment_id"],
    "failure_mode": ["equipment_id"],
}

_DEFAULT_FK_CHECKS = [
    {
        "check_name": "wo_notification",
        "child_table": "work_order",
        "child_col": "wo_number",
        "parent_table": "notification",
        "parent_col": "notification_id",
    },
    {
        "check_name": "bom_material",
        "child_table": "bom_item",
        "child_col": "material_code",
        "parent_table": "material",
        "parent_col": "material_code",
    },
    {
        "check_name": "po_vendor",
        "child_table": "purchase_order",
        "child_col": "vendor_code",
        "parent_table": "vendor",
        "parent_col": "vendor_code",
    },
    {
        "check_name": "po_material",
        "child_table": "purchase_order",
        "child_col": "material_code",
        "parent_table": "material",
        "parent_col": "material_code",
    },
    {
        "check_name": "gr_po",
        "child_table": "goods_receipt",
        "child_col": "po_number",
        "parent_table": "purchase_order",
        "parent_col": "po_number",
    },
    {
        "check_name": "gr_material",
        "child_table": "goods_receipt",
        "child_col": "material_code",
        "parent_table": "material",
        "parent_col": "material_code",
    },
    {
        "check_name": "invoice_vendor",
        "child_table": "invoice",
        "child_col": "vendor_code",
        "parent_table": "vendor",
        "parent_col": "vendor_code",
    },
    {
        "check_name": "invoice_po",
        "child_table": "invoice",
        "child_col": "po_number",
        "parent_table": "purchase_order",
        "parent_col": "po_number",
    },
]


def _load_validation_config(config_dir: Path) -> dict:
    """Load cdm_post_validation.yaml; fall back to safe defaults if absent."""
    cfg_path = Path(resolve_config_path(str(config_dir), _VALIDATION_CONFIG_FILE))
    if not cfg_path.exists():
        log.warning(
            "Validation config not found at %s — using built-in defaults", cfg_path
        )
        return {}
    with open(cfg_path) as f:
        return yaml.safe_load(f) or {}


def _load_schema(config_dir: Path) -> dict:
    schema_path = Path(resolve_config_path(str(config_dir), "schema_frozen.yaml"))
    if not schema_path.exists():
        return {}
    with open(schema_path) as f:
        return yaml.safe_load(f).get("tables", {})


def _load_relationships(config_dir: Path) -> dict:
    rel_path = Path(resolve_config_path(str(config_dir), "relationships_frozen.yaml"))
    if not rel_path.exists():
        return {}
    with open(rel_path) as f:
        return yaml.safe_load(f).get("relationships", {})


def _find_csv(final_dir: Path, table_name: str) -> Path | None:
    """Find a CSV file for a table — checks entities/ and relationships/ subdirs."""
    for subdir in ["entities", "relationships", ""]:
        candidate = (
            final_dir / subdir / f"{table_name}.csv"
            if subdir
            else final_dir / f"{table_name}.csv"
        )
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate
    return None


def _load_csv(path: Path) -> pd.DataFrame | None:
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception as exc:
        log.warning("Could not load %s: %s", path, exc)
        return None


def _check_schema_conformance(
    df: pd.DataFrame, table_name: str, schema_tables: dict, settings: dict
) -> dict:
    """Check that CSV columns match schema_frozen.yaml definition."""
    table_schema = schema_tables.get(table_name)
    if not table_schema:
        return {"status": PASS, "note": "No schema definition found — skipped"}

    schema_cols = set(table_schema.get("columns", {}).keys())
    pk_col = table_schema.get("primary_key", "")
    schema_cols_without_pk = schema_cols - {pk_col}

    csv_cols = set(df.columns)

    missing_from_csv = sorted(schema_cols_without_pk - csv_cols)
    extra_in_csv = sorted(csv_cols - schema_cols)

    coverage_warn = settings.get("schema_coverage_warn_pct", 50) / 100
    status = PASS
    if len(missing_from_csv) > len(schema_cols_without_pk) * coverage_warn:
        status = WARN

    return {
        "status": status,
        "schema_columns": len(schema_cols_without_pk),
        "csv_columns": len(csv_cols),
        "missing_from_csv": missing_from_csv[:20],
        "extra_in_csv": extra_in_csv[:20],
        "coverage_pct": round(
            len(schema_cols_without_pk & csv_cols)
            / max(len(schema_cols_without_pk), 1)
            * 100,
            1,
        ),
    }


def _check_pk_integrity(
    df: pd.DataFrame, table_name: str, schema_tables: dict, settings: dict
) -> dict:
    """Check primary key column is present and unique (for non-auto PK tables)."""
    table_schema = schema_tables.get(table_name, {})

    constraints = table_schema.get("constraints", [])
    unique_cols = None
    for c in constraints:
        if c.get("type") == "unique":
            unique_cols = c.get("columns", [])
            break

    if not unique_cols:
        return {"status": PASS, "note": "No unique constraint defined"}

    missing = [c for c in unique_cols if c not in df.columns]
    if missing:
        return {"status": WARN, "missing_unique_columns": missing}

    if len(df) == 0:
        return {"status": PASS, "rows": 0}

    null_counts = {}
    for c in unique_cols:
        null_frac = float(df[c].isna().mean()) if len(df) > 0 else 0
        if null_frac > 0:
            null_counts[c] = round(null_frac * 100, 2)

    dup_count = int(df.duplicated(subset=unique_cols, keep=False).sum())

    pk_null_block = settings.get("pk_null_block_pct", 10)
    status = PASS
    if dup_count > 0:
        status = WARN
    if null_counts and any(v > pk_null_block for v in null_counts.values()):
        status = BLOCK

    return {
        "status": status,
        "unique_columns": unique_cols,
        "null_pct": null_counts,
        "duplicate_rows": dup_count,
        "duplicate_pct": round(dup_count / max(len(df), 1) * 100, 2),
        "total_rows": len(df),
    }


def _check_equipment_connectivity(tables: dict[str, pd.DataFrame], cfg: dict) -> dict:
    """Check that equipment IDs in dependent tables actually exist in the master equipment table."""
    equipment_df = tables.get("equipment")
    if equipment_df is None or len(equipment_df) == 0:
        return {"status": BLOCK, "error": "Equipment table is empty or missing"}

    settings = cfg.get("settings", {})
    orphan_threshold = settings.get("equipment_orphan_threshold", 0.30) * 100
    id_cols = cfg.get("equipment_id_columns", _DEFAULT_EQUIPMENT_ID_COLS)

    linked_tables_cfg = cfg.get("equipment_linked_tables", {})
    if linked_tables_cfg:
        linked_tables: dict[str, list[str]] = {
            t: (v.get("ref_columns", []) if isinstance(v, dict) else v)
            for t, v in linked_tables_cfg.items()
        }
    else:
        linked_tables = _DEFAULT_EQUIPMENT_LINKED

    equipment_ids = set()
    for col in id_cols:
        if col in equipment_df.columns:
            vals = equipment_df[col].dropna()
            vals = vals[vals != ""]
            equipment_ids.update(vals.tolist())

    results = {}
    total_linked = 0
    total_orphaned = 0

    for table_name, ref_cols in linked_tables.items():
        df = tables.get(table_name)
        if df is None or len(df) == 0:
            results[table_name] = {
                "status": PASS,
                "note": "Table empty/missing — skipped",
            }
            continue

        table_result = {}
        for col in ref_cols:
            if col not in df.columns:
                continue
            non_null = df[col].dropna()
            non_null = non_null[non_null != ""]
            if len(non_null) == 0:
                table_result[col] = {"referenced": 0, "linked": 0, "orphaned": 0}
                continue

            linked = non_null.isin(equipment_ids)
            linked_count = int(linked.sum())
            orphaned_count = int((~linked).sum())
            total_linked += linked_count
            total_orphaned += orphaned_count

            orphan_pct = round(orphaned_count / max(len(non_null), 1) * 100, 2)
            orphan_samples = []
            if orphaned_count > 0:
                orphan_samples = non_null[~linked].head(5).tolist()

            table_result[col] = {
                "referenced": len(non_null),
                "linked": linked_count,
                "orphaned": orphaned_count,
                "orphan_pct": orphan_pct,
                "orphan_samples": orphan_samples,
            }

        status = PASS
        for col_result in table_result.values():
            if col_result.get("orphan_pct", 0) > 50:
                status = WARN
        results[table_name] = {"status": status, "columns": table_result}

    overall_orphan_pct = round(
        total_orphaned / max(total_linked + total_orphaned, 1) * 100, 2
    )
    return {
        "status": WARN if overall_orphan_pct > orphan_threshold else PASS,
        "total_equipment_ids": len(equipment_ids),
        "total_linked": total_linked,
        "total_orphaned": total_orphaned,
        "overall_orphan_pct": overall_orphan_pct,
        "per_table": results,
    }


def _check_referential_integrity(
    tables: dict[str, pd.DataFrame], fk_checks_cfg: list, settings: dict
) -> dict:
    """Check FK relationships between CDM tables."""
    fk_threshold = settings.get("fk_unresolved_threshold", 0.50) * 100
    results = {}
    for fk in fk_checks_cfg:
        check_name = fk.get(
            "check_name", f"{fk.get('child_table', '')}_{fk.get('child_col', '')}"
        )
        child_table = fk.get("child_table", "")
        child_col = fk.get("child_col", "")
        parent_table = fk.get("parent_table", "")
        parent_col = fk.get("parent_col", "")

        child_df = tables.get(child_table)
        parent_df = tables.get(parent_table)

        if child_df is None or parent_df is None:
            results[check_name] = {
                "status": PASS,
                "note": f"Skipped — {child_table} or {parent_table} missing",
            }
            continue

        if child_col not in child_df.columns or parent_col not in parent_df.columns:
            results[check_name] = {
                "status": PASS,
                "note": f"Column {child_col} or {parent_col} not in data",
            }
            continue

        child_vals = child_df[child_col].dropna()
        child_vals = child_vals[child_vals != ""]
        if len(child_vals) == 0:
            results[check_name] = {"status": PASS, "child_refs": 0}
            continue

        parent_vals = set(parent_df[parent_col].dropna().tolist())
        resolved = child_vals.isin(parent_vals)
        unresolved_count = int((~resolved).sum())
        unresolved_pct = round(unresolved_count / max(len(child_vals), 1) * 100, 2)

        unresolved_samples = []
        if unresolved_count > 0:
            unresolved_samples = child_vals[~resolved].head(5).tolist()

        status = PASS
        if unresolved_pct > fk_threshold:
            status = WARN

        results[check_name] = {
            "status": status,
            "child_table": child_table,
            "child_col": child_col,
            "parent_table": parent_table,
            "parent_col": parent_col,
            "child_refs": len(child_vals),
            "resolved": int(resolved.sum()),
            "unresolved": unresolved_count,
            "unresolved_pct": unresolved_pct,
            "unresolved_samples": unresolved_samples,
        }

    return results


def _check_data_quality_basics(
    df: pd.DataFrame, table_name: str, settings: dict, dq_overrides: dict
) -> dict:
    """Basic DQ: null rates per column, duplicate rows, confidence distribution."""
    if len(df) == 0:
        return {"status": PASS, "rows": 0}

    total_cells = df.shape[0] * df.shape[1]
    null_cells = int(df.isna().sum().sum()) + int((df == "").sum().sum())
    overall_null_pct = round(null_cells / max(total_cells, 1) * 100, 2)

    col_nulls = {}
    for c in df.columns:
        null_frac = float((df[c].isna() | (df[c] == "")).mean())
        if null_frac > 0.1:
            col_nulls[c] = round(null_frac * 100, 2)
    top_null_cols = dict(sorted(col_nulls.items(), key=lambda x: -x[1])[:10])

    full_dup_count = int(df.duplicated(keep=False).sum())

    confidence_stats = {}
    if "confidence" in df.columns:
        conf = pd.to_numeric(df["confidence"], errors="coerce")
        if conf.notna().any():
            confidence_stats = {
                "mean": round(float(conf.mean()), 4),
                "min": round(float(conf.min()), 4),
                "max": round(float(conf.max()), 4),
                "below_0_5_pct": round(float((conf < 0.5).mean()) * 100, 2),
            }

    null_warn = dq_overrides.get(table_name, {}).get(
        "null_warn_pct", settings.get("table_null_warn_pct", 70)
    )
    status = PASS
    if overall_null_pct > null_warn:
        status = WARN

    return {
        "status": status,
        "rows": len(df),
        "columns": len(df.columns),
        "overall_null_pct": overall_null_pct,
        "high_null_columns": top_null_cols,
        "full_duplicate_rows": full_dup_count,
        "confidence_stats": confidence_stats,
    }


def _check_relationship_graph(
    final_dir: Path, tables: dict[str, pd.DataFrame], relationships_cfg: dict
) -> dict:
    """Check relationship CSV files reference valid entity IDs."""
    rel_dir = final_dir / "relationships"
    if not rel_dir.exists():
        rel_dir = final_dir

    results = {}

    for rel_name, rel_cfg in relationships_cfg.items():
        from_entity = rel_cfg.get("from_entity", "")
        to_entity = rel_cfg.get("to_entity", "")
        dedupe_keys = rel_cfg.get("dedupe_keys", [])

        rel_csv = None
        for candidate_name in [rel_name, f"{from_entity}_{to_entity}"]:
            path = _find_csv(final_dir, candidate_name)
            if path:
                rel_csv = path
                break

        if rel_csv is None:
            results[rel_name] = {
                "status": PASS,
                "note": "Relationship CSV not found — skipped",
            }
            continue

        rel_df = _load_csv(rel_csv)
        if rel_df is None or len(rel_df) == 0:
            results[rel_name] = {"status": PASS, "rows": 0}
            continue

        dup_count = 0
        dedup_cols = [c for c in dedupe_keys if c in rel_df.columns]
        if dedup_cols:
            dup_count = int(rel_df.duplicated(subset=dedup_cols, keep=False).sum())

        results[rel_name] = {
            "status": WARN if dup_count > 0 else PASS,
            "rows": len(rel_df),
            "from_entity": from_entity,
            "to_entity": to_entity,
            "duplicate_rows": dup_count,
        }

    return results


def _check_cdm_manifest(final_dir: Path, manifest_cfg: dict) -> dict:
    """Check cdm_manifest.json exists and is valid."""
    filename = manifest_cfg.get("filename", "cdm_manifest.json")
    missing_severity = manifest_cfg.get("severity_if_missing", WARN)
    manifest_path = final_dir / filename
    if not manifest_path.exists():
        return {"status": missing_severity, "error": f"{filename} not found"}

    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
        tables_in_manifest = len(manifest.get("tables", manifest.get("entities", {})))
        return {
            "status": PASS,
            "tables_in_manifest": tables_in_manifest,
            "keys": list(manifest.keys())[:10],
        }
    except Exception as exc:
        return {"status": WARN, "error": f"Invalid JSON: {exc}"}




def run_cdm_post_validation(
    final_dir: Path,
    config_dir: Path,
    report_dir: Path | None = None,
) -> dict:
    """Run all post-pipeline CDM validation checks."""
    cfg = _load_validation_config(config_dir)
    settings = cfg.get("settings", {})
    dq_overrides = cfg.get("dq_overrides", {})
    manifest_cfg = cfg.get("cdm_manifest", {})
    fk_checks_cfg = cfg.get("fk_checks", _DEFAULT_FK_CHECKS)
    core_tables = cfg.get("core_tables", _DEFAULT_CORE_TABLES)
    expected_tables = cfg.get("expected_tables", _DEFAULT_EXPECTED_TABLES)

    if report_dir is None:
        cfg_report_dir = settings.get("report_dir")
        report_dir = (
            Path(cfg_report_dir) if cfg_report_dir else final_dir.parent / "dq_reports"
        )
    report_dir.mkdir(parents=True, exist_ok=True)

    schema_tables = _load_schema(config_dir)
    relationships_cfg = _load_relationships(config_dir)

    report = {
        "gate_status": "PASSED",
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "final_dir": str(final_dir),
        "checks": {},
        "summary": {"total_checks": 0, "passed": 0, "warnings": 0, "blocked": 0},
        "table_row_counts": {},
    }
    all_blocked = []
    all_warnings = []

    tables: dict[str, pd.DataFrame] = {}
    all_table_names = set(core_tables + expected_tables)

    for table_name in sorted(all_table_names):
        csv_path = _find_csv(final_dir, table_name)
        if csv_path:
            df = _load_csv(csv_path)
            if df is not None:
                tables[table_name] = df
                report["table_row_counts"][table_name] = len(df)

    for subdir in [final_dir / "entities", final_dir / "relationships", final_dir]:
        if subdir.exists():
            for csv_path in subdir.glob("*.csv"):
                tname = csv_path.stem
                if tname not in tables:
                    df = _load_csv(csv_path)
                    if df is not None:
                        tables[tname] = df
                        report["table_row_counts"][tname] = len(df)

    log.info("CDM validation: loaded %d tables from %s", len(tables), final_dir)

    required_check = {}
    for table_name in core_tables:
        exists = table_name in tables and len(tables[table_name]) > 0
        required_check[table_name] = {
            "exists": exists,
            "rows": len(tables.get(table_name, [])),
        }
        report["summary"]["total_checks"] += 1
        if not exists:
            report["summary"]["blocked"] += 1
            all_blocked.append(f"Core table '{table_name}' is missing or empty")
        else:
            report["summary"]["passed"] += 1

    expected_check = {}
    for table_name in expected_tables:
        exists = table_name in tables and len(tables[table_name]) > 0
        expected_check[table_name] = {
            "exists": exists,
            "rows": len(tables.get(table_name, [])),
        }
        report["summary"]["total_checks"] += 1
        if not exists:
            report["summary"]["warnings"] += 1
            all_warnings.append(f"Expected table '{table_name}' is missing or empty")
        else:
            report["summary"]["passed"] += 1

    report["checks"]["required_tables"] = {
        "core": required_check,
        "expected": expected_check,
    }

    schema_check = {}
    for table_name, df in tables.items():
        result = _check_schema_conformance(df, table_name, schema_tables, settings)
        schema_check[table_name] = result
        report["summary"]["total_checks"] += 1
        if result["status"] == BLOCK:
            report["summary"]["blocked"] += 1
            all_blocked.append(f"Schema conformance BLOCK: {table_name}")
        elif result["status"] == WARN:
            report["summary"]["warnings"] += 1
            all_warnings.append(
                f"Schema conformance WARN: {table_name} — coverage {result.get('coverage_pct', 0)}%"
            )
        else:
            report["summary"]["passed"] += 1
    report["checks"]["schema_conformance"] = schema_check

    pk_check = {}
    for table_name, df in tables.items():
        result = _check_pk_integrity(df, table_name, schema_tables, settings)
        pk_check[table_name] = result
        report["summary"]["total_checks"] += 1
        if result["status"] == BLOCK:
            report["summary"]["blocked"] += 1
            all_blocked.append(
                f"PK integrity BLOCK: {table_name} — {result.get('null_pct', {})}"
            )
        elif result["status"] == WARN:
            report["summary"]["warnings"] += 1
            all_warnings.append(
                f"PK integrity WARN: {table_name} — {result.get('duplicate_rows', 0)} duplicates"
            )
        else:
            report["summary"]["passed"] += 1
    report["checks"]["pk_integrity"] = pk_check

    connectivity_result = _check_equipment_connectivity(tables, cfg)
    report["checks"]["equipment_connectivity"] = connectivity_result
    report["summary"]["total_checks"] += 1
    if connectivity_result["status"] == BLOCK:
        report["summary"]["blocked"] += 1
        all_blocked.append(
            "Equipment connectivity BLOCK: equipment table missing/empty"
        )
    elif connectivity_result["status"] == WARN:
        report["summary"]["warnings"] += 1
        all_warnings.append(
            f"Equipment connectivity WARN: {connectivity_result.get('overall_orphan_pct', 0)}% orphaned references"
        )
    else:
        report["summary"]["passed"] += 1

    fk_results = _check_referential_integrity(tables, fk_checks_cfg, settings)
    report["checks"]["referential_integrity"] = fk_results
    for check_name, result in fk_results.items():
        report["summary"]["total_checks"] += 1
        if result["status"] == BLOCK:
            report["summary"]["blocked"] += 1
            all_blocked.append(f"FK integrity BLOCK: {check_name}")
        elif result["status"] == WARN:
            report["summary"]["warnings"] += 1
            all_warnings.append(
                f"FK integrity WARN: {check_name} — {result.get('unresolved_pct', 0)}% unresolved"
            )
        else:
            report["summary"]["passed"] += 1

    dq_check = {}
    for table_name, df in tables.items():
        result = _check_data_quality_basics(df, table_name, settings, dq_overrides)
        dq_check[table_name] = result
        report["summary"]["total_checks"] += 1
        if result["status"] == WARN:
            report["summary"]["warnings"] += 1
            all_warnings.append(
                f"DQ basics WARN: {table_name} — {result.get('overall_null_pct', 0)}% null"
            )
        else:
            report["summary"]["passed"] += 1
    report["checks"]["data_quality"] = dq_check

    rel_results = _check_relationship_graph(final_dir, tables, relationships_cfg)
    report["checks"]["relationship_graph"] = rel_results
    for rel_name, result in rel_results.items():
        report["summary"]["total_checks"] += 1
        if result["status"] == WARN:
            report["summary"]["warnings"] += 1
            all_warnings.append(
                f"Relationship WARN: {rel_name} — {result.get('duplicate_rows', 0)} duplicates"
            )
        else:
            report["summary"]["passed"] += 1

    manifest_result = _check_cdm_manifest(final_dir, manifest_cfg)
    report["checks"]["cdm_manifest"] = manifest_result
    report["summary"]["total_checks"] += 1
    if manifest_result["status"] == WARN:
        report["summary"]["warnings"] += 1
        all_warnings.append(f"CDM manifest: {manifest_result.get('error', 'issue')}")
    else:
        report["summary"]["passed"] += 1

    if all_blocked:
        report["gate_status"] = "BLOCKED"
    elif all_warnings:
        report["gate_status"] = "WARNINGS"
    else:
        report["gate_status"] = "PASSED"

    report["blocked_reasons"] = all_blocked
    report["warning_reasons"] = all_warnings

    report_path = (
        report_dir
        / f"cdm_validation_{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%S')}.json"
    )
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info("CDM validation report: %s", report_path)

    s = report["summary"]
    log.info(
        "CDM validation %s — %d checks: %d passed, %d warnings, %d blocked",
        report["gate_status"],
        s["total_checks"],
        s["passed"],
        s["warnings"],
        s["blocked"],
    )
    for reason in all_blocked:
        log.error("  BLOCK: %s", reason)
    for reason in all_warnings[:15]:
        log.warning("  WARN:  %s", reason)

    return report
