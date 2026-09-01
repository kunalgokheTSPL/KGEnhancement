"""Staging quality gate: data checks between download and pipeline."""

from __future__ import annotations

import chardet
import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from p0.utils.config_resolver import resolve_config_path
from p0.utils.tabular import read_table

log = logging.getLogger("p0-cdm-worker.staging-dq")

BLOCK = "BLOCK"
WARN = "WARN"
PASS = "PASS"


def run_staging_gate_or_raise(
    config_dir: str | Path,
    staging_dir: Path,
    plant_code_id: str,
    report_dir: Path | None = None,
) -> dict:
    """Resolve the staging-gate config and run it, raising rather than skipping."""
    config_path = Path(resolve_config_path(str(config_dir), "staging_quality_gate.yaml"))
    if not config_path.exists():
        raise RuntimeError(
            f"Staging quality gate config not found at {config_path} — refusing to run "
            f"the pipeline ungated (packaging/config error)."
        )
    report = run_staging_quality_gate(
        staging_dir=staging_dir,
        config_path=config_path,
        plant_code_id=plant_code_id,
        report_dir=report_dir,
    )
    if report["gate_status"] == "BLOCKED":
        raise RuntimeError(
            f"Staging quality gate BLOCKED for plant {plant_code_id}: "
            + "; ".join(report["blocked_reasons"][:5])
        )
    return report

TABULAR_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def _load_config(config_path: Path) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)




def _check_encoding(file_path: Path, expected: str = "utf-8") -> dict:
    """Detect file encoding; flag if not matching expected."""
    try:
        raw = file_path.read_bytes()[:10_000]
        result = chardet.detect(raw)
        detected = (result.get("encoding") or "").lower().replace("-", "")
        expected_norm = expected.lower().replace("-", "")
        is_ok = detected in (expected_norm, "ascii", "utf8")
        return {
            "detected_encoding": result.get("encoding"),
            "confidence": result.get("confidence"),
            "matches_expected": is_ok,
        }
    except Exception as exc:
        return {"detected_encoding": None, "error": str(exc), "matches_expected": False}


def _check_csv_parseable(file_path: Path, delimiter: str = ",") -> dict:
    try:
        with open(file_path, newline="", encoding="utf-8", errors="replace") as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader, None)
            if header is None:
                return {
                    "parseable": False,
                    "error": "Empty file or no header",
                    "headers": [],
                    "row_count": 0,
                }
            row_count = sum(1 for _ in reader)
        return {"parseable": True, "headers": header, "row_count": row_count}
    except Exception as exc:
        return {"parseable": False, "error": str(exc), "headers": [], "row_count": 0}


def _check_xlsx_valid(file_path: Path) -> dict:
    try:
        import zipfile

        if not zipfile.is_zipfile(file_path):
            return {
                "valid": False,
                "parseable": False,
                "error": "Not a valid ZIP/XLSX file",
            }
        with zipfile.ZipFile(file_path) as z:
            names = z.namelist()
        has_xl = any(n.startswith("xl/") for n in names)
        return {"valid": has_xl, "parseable": has_xl, "entries": len(names)}
    except Exception as exc:
        return {"valid": False, "parseable": False, "error": str(exc)}


def _check_pdf_valid(file_path: Path) -> dict:
    try:
        with open(file_path, "rb") as f:
            magic = f.read(5)
        is_pdf = magic == b"%PDF-"
        size = file_path.stat().st_size
        valid = is_pdf and size > 100
        return {"valid": valid, "parseable": valid, "size_bytes": size}
    except Exception as exc:
        return {"valid": False, "parseable": False, "error": str(exc)}


def _check_docx_valid(file_path: Path) -> dict:
    try:
        import zipfile

        if not zipfile.is_zipfile(file_path):
            return {
                "valid": False,
                "parseable": False,
                "error": "Not a valid ZIP/DOCX file",
            }
        with zipfile.ZipFile(file_path) as z:
            names = z.namelist()
        has_word = any(n.startswith("word/") for n in names)
        return {"valid": has_word, "parseable": has_word, "entries": len(names)}
    except Exception as exc:
        return {"valid": False, "parseable": False, "error": str(exc)}


def _check_doc_valid(file_path: Path) -> dict:
    try:
        with open(file_path, "rb") as f:
            magic = f.read(8)
        is_ole = magic == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
        size = file_path.stat().st_size
        valid = is_ole and size > 512
        return {"valid": valid, "parseable": valid, "size_bytes": size}
    except Exception as exc:
        return {"valid": False, "parseable": False, "error": str(exc)}


def _validate_file_format(file_path: Path, delimiter: str = ",") -> dict:
    """Auto-detect and validate based on extension."""
    ext = file_path.suffix.lower()
    if ext == ".csv":
        return _check_csv_parseable(file_path, delimiter)
    elif ext in (".xlsx", ".xls"):
        return _check_xlsx_valid(file_path)
    elif ext == ".pdf":
        return _check_pdf_valid(file_path)
    elif ext == ".docx":
        return _check_docx_valid(file_path)
    elif ext == ".doc":
        return _check_doc_valid(file_path)
    return {"parseable": False, "error": f"Unsupported format: {ext}"}


def _load_tabular_file(file_path: Path, delimiter: str = ",") -> pd.DataFrame | None:
    """Load CSV / XLSX / XLS into a DataFrame."""
    ext = file_path.suffix.lower()
    if ext not in (".csv", ".xlsx", ".xls"):
        return None
    try:
        df, _, _ = read_table(
            file_path, keep_default_na=False, delimiter=delimiter
        )
        return df
    except Exception as exc:
        log.warning("Could not load %s: %s", file_path, exc)
    return None


def _find_file_by_stem(
    directory: Path, stem: str, accepted_exts: list[str] | None = None
) -> Path | None:
    """Find a file by stem name, trying multiple extensions."""
    if accepted_exts is None:
        accepted_exts = [".csv", ".xlsx", ".xls"]
    for ext in accepted_exts:
        candidate = directory / f"{stem}{ext}"
        if candidate.exists():
            return candidate
    return None




def _check_natural_keys(
    df: pd.DataFrame, keys: list[str], null_threshold: float
) -> dict:
    missing_keys = [k for k in keys if k not in df.columns]
    if missing_keys:
        return {
            "status": BLOCK,
            "missing_key_columns": missing_keys,
            "null_fractions": {},
            "duplicate_count": 0,
        }

    null_fractions = {}
    any_blocked = False
    for k in keys:
        null_frac = float(df[k].isna().mean()) if len(df) > 0 else 1.0
        null_fractions[k] = round(null_frac, 4)
        if null_frac > null_threshold:
            any_blocked = True

    dup_count = 0
    if len(df) > 0:
        dup_count = int(df.duplicated(subset=keys, keep=False).sum())

    return {
        "status": BLOCK if any_blocked else PASS,
        "missing_key_columns": [],
        "null_fractions": null_fractions,
        "duplicate_count": dup_count,
        "duplicate_pct": round(dup_count / max(len(df), 1) * 100, 2),
    }


def _check_type_conformance(df: pd.DataFrame, type_checks: dict) -> dict:
    results = {}
    for col, expected_type in type_checks.items():
        if col not in df.columns:
            results[col] = {"status": "MISSING", "violations": 0}
            continue
        violations = 0
        sample_violations = []
        if expected_type == "date":
            parsed = pd.to_datetime(df[col], errors="coerce")
            violations = int(parsed.isna().sum() - df[col].isna().sum())
            if violations > 0:
                bad_mask = parsed.isna() & df[col].notna()
                sample_violations = df.loc[bad_mask, col].head(5).tolist()
        elif expected_type in ("numeric", "float", "int"):
            parsed = pd.to_numeric(df[col], errors="coerce")
            violations = int(parsed.isna().sum() - df[col].isna().sum())
            if violations > 0:
                bad_mask = parsed.isna() & df[col].notna()
                sample_violations = df.loc[bad_mask, col].head(5).tolist()
        results[col] = {
            "expected_type": expected_type,
            "violations": violations,
            "violation_pct": round(violations / max(len(df), 1) * 100, 2),
            "sample_violations": sample_violations,
        }
    return results


def _check_header_schema(
    actual_headers: list[str], required_columns: list[str]
) -> dict:
    actual_set = {h.strip() for h in actual_headers}
    missing = [c for c in required_columns if c not in actual_set]
    extra = sorted(actual_set - set(required_columns))
    return {
        "missing_required": missing,
        "extra_columns": extra[:20],
        "status": BLOCK if missing else PASS,
    }


def _load_previous_manifest(manifest_path: Path) -> dict:
    if manifest_path.exists():
        try:
            with open(manifest_path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _check_drift(
    current_counts: dict[str, int], previous_manifest: dict, threshold: float
) -> dict:
    prev_counts = previous_manifest.get("row_counts", {})
    if not prev_counts:
        return {"status": "NO_BASELINE", "comparisons": {}}
    comparisons = {}
    for name, current in current_counts.items():
        prev = prev_counts.get(name)
        if prev is None or prev == 0:
            comparisons[name] = {"current": current, "previous": None, "drift": None}
            continue
        drift = abs(current - prev) / prev
        comparisons[name] = {
            "current": current,
            "previous": prev,
            "drift_pct": round(drift * 100, 1),
            "alert": drift > threshold,
        }
    return {
        "status": WARN if any(c.get("alert") for c in comparisons.values()) else PASS,
        "comparisons": comparisons,
    }




def run_staging_quality_gate(
    staging_dir: Path,
    config_path: Path,
    plant_code_id: str,
    report_dir: Path | None = None,
    previous_manifest: dict | None = None,
) -> dict:
    cfg = _load_config(config_path)
    settings = cfg.get("settings", {})
    sources_cfg = cfg.get("sources", {})
    drift_cfg = cfg.get("drift", {})

    null_threshold = settings.get("natural_key_null_threshold", 0.02)
    drift_threshold = settings.get("drift_threshold", 0.50)
    expected_encoding = settings.get("encoding", "utf-8")

    if report_dir is None:
        report_dir = Path(settings.get("report_dir", "data/out/dq_reports"))
    report_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "gate_status": "PASSED",
        "plant_code_id": plant_code_id,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "checks": {},
        "summary": {"total_checks": 0, "passed": 0, "warnings": 0, "blocked": 0},
        "row_counts": {},
    }
    all_blocked = []
    all_warnings = []

    for source_name, src_cfg in sources_cfg.items():
        subdir = src_cfg.get("staging_subdir", source_name)
        source_dir = staging_dir / subdir
        delimiter = src_cfg.get("delimiter", ",")
        accepted_exts = src_cfg.get("accepted_extensions", [".csv"])
        source_report: dict = {}

        log.info("DQ gate — checking source: %s  dir: %s", source_name, source_dir)

        for req_file in src_cfg.get("required_files", []):
            fpath = source_dir / req_file
            exists = fpath.exists() and fpath.stat().st_size > 0
            if not exists:
                alt = _find_file_by_stem(source_dir, Path(req_file).stem, accepted_exts)
                if alt:
                    exists = True
                    fpath = alt
            result = {
                "check": "file_existence",
                "file": req_file,
                "resolved": fpath.name if exists else None,
                "exists": exists,
                "severity": BLOCK if not exists else PASS,
            }
            source_report.setdefault(req_file, {})["file_existence"] = result
            report["summary"]["total_checks"] += 1
            if not exists:
                report["summary"]["blocked"] += 1
                all_blocked.append(f"{source_name}/{req_file}: required file missing")
            else:
                report["summary"]["passed"] += 1

        min_count = src_cfg.get("min_file_count")
        if min_count is not None and source_dir.exists():
            matching = [
                f
                for f in source_dir.iterdir()
                if f.is_file() and f.suffix.lower() in set(accepted_exts)
            ]
            result = {
                "check": "min_file_count",
                "found": len(matching),
                "required": min_count,
                "severity": BLOCK if len(matching) < min_count else PASS,
            }
            source_report.setdefault("_source_level", {})["min_file_count"] = result
            report["summary"]["total_checks"] += 1
            if len(matching) < min_count:
                report["summary"]["blocked"] += 1
                all_blocked.append(
                    f"{source_name}: need ≥{min_count} files, found {len(matching)}"
                )
            else:
                report["summary"]["passed"] += 1

        if not source_dir.exists():
            report["checks"][source_name] = source_report
            continue

        datasets_cfg = src_cfg.get("datasets", {})
        files_to_check: list[tuple[Path, str | None]] = []
        for ds_name, ds_cfg in datasets_cfg.items():
            file_stem = ds_cfg.get("file_stem", ds_name)
            fpath = _find_file_by_stem(source_dir, file_stem, accepted_exts)
            if fpath:
                files_to_check.append((fpath, ds_name))

        checked_paths = {p for p, _ in files_to_check}
        for fpath in source_dir.iterdir():
            if (
                fpath.is_file()
                and fpath.suffix.lower() in set(accepted_exts)
                and fpath not in checked_paths
            ):
                files_to_check.append((fpath, None))

        for fpath, ds_name in files_to_check:
            fname = fpath.name
            ext = fpath.suffix.lower()
            file_checks: dict = source_report.setdefault(fname, {})

            fmt_result = _validate_file_format(fpath, delimiter)
            file_checks["format_validation"] = fmt_result
            report["summary"]["total_checks"] += 1
            if not fmt_result.get("parseable", False):
                report["summary"]["blocked"] += 1
                all_blocked.append(
                    f"{source_name}/{fname}: format invalid — {fmt_result.get('error', 'unparseable')}"
                )
            else:
                report["summary"]["passed"] += 1

            if ext == ".csv":
                enc_result = _check_encoding(fpath, expected_encoding)
                file_checks["encoding"] = enc_result
                report["summary"]["total_checks"] += 1
                if not enc_result["matches_expected"]:
                    report["summary"]["warnings"] += 1
                    all_warnings.append(
                        f"{source_name}/{fname}: encoding={enc_result.get('detected_encoding')}"
                    )
                else:
                    report["summary"]["passed"] += 1

            if fpath.stat().st_size == 0:
                file_checks["empty_file"] = {"size_bytes": 0, "severity": BLOCK}
                report["summary"]["total_checks"] += 1
                report["summary"]["blocked"] += 1
                all_blocked.append(f"{source_name}/{fname}: zero-byte file")
                continue

            if ds_name is None or ext not in TABULAR_EXTENSIONS:
                continue
            ds_cfg = datasets_cfg.get(ds_name, {})
            if not ds_cfg:
                continue

            df = _load_tabular_file(fpath, delimiter)
            if df is None:
                file_checks["load_error"] = "Could not load as tabular data"
                continue

            row_count = len(df)
            report["row_counts"][f"{source_name}.{ds_name}"] = row_count

            req_cols = ds_cfg.get("required_columns", [])
            if req_cols:
                header_result = _check_header_schema(list(df.columns), req_cols)
                file_checks["header_schema"] = header_result
                report["summary"]["total_checks"] += 1
                if header_result["status"] == BLOCK:
                    report["summary"]["blocked"] += 1
                    all_blocked.append(
                        f"{source_name}/{fname}: missing columns {header_result['missing_required']}"
                    )
                else:
                    report["summary"]["passed"] += 1

            nat_keys = ds_cfg.get("natural_keys", [])
            if nat_keys:
                key_result = _check_natural_keys(df, nat_keys, null_threshold)
                file_checks["natural_keys"] = key_result
                report["summary"]["total_checks"] += 1
                if key_result["status"] == BLOCK:
                    report["summary"]["blocked"] += 1
                    all_blocked.append(
                        f"{source_name}/{fname}: natural key nulls exceed {null_threshold}"
                    )
                else:
                    report["summary"]["passed"] += 1
                if key_result.get("duplicate_count", 0) > 0:
                    report["summary"]["warnings"] += 1
                    all_warnings.append(
                        f"{source_name}/{fname}: {key_result['duplicate_count']} duplicate key rows"
                    )

            type_checks = ds_cfg.get("type_checks", {})
            if type_checks:
                type_result = _check_type_conformance(df, type_checks)
                file_checks["type_conformance"] = type_result
                report["summary"]["total_checks"] += 1
                has_violations = any(v["violations"] > 0 for v in type_result.values())
                if has_violations:
                    report["summary"]["warnings"] += 1
                    for col, res in type_result.items():
                        if res["violations"] > 0:
                            all_warnings.append(
                                f"{source_name}/{fname}.{col}: {res['violations']} type violations"
                            )
                else:
                    report["summary"]["passed"] += 1

        report["checks"][source_name] = source_report

    if drift_cfg.get("enabled", False):
        if previous_manifest is None:
            manifest_path = Path(
                drift_cfg.get(
                    "manifest_path", "data/out/dq_reports/previous_manifest.json"
                )
            )
            previous_manifest = _load_previous_manifest(manifest_path)
        drift_result = _check_drift(
            report["row_counts"], previous_manifest, drift_threshold
        )
        report["drift"] = drift_result
        if drift_result["status"] == WARN:
            for name, comp in drift_result["comparisons"].items():
                if comp.get("alert"):
                    all_warnings.append(
                        f"DRIFT {name}: {comp['previous']}→{comp['current']} rows ({comp['drift_pct']}%)"
                    )

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
        / f"staging_dq_{plant_code_id}_{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%S')}.json"
    )
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info("DQ report written: %s", report_path)

    manifest_out = report_dir / "previous_manifest.json"
    with open(manifest_out, "w") as f:
        json.dump(
            {
                "plant_code_id": plant_code_id,
                "timestamp": report["timestamp"],
                "row_counts": report["row_counts"],
            },
            f,
            indent=2,
        )

    s = report["summary"]
    log.info(
        "DQ gate %s — %d checks: %d passed, %d warnings, %d blocked",
        report["gate_status"],
        s["total_checks"],
        s["passed"],
        s["warnings"],
        s["blocked"],
    )
    for reason in all_blocked:
        log.error("  BLOCK: %s", reason)
    for reason in all_warnings[:10]:
        log.warning("  WARN:  %s", reason)

    return report
