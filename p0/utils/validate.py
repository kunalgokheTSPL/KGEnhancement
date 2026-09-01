from __future__ import annotations
import json
import pandas as pd

CONTRACT_SECTIONS = ("required_after_rename_or_derive", "datasets")
ALL_NULL = 1.0


def _dataset_contract(name: str, validation_cfg: dict) -> dict:
    """Resolve one dataset's contract across the shapes the configs actually ship."""
    for section_name in CONTRACT_SECTIONS:
        section = validation_cfg.get(section_name) or {}
        if not isinstance(section, dict):
            continue
        entry = section.get(name)
        if isinstance(entry, list):
            return {"required_columns": entry}
        if isinstance(entry, dict):
            return entry
    entry = validation_cfg.get(name)
    if isinstance(entry, list):
        return {"required_columns": entry}
    return entry if isinstance(entry, dict) else {}


def _required_columns(ds_cfg: dict) -> list[str]:
    """The columns a dataset must carry to be usable downstream."""
    return list(ds_cfg.get("required_columns") or ds_cfg.get("required") or [])


def _null_limit(ds_cfg: dict, validation_cfg: dict) -> float:
    """Null fraction at which a required column counts as unpopulated."""
    raw = ds_cfg.get(
        "max_null_fraction", validation_cfg.get("max_null_fraction", ALL_NULL)
    )
    try:
        limit = float(raw)
    except (TypeError, ValueError):
        return ALL_NULL
    return limit if 0.0 < limit <= 1.0 else ALL_NULL


def _dataset_problems(
    df: pd.DataFrame, required: list[str], limit: float
) -> tuple[list[str], list[str], dict[str, float]]:
    """Every reason this dataset is not usable, with the numbers behind them."""
    problems: list[str] = []
    missing = [c for c in required if c not in df.columns]
    if missing:
        problems.append(f"missing required columns: {', '.join(missing)}")

    present = [c for c in required if c in df.columns]
    null_stats: dict[str, float] = {}
    if len(df) == 0:
        problems.append("produced 0 rows")
        null_stats = {c: 1.0 for c in present}
        return problems, missing, null_stats

    for c in present:
        fraction = float(df[c].isna().mean())
        null_stats[c] = fraction
        if fraction >= limit:
            problems.append(f"{c} is {fraction:.0%} null, at or over the {limit:.0%} limit")

    if not required:
        problems.append("no contract for this dataset, so nothing was checked")

    return problems, missing, null_stats


def run_validations(datasets: dict[str, pd.DataFrame], validation_cfg: dict) -> dict:
    """Check every dataset against its contract and report every reason it is unusable."""
    report: dict = {"status": "PASS", "datasets": {}, "problems": []}

    if not datasets:
        report["status"] = "FAIL"
        report["problems"].append("no datasets reached validation")
        return report

    for name, df in datasets.items():
        ds_cfg = _dataset_contract(name, validation_cfg)
        required = _required_columns(ds_cfg)
        problems, missing, null_stats = _dataset_problems(
            df, required, _null_limit(ds_cfg, validation_cfg)
        )

        report["datasets"][name] = {
            "rows": int(len(df)),
            "missing_required_columns": missing,
            "required_null_fraction": null_stats,
            "problems": problems,
            "status": "FAIL" if problems else "PASS",
        }
        report["problems"].extend(f"{name}: {p}" for p in problems)

    if report["problems"]:
        report["status"] = "FAIL"

    return report


def log_validation_report(report: dict, log) -> None:
    """Put every validation problem in the run log, where a failed run is visible."""
    if report.get("status") == "PASS":
        log.info("Validation passed for %d dataset(s)", len(report.get("datasets", {})))
        return
    problems = report.get("problems", [])
    log.error("VALIDATION FAILED - %d problem(s), the run is not trustworthy", len(problems))
    for problem in problems:
        log.error("  %s", problem)


def report_to_json(report: dict) -> str:
    return json.dumps(report, indent=2)
