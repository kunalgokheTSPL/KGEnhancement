"""
Config path resolver — maps legacy filenames to templates/_common/.

Pipeline code references config files by name (e.g. "schema_frozen.yaml").
The actual standardized templates live in config/templates/_common/ with
different names (e.g. "schema.yaml"). This module resolves the mapping
so pipeline code doesn't need to change.
"""

from __future__ import annotations

import os
from pathlib import Path

_TEMPLATE_MAP: dict[str, str] = {
    "cdm_config_frozen.yaml": "templates/_common/cdm_config.yaml",
    "entities_frozen.yaml": "templates/_common/entities.yaml",
    "schema_frozen.yaml": "templates/_common/schema.yaml",
    "relationships_frozen.yaml": "templates/_common/relationships.yaml",
    "identity_frozen.yaml": "templates/_common/identity_resolution.yaml",
    "priority_frozen.yaml": "templates/_common/priority.yaml",
    "derived_fields.yaml": "templates/_common/derived_fields.yaml",
    "validation_contracts.yaml": "templates/_common/validation_contracts.yaml",
    "sap_column_rename.yaml": "templates/_common/column_rename/sap_column_rename.yaml",
    "docs_column_rename.yaml": "templates/_common/column_rename/docs_column_rename.yaml",
    "pnid_column_rename.yaml": "templates/_common/column_rename/pnid_column_rename.yaml",
    "fmea_column_rename.yaml": "templates/_common/column_rename/fmea_column_rename.yaml",
    "timeseries_column_rename.yaml": "templates/_common/column_rename/timeseries_column_rename.yaml",
    "sap_extraction.yaml": "templates/_common/sap_extraction.yaml",
    "cdm_post_validation.yaml": "templates/_common/cdm_post_validation.yaml",
    "staging_quality_gate.yaml": "templates/_common/staging_quality_gate.yaml",
    "ontology_template.yaml": "templates/_common/ontology_template.yaml",
    "tag_naming_convention.json": "templates/_common/tag_naming_convention.json",
}


def resolve_config_path(config_dir: str | Path, filename: str) -> str:
    """Resolve a config filename to its actual path."""
    config_dir = str(config_dir)
    direct = os.path.join(config_dir, filename)
    if os.path.exists(direct):
        return direct
    mapped = _TEMPLATE_MAP.get(filename)
    if mapped:
        resolved = os.path.join(config_dir, mapped)
        if os.path.exists(resolved):
            return resolved
    return direct


def merge_industry_overlay(
    config_dir: str | Path,
    base_cfg: dict,
    industry: str,
    config_type: str,
) -> dict:
    """Deep-merge an industry overlay into the base config."""
    import copy
    import yaml

    overlay_path = (
        Path(config_dir) / "templates" / industry / f"{config_type}_overlay.yaml"
    )
    if not overlay_path.exists():
        return base_cfg

    with open(overlay_path, "r") as f:
        overlay_cfg = yaml.safe_load(f) or {}

    remapped: dict = {}
    for key, val in overlay_cfg.items():
        base_key = key.removesuffix("_overlay")
        remapped[base_key] = val

    return _deep_merge(copy.deepcopy(base_cfg), remapped)


def _deep_merge(base: dict, overlay: dict) -> dict:
    """Recursively merge *overlay* into *base*; overlay wins on conflicts."""
    for key, oval in overlay.items():
        if key in base and isinstance(base[key], dict) and isinstance(oval, dict):
            _deep_merge(base[key], oval)
        elif key in base and isinstance(base[key], list) and isinstance(oval, list):
            base[key].extend(oval)
        elif key in base and isinstance(base[key], dict) and isinstance(oval, list):
            for item in oval:
                if isinstance(item, dict) and "name" in item:
                    base[key][item["name"]] = {
                        k: v for k, v in item.items() if k != "name"
                    }
        else:
            base[key] = oval
    return base
