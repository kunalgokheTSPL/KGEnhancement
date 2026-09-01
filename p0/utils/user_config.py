"""
User-specific configuration loader.

Loads user_config.yaml and merges it with the frozen default configs,
giving priority to user-defined values.

Usage:
    from src.utils.user_config import load_user_config, merge_doc_types, get_sap_join_columns

    user_cfg = load_user_config(config_dir)
    doc_types = merge_doc_types(default_doc_types, user_cfg)
    join_cols = get_sap_join_columns(user_cfg, "workorder", "aufk_afko", default=["AUFNR"])
"""

from __future__ import annotations

import copy
import os

import yaml

USER_CONFIG_FILENAME = "user_config.yaml"


def load_user_config(config_dir: str, plant_code_id: str | None = None) -> dict:
    """Load user_config.yaml from config_dir, then layer in DB-stored
    sap_processing overrides for plant_code_id on top (DB wins). If the DB
    is unavailable or has no overrides for this plant, falls back to the
    YAML-only config silently.
    """
    path = os.path.join(config_dir, USER_CONFIG_FILENAME)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    else:
        cfg = {}

    if plant_code_id:
        try:
            from p0.api.services.user_config_store import (
                PLANT_SCOPE,
                deep_merge,
                read_layer,
            )
        except ImportError:
            # The DB-backed config store is an optional dependency of this
            # loader (e.g. minimal/test environments without p0.api.services);
            # falling back to YAML-only config is the intended behavior here.
            # read_layer() itself already catches and logs DB outages and
            # returns {}, so we must not also swallow exceptions below —
            # doing so previously hid real deep_merge bugs.
            pass
        else:
            db_layer = read_layer(plant_code_id, PLANT_SCOPE)
            db_sap_processing = db_layer.get("sap_processing") if isinstance(db_layer, dict) else None
            if db_sap_processing:
                cfg["sap_processing"] = deep_merge(cfg.get("sap_processing", {}), db_sap_processing)

    return cfg


def save_user_config(config_dir: str, cfg: dict) -> None:
    """Write user_config.yaml to config_dir."""
    path = os.path.join(config_dir, USER_CONFIG_FILENAME)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def merge_doc_types(
    default_doc_types: dict[str, dict],
    user_cfg: dict,
) -> dict[str, dict]:
    """Merge user-defined document types with defaults."""
    merged = copy.deepcopy(default_doc_types)
    user_doc_types = user_cfg.get("document_processing", {}).get("document_types", {})
    for type_name, type_cfg in user_doc_types.items():
        if type_name in merged:
            merged[type_name] = {**merged[type_name], **type_cfg}
        else:
            merged[type_name] = type_cfg
    return merged


def get_sap_column_renames(
    user_cfg: dict,
    source_key: str,
    plant_code_id: str | None = None,
) -> dict[str, str]:
    """Get user column rename overrides for a SAP source key.

    Overrides are stored per-plant: column_rename_overrides[plant_code_id][source_key]["mappings"].
    When plant_code_id is None, falls back to the old flat structure for backward compat.
    Returns an empty dict if no overrides exist.
    """
    all_overrides = user_cfg.get("sap_processing", {}).get("column_rename_overrides", {})
    if plant_code_id is not None:
        plant_section = all_overrides.get(plant_code_id, {})
    else:
        plant_section = all_overrides
    return plant_section.get(source_key, {}).get("mappings", {})


def get_sap_join_columns(
    user_cfg: dict,
    dataset: str,
    merge_step: str,
    default: list[str],
    plant_code_id: str | None = None,
) -> list[str]:
    """Get the join columns for a SAP merge step.

    Overrides are stored per-plant: join_overrides[plant_code_id][dataset][merge_step].
    Callers that supply plant_code_id get per-plant overrides; omitting it falls back
    to the old flat structure for backward compatibility with non-plant-aware callers.
    """
    all_overrides = user_cfg.get("sap_processing", {}).get("join_overrides", {})
    if plant_code_id is not None:
        plant_section = all_overrides.get(plant_code_id, {})
    else:
        plant_section = all_overrides
    user_cols = plant_section.get(dataset, {}).get(merge_step)
    if user_cols and isinstance(user_cols, list) and len(user_cols) > 0:
        return user_cols
    return default


def get_sap_equipment_id_column(
    user_cfg: dict,
    source_key: str,
    default: list[str],
    plant_code_id: str | None = None,
) -> list[str]:
    """Get the candidate equipment-ID column(s) for a SAP source.

    Overrides are stored per-plant: equipment_id_columns[plant_code_id][source_key].
    Callers that supply plant_code_id get per-plant overrides; omitting it falls back
    to the old flat structure for backward compatibility with non-plant-aware callers.
    """
    all_overrides = user_cfg.get("sap_processing", {}).get("equipment_id_columns", {})
    if plant_code_id is not None:
        plant_section = all_overrides.get(plant_code_id, {})
    else:
        plant_section = all_overrides
    user_cols = plant_section.get(source_key)
    if user_cols and isinstance(user_cols, list) and len(user_cols) > 0:
        return user_cols
    return default
