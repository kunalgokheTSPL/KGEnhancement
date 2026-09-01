"""User config as durable layers: standard template <- global override <- plant override."""

from __future__ import annotations

import json
import logging

from ..database.connection import _get_conn

_log = logging.getLogger("p0.user_config")

GLOBAL_SCOPE = "global"
PLANT_SCOPE = "plant"
GLOBAL_KEY = "__global__"


def deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base without mutating either."""
    result = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _as_dict(value) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def _scope_key(plant_code_id: str | None, scope: str) -> str:
    return GLOBAL_KEY if scope == GLOBAL_SCOPE else (plant_code_id or "")


def read_layer(plant_code_id: str, scope: str) -> dict:
    """One stored override layer; an absent layer is an empty dict, never an error."""
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT config, version FROM user_config "
                "WHERE plant_code_id = %s AND scope = %s AND COALESCE(is_active, TRUE)",
                (_scope_key(plant_code_id, scope), scope),
            )
            row = cur.fetchone()
            if not row:
                return {}
            return _as_dict(row[0])
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[user_config] read %s layer failed: %s", scope, exc)
        return {}


def layer_version(plant_code_id: str, scope: str) -> int:
    """Current version of a layer, 0 when it has never been written."""
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COALESCE(version, 0) FROM user_config "
                "WHERE plant_code_id = %s AND scope = %s",
                (_scope_key(plant_code_id, scope), scope),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            conn.close()
    except Exception:
        return 0


def resolve(plant_code_id: str, template: dict | None = None) -> dict:
    """The effective config: template first, then global, then this plant."""
    effective = dict(template or {})
    effective = deep_merge(effective, read_layer(plant_code_id, GLOBAL_SCOPE))
    effective = deep_merge(effective, read_layer(plant_code_id, PLANT_SCOPE))
    return effective


def describe(plant_code_id: str, template: dict | None = None) -> dict:
    """Every layer plus the resolved result, so the UI can show what came from where."""
    template_layer = dict(template or {})
    global_layer = read_layer(plant_code_id, GLOBAL_SCOPE)
    plant_layer = read_layer(plant_code_id, PLANT_SCOPE)
    effective = deep_merge(deep_merge(template_layer, global_layer), plant_layer)
    return {
        "plant_code_id": plant_code_id,
        "layers": {
            "template": template_layer,
            "global": global_layer,
            "plant": plant_layer,
        },
        "config": effective,
        "versions": {
            "global": layer_version(plant_code_id, GLOBAL_SCOPE),
            "plant": layer_version(plant_code_id, PLANT_SCOPE),
        },
        "is_customised": bool(global_layer or plant_layer),
    }


def _record_history(
    cur,
    plant_code_id: str,
    scope: str,
    action: str,
    patch: dict,
    before: dict,
    after: dict,
    version: int,
    actor: str | None,
) -> None:
    """Append-only trail: what changed, who changed it, and what it looked like before."""
    section = next(iter(patch), None) if isinstance(patch, dict) else None
    cur.execute(
        "INSERT INTO user_config_history "
        "(plant_code_id, scope, action, section, patch, config_before, config_after, "
        " version_after, actor, ts) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
        (
            _scope_key(plant_code_id, scope),
            scope,
            action,
            section,
            json.dumps(patch or {}),
            json.dumps(before or {}),
            json.dumps(after or {}),
            version,
            actor,
        ),
    )


def write_layer(
    plant_code_id: str,
    scope: str,
    patch: dict,
    *,
    actor: str | None = None,
    replace: bool = False,
) -> dict:
    """Merge a patch into one layer (or replace it), keeping the previous value in history."""
    key = _scope_key(plant_code_id, scope)
    action = "replace" if replace else "patch"
    conn = _get_conn(plant_code_id)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT config, COALESCE(version, 0) FROM user_config "
            "WHERE plant_code_id = %s AND scope = %s FOR UPDATE",
            (key, scope),
        )
        row = cur.fetchone()
        before = _as_dict(row[0]) if row else {}
        version = (int(row[1]) if row else 0) + 1
        after = dict(patch or {}) if replace else deep_merge(before, patch or {})

        if row:
            cur.execute(
                "UPDATE user_config SET config = %s, version = %s, is_active = TRUE, "
                "updated_by = %s, updated_at = NOW() "
                "WHERE plant_code_id = %s AND scope = %s",
                (json.dumps(after), version, actor, key, scope),
            )
        else:
            cur.execute(
                "INSERT INTO user_config "
                "(plant_code_id, scope, config, version, is_active, created_by, created_at, "
                " updated_by, updated_at) "
                "VALUES (%s, %s, %s, %s, TRUE, %s, NOW(), %s, NOW())",
                (key, scope, json.dumps(after), version, actor, actor),
            )

        _record_history(cur, plant_code_id, scope, action, patch, before, after, version, actor)
        conn.commit()
        return after
    finally:
        conn.close()


def initialise_plant(plant_code_id: str, actor: str | None = None) -> None:
    """Create the empty per-plant layer when a plant is provisioned."""
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO user_config "
                "(plant_code_id, scope, config, version, is_active, created_by, created_at, "
                " updated_by, updated_at) "
                "VALUES (%s, %s, %s, 0, TRUE, %s, NOW(), %s, NOW()) "
                "ON CONFLICT (plant_code_id, scope) DO NOTHING",
                (plant_code_id, PLANT_SCOPE, json.dumps({}), actor, actor),
            )
            _record_history(cur, plant_code_id, PLANT_SCOPE, "init", {}, {}, {}, 0, actor)
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[user_config] init for %s skipped: %s", plant_code_id, exc)


def history(plant_code_id: str, limit: int = 50) -> list[dict]:
    """Recent changes to either layer, newest first."""
    columns = (
        "history_uid",
        "plant_code_id",
        "scope",
        "action",
        "section",
        "patch",
        "version_after",
        "actor",
        "ts",
    )
    try:
        conn = _get_conn(plant_code_id)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT {', '.join(columns)} FROM user_config_history "
                "WHERE plant_code_id IN (%s, %s) ORDER BY ts DESC LIMIT %s",
                (plant_code_id, GLOBAL_KEY, limit),
            )
            rows = []
            for record in cur.fetchall():
                entry = dict(zip(columns, record))
                entry["ts"] = str(entry["ts"]) if entry["ts"] is not None else None
                rows.append(entry)
            return rows
        finally:
            conn.close()
    except Exception as exc:
        _log.warning("[user_config] history read failed: %s", exc)
        return []
