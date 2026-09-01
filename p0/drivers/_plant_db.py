"""Per-plant database naming p0 owns.

p0 provisions every plant as ``decisionops_<normalized_plant_code>`` (see
``p0.api.plants.plant_db_name``). The upstream ``utility.drivers`` Postgres
drivers derive a different name from the raw context value, so each adapter
re-derives it here to keep routing pointed at the databases p0 actually creates.
"""

from __future__ import annotations

import re

from utility.middleware import plant_code_ctx

_PLANT_DB_BASE = "decisionops"
_DEFAULT_DB = "decisionops_cdm"


def plant_database_name(plant_code_id: str | None = None) -> str:
    """The database p0 routes to for ``plant_code_id`` (shared CDM db when unscoped)."""
    if plant_code_id is None:
        plant_code_id = plant_code_ctx.get()
    if not plant_code_id:
        return _DEFAULT_DB
    suffix = re.sub(r"[^a-z0-9]+", "_", str(plant_code_id).lower()).strip("_")
    return f"{_PLANT_DB_BASE}_{suffix}" if suffix else _DEFAULT_DB


def plant_scoped(upstream):
    """Subclass ``upstream`` so ``config['database']`` follows p0's per-plant naming."""

    class _PlantScopedDriver(upstream):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.config["database"] = plant_database_name()

        def _p0_preserve_database(self) -> None:
            """Run the upstream's lazy config load, then restore p0's database choice."""
            desired = self.config.get("database")
            lazy_load = getattr(self, "_load_config", None)
            if callable(lazy_load):
                lazy_load()
            if desired:
                self.config["database"] = desired

        def connect(self, *args, **kwargs):
            self._p0_preserve_database()
            return super().connect(*args, **kwargs)

        def get_engine(self, *args, **kwargs):
            self._p0_preserve_database()
            return super().get_engine(*args, **kwargs)

    _PlantScopedDriver.__name__ = upstream.__name__
    _PlantScopedDriver.__qualname__ = upstream.__qualname__
    return _PlantScopedDriver
