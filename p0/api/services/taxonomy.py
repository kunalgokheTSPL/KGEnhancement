"""One canonical grouping vocabulary per connector, used in both directions."""

from __future__ import annotations

import logging

_log = logging.getLogger("p0.taxonomy")

CATEGORY = "category"
FILE_ROLE = "file_role"
TYPE = "type"
SUBTYPE = "subtype"
PIPELINE_GROUP = "pipeline_group"
TABLE = "table"


PNID_CATEGORIES = [
    {"key": "pnid", "label": "P&ID Diagrams", "sort_order": 1},
    {"key": "pfd", "label": "PFD Diagrams", "sort_order": 2},
    {"key": "line_list", "label": "Line Lists", "sort_order": 3},
    {"key": "others", "label": "Other Files", "sort_order": 4},
]

TIMESERIES_ROLES = [
    {"key": "metadata", "label": "Metadata Upload", "sort_order": 1},
    {"key": "values", "label": "Values Upload", "sort_order": 2},
]

SAP_PIPELINE_GROUPS = [
    {"key": "workorder", "label": "Work Order", "sort_order": 1},
    {"key": "floc", "label": "Functional Location", "sort_order": 2},
    {"key": "tasklist", "label": "Task List", "sort_order": 3},
    {"key": "material", "label": "Material", "sort_order": 4},
]

FINDINGS_CATEGORIES = {
    "aif": [
        {
            "key": "aif",
            "label": "Asset Integrity Findings",
            "sort_order": 1,
        }
    ],
    "gloc": [
        {
            "key": "gloc",
            "label": "Gas Loss of Containment",
            "sort_order": 1,
        }
    ],
    "lopc": [
        {
            "key": "lopc",
            "label": "Loss of Primary Containment",
            "sort_order": 1,
        }
    ],
    "alerts": [
        {
            "key": "alerts",
            "label": "Alerts",
            "sort_order": 1,
        }
    ],
}

SAP_TABLE_TO_GROUP = {
    "AUFK": "workorder",
    "AFKO": "workorder",
    "AFVC": "workorder",
    "AFVV": "workorder",
    "QMEL": "workorder",
    "QMSM": "workorder",
    "JEST": "workorder",
    "JSTO": "workorder",
    "RESB": "workorder",
    "COEP": "workorder",
    "EQUI": "workorder",
    "STXH": "workorder",
    "STXL": "workorder",
    "IFLOT": "floc",
    "ILOA": "floc",
    "PLKO": "tasklist",
    "PLPO": "tasklist",
    "MAPL": "tasklist",
    "MARA": "material",
    "MARC": "material",
    "MARD": "material",
}


_UPLOAD_ALIASES = {
    "pnid_files": "pnid",
    "pfd_files": "pfd",
    "linelist_files": "line_list",
    "linelist": "line_list",
    "other_files": "others",
    "other": "others",
    "historian": "historian",
    "source": "historian",
}


def canonical_category(
    raw: str | None,
) -> str | None:
    """Resolve any inbound spelling to the one canonical key used on reads."""
    if raw is None:
        return None

    trimmed = str(raw).strip()

    if not trimmed:
        return None

    if trimmed.upper() in SAP_TABLE_TO_GROUP:
        return trimmed.upper()

    value = (
        trimmed.lower()
        .replace("-", "_")
        .replace(" ", "_")
    )

    return _UPLOAD_ALIASES.get(
        value,
        value,
    )


def levels(
    connector: str,
) -> int:
    """P&ID and timeseries are one level; documents and SAP are two."""
    return (
        2
        if connector in ("documents", "sap")
        else 1
    )


def group_kinds(
    connector: str,
) -> tuple[str, str | None]:
    """(leaf_kind, parent_kind) for one connector."""

    if connector == "pnid":
        return CATEGORY, None

    if connector == "timeseries":
        return FILE_ROLE, None

    if connector == "documents":
        return SUBTYPE, TYPE

    if connector == "sap":
        return TABLE, PIPELINE_GROUP

    return CATEGORY, None


def _labelise(
    key: str,
) -> str:
    """Readable label for a key we have no catalog entry for."""

    return (
        str(key or "")
        .replace("_", " ")
        .replace("-", " ")
        .strip()
        .title()
        or "Uncategorised"
    )


def parent_of(
    connector: str,
    key: str,
    doc_parents: dict[str, str] | None = None,
) -> str | None:
    """Which level-1 node a leaf belongs to, or None for a one-level connector."""

    if connector == "sap":
        return SAP_TABLE_TO_GROUP.get(
            str(key or "").upper()
        )

    if connector == "documents":
        return (
            doc_parents or {}
        ).get(key)

    return None


def catalog(
    connector: str,
) -> list[dict]:
    """The declared vocabulary for a connector, so the UI renders empty groups too."""

    if connector == "pnid":
        return [
            dict(c)
            for c in PNID_CATEGORIES
        ]

    if connector == "timeseries":
        return [
            dict(c)
            for c in TIMESERIES_ROLES
        ]

    if connector == "sap":
        return [
            dict(c)
            for c in SAP_PIPELINE_GROUPS
        ]

    if connector in FINDINGS_CATEGORIES:
        return [
            dict(c)
            for c in FINDINGS_CATEGORIES[
                connector
            ]
        ]

    return []


def empty_counts() -> dict[str, int]:
    """The same seven counts on every group, on every connector."""

    return {
        "files_total": 0,
        "pending": 0,
        "processing": 0,
        "processed": 0,
        "failed": 0,
        "skipped": 0,
        "superseded": 0,
    }


def taxonomy_for_file(
    connector: str,
    group_key: str | None,
    *,
    labels: dict[str, str] | None = None,
    doc_parents: dict[str, str] | None = None,
) -> dict:
    """The taxonomy block carried on every file row."""

    leaf_kind, parent_kind = group_kinds(
        connector
    )

    key = canonical_category(
        group_key
    )

    label_map = labels or {}

    parent_key = (
        parent_of(
            connector,
            key,
            doc_parents,
        )
        if key
        else None
    )

    return {
        "group_kind": leaf_kind,
        "group_key": key,
        "group_label": (
            label_map.get(key)
            or (
                _labelise(key)
                if key
                else None
            )
        ),
        "parent_key": parent_key,
        "parent_label": (
            label_map.get(parent_key)
            or (
                _labelise(parent_key)
                if parent_key
                else None
            )
        ),
    }


def build_groups(
    connector: str,
    rows: list[dict],
    *,
    classify,
    key_of,
    labels: dict[str, str] | None = None,
    doc_parents: dict[str, str] | None = None,
) -> list[dict]:
    """Roll files up into the generic 1-or-2 level groups[] the UI renders."""

    leaf_kind, parent_kind = group_kinds(
        connector
    )

    label_map = dict(
        labels or {}
    )

    for entry in catalog(
        connector
    ):
        label_map.setdefault(
            entry["key"],
            entry["label"],
        )

    leaves: dict[str, dict] = {}

    order = {
        entry["key"]: entry["sort_order"]
        for entry in catalog(connector)
    }

    for entry in catalog(
        connector
    ):
        if (
            parent_kind
            and connector == "sap"
        ):
            continue

        leaves[
            entry["key"]
        ] = {
            "group_kind": leaf_kind,
            "group_key": entry["key"],
            "parent_key": None,
            "label": entry["label"],
            "is_custom": False,
            "sort_order": entry["sort_order"],
            "counts": empty_counts(),
            "file_ids": [],
        }

    for row in rows:
        key = canonical_category(
            key_of(row)
        )

        if not key:
            key = "uncategorised"

        node = leaves.get(
            key
        )

        if node is None:
            node = {
                "group_kind": leaf_kind,
                "group_key": key,
                "parent_key": parent_of(
                    connector,
                    key,
                    doc_parents,
                ),
                "label": (
                    label_map.get(key)
                    or _labelise(key)
                ),
                "is_custom": key not in order,
                "sort_order": order.get(
                    key,
                    100 + len(leaves),
                ),
                "counts": empty_counts(),
                "file_ids": [],
            }

            leaves[key] = node

        node["counts"][
            "files_total"
        ] += 1

        node["counts"][
            classify(row)
        ] += 1

        file_id = row.get(
            "flow_uid"
        )

        if file_id is not None:
            node["file_ids"].append(
                file_id
            )

    groups = list(
        leaves.values()
    )

    if parent_kind:
        parents: dict[str, dict] = {}

        for node in groups:
            parent_key = node.get(
                "parent_key"
            )

            if not parent_key:
                continue

            parent = parents.get(
                parent_key
            )

            if parent is None:
                parent = {
                    "group_kind": parent_kind,
                    "group_key": parent_key,
                    "parent_key": None,
                    "label": (
                        label_map.get(parent_key)
                        or _labelise(parent_key)
                    ),
                    "is_custom": (
                        parent_key not in order
                    ),
                    "sort_order": order.get(
                        parent_key,
                        100 + len(parents),
                    ),
                    "counts": empty_counts(),
                    "file_ids": [],
                }

                parents[
                    parent_key
                ] = parent

            for (
                bucket,
                value,
            ) in node[
                "counts"
            ].items():
                parent["counts"][
                    bucket
                ] += value

        groups = (
            list(
                parents.values()
            )
            + groups
        )

    groups.sort(
        key=lambda g: (
            g["parent_key"] or "",
            g["sort_order"],
            g["group_key"],
        )
    )

    return groups


CONNECTOR_TO_SOURCE = {
    "pnid": "pnid",
    "p&id": "pnid",
    "pid": "pnid",

    "documents": "docs",
    "docs": "docs",
    "document": "docs",

    "timeseries": "ts",
    "ts": "ts",
    "time_series": "ts",

    "sap": "sap",
    "sap_mapping": "sap",

    "aif": "aif",
    "gloc": "gloc",
    "lopc": "lopc",
    
    

    "upd_event": "upd_event",
    "trip_event": "trip_event",

    "other_files": "other_files",
    "other-files": "other_files",
    "others": "other_files",
    "other": "other_files",
    "alerts": "alerts",
    "alert": "alerts",
}


def resolve_source(
    raw: str | None,
) -> str | None:
    """Accept either the public connector name or the internal source name."""

    return CONNECTOR_TO_SOURCE.get(
        (raw or "")
        .strip()
        .lower()
    )