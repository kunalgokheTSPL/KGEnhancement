"""Pydantic models for configuration endpoints."""

from __future__ import annotations

import re

from pydantic import BaseModel, Field, field_validator

_SAFE_ID = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def validate_identifier(value: str, label: str = "identifier") -> str:
    if not _SAFE_ID.match(value):
        raise ValueError(
            f"Invalid {label}: must be alphanumeric/underscore, max 64 chars"
        )
    return value


class ColumnDef(BaseModel):
    """One column definition: SAP field name → canonical name."""

    sap_field: str = Field(
        ..., min_length=1, max_length=64, description="SAP field name (e.g. AUFNR)"
    )
    canonical_name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Human-friendly canonical name (e.g. work_order_number)",
    )

    @field_validator("sap_field")
    @classmethod
    def sap_field_safe(cls, v: str) -> str:
        validate_identifier(v, "sap_field")
        return v

    @field_validator("canonical_name")
    @classmethod
    def canonical_name_safe(cls, v: str) -> str:
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_ ]{0,127}$", v):
            raise ValueError("canonical_name contains invalid characters")
        return v.strip()


class AddSapTableRequest(BaseModel):
    """Register a new SAP table in both extraction and rename configs."""

    table_name: str = Field(..., description="SAP table name, e.g. EQUI, MARA")
    group: str = Field(
        default="custom",
        description="Logical group (technical_objects, notifications …)",
    )
    label: str = Field(..., description="Human-friendly label for this table")
    key_fields: list[str] = Field(
        default_factory=lambda: ["MANDT"],
        description="Key / join fields for this table",
    )
    columns: list[ColumnDef] = Field(
        ..., min_length=1, description="Column definitions (SAP → canonical)"
    )
    source_key: str = Field(
        ..., description="CDM source key for rename map, e.g. sap_custom_xyz"
    )

    @field_validator("table_name")
    @classmethod
    def table_name_safe(cls, v: str) -> str:
        validate_identifier(v, "table_name")
        return v.upper()

    @field_validator("source_key")
    @classmethod
    def source_key_safe(cls, v: str) -> str:
        validate_identifier(v, "source_key")
        return v.lower()

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "table_name": "EQUI",
                    "group": "technical_objects",
                    "label": "Equipment Master",
                    "key_fields": ["EQUNR"],
                    "columns": [
                        {"sap_field": "EQUNR", "canonical_name": "equipment_id"},
                        {"sap_field": "EQKTX", "canonical_name": "description"},
                    ],
                    "source_key": "sap_equipment",
                }
            ]
        }
    }


class UpdateColumnRenamesRequest(BaseModel):
    """Replace column rename mappings for a source key."""

    mappings: dict[str, str] = Field(
        ..., description="SAP field → canonical name mapping"
    )


class UserConfigUpdate(BaseModel):
    """Partial user config update (deep-merged with existing config)."""

    document_processing: dict | None = Field(
        default=None, description="Document processing overrides"
    )
    sap_processing: dict | None = Field(
        default=None, description="SAP processing overrides"
    )
    extra: dict | None = Field(
        default=None, description="Any additional user overrides"
    )
