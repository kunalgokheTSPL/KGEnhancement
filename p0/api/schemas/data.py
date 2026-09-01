"""Pydantic models for CDM data query endpoints."""

from __future__ import annotations

from pydantic import BaseModel, Field, ValidationInfo, field_validator


class EquipmentRecord(BaseModel):
    """Canonical equipment entity from CDM output."""

    equipment_uid: str | None = None
    plant_code_id: str | None = None
    normalized_asset: str | None = None
    equipment_id: str | None = None
    equipment_tag: str | None = None
    equipment_type: str | None = None
    functional_location: str | None = None
    area: str | None = None
    process_unit: str | None = None
    description: str | None = None
    manufacturer: str | None = None
    model: str | None = None
    status: str | None = None
    criticality: str | None = None
    source_system: str | None = None


class WorkOrderRecord(BaseModel):
    """Canonical work-order entity from CDM output."""

    work_order_uid: str | None = None
    plant_code_id: str | None = None
    wo_number: str | None = None
    description: str | None = None
    normalized_asset: str | None = None
    order_type: str | None = None
    priority: str | None = None
    status: str | None = None
    created_date: str | None = None
    source_system: str | None = None


class RelationshipRecord(BaseModel):
    """CDM relationship edge."""

    relationship_uid: str | None = None
    plant_code_id: str | None = None
    relationship_type: str | None = None
    parent_ref: str | None = None
    child_ref: str | None = None
    source_system: str | None = None
    confidence: float | None = None
    is_active: bool | None = None


class DataQueryParams(BaseModel):
    """Generic query parameters for CDM data."""

    plant_code_id: str | None = Field(default=None, description="Filter by plant code")
    limit: int = Field(default=100, ge=1, le=10000, description="Max rows to return")
    offset: int = Field(default=0, ge=0, description="Rows to skip (pagination)")
    search: str | None = Field(
        default=None,
        max_length=200,
        description="Search term (filters description/tag fields)",
    )

    @field_validator("plant_code_id", "search")
    @classmethod
    def validate_non_empty(cls, value, info: ValidationInfo):
        if value is not None:
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned:
                    raise ValueError(f"{info.field_name} cannot be empty")
                return cleaned
        return value


class OntologyExportRequest(BaseModel):
    """Parameters for KG ontology export."""

    use_case: str | None = Field(
        default=None, description="Use-case profile (e.g. maintenance, reliability)"
    )
    plant_code_id: str | None = Field(default=None, description="Filter by plant code")

    @field_validator("use_case", "plant_code_id")
    @classmethod
    def validate_non_empty(cls, value, info: ValidationInfo):
        if value is not None:
            if isinstance(value, str):
                cleaned = value.strip()
                if not cleaned:
                    raise ValueError(f"{info.field_name} cannot be empty")
                return cleaned
        return value
