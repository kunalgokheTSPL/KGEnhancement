"""Pydantic models for pipeline endpoints."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RunPipelineRequest(BaseModel):
    """Trigger a CDM pipeline stage."""

    stage: str = Field(
        ...,
        pattern=r"^(sap_mapping|sap|docs|ts|pnid|aif|gloc|lopc|upd_event|trip_event|alerts|other_files|full)$",
        description=(
            "Pipeline stage to run: sap_mapping, sap, docs, ts, pnid, "
            "aif, gloc, lopc, upd_event, trip_event, other_files, alerts, or full"
        ),
    )

    plant_code_id: str = Field(
        default="ARAMCO",
        description="Plant identifier (e.g. ARAMCO_MAT)",
    )

    industry: str | None = Field(
        default=None,
        description=(
            "Industry ID (cement, oil_gas). "
            "Falls back to user_config.yaml if omitted."
        ),
    )

    incremental: bool = Field(
        default=False,
        description="If true, merge with existing output instead of overwrite",
    )

    file_names: list[str] | None = Field(
        default=None,
        description=(
            "For pnid stage: list of PDF filenames uploaded in this session. "
            "Pipeline will only process data for these files. "
            "For docs stage (legacy): list of doc_type keys to process "
            "(prefer the explicit `doc_types` field for new clients). "
            "For other_files stage: list of Other Files filenames to process."
            "For alerts: uploaded ALERTS workbook filename. "
        ),
    )

    other_type: str | None = Field(
        default=None,
        description="Other Files only: user-defined processing type.",
    )

    equipment_column: str | None = Field(
        default=None,
        description="Other Files only: optional equipment identifier column.",
    )

    extraction_fields: list[str] | None = Field(
        default=None,
        description=(
            "Other Files only: fields to extract from unstructured content."
        ),
    )

    doc_types: list[str] | None = Field(
        default=None,
        description=(
            "Docs stage: explicit list of document_type keys "
            "(e.g. sop, rca_reports) to process. "
            "If omitted, all types from user_config.yaml are processed."
        ),
    )

    doc_files: list[str] | None = Field(
        default=None,
        description=(
            "Docs stage: list of filenames uploaded in this session. "
            "Pipeline only processes these (already-processed files are "
            "tracked in {work_dir}/docs_done.json and skipped). "
            "Omit to process every file under the configured doc types."
        ),
    )

    upload_batch_id: str | None = Field(
        default=None,
        description=(
            "Optional opaque batch id stamped onto every processed row so the "
            "review API can scope the preview to exactly this run. "
            "If omitted, the pipeline pipeline_job_id is used as the "
            "upload_batch_id (recommended — one run == one reviewable batch). "
            "Durable across restarts and robust to re-uploading the same filename."
        ),
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "stage": "sap",
                    "plant_code_id": "ARAMCO_MAT",
                    "industry": "cement",
                    "incremental": False,
                },
                {
                    "stage": "other_files",
                    "plant_code_id": "ARAMCO_MAT",
                    "industry": "cement",
                    "file_names": ["sample.xlsx"],
                    "other_type": "inspection_data",
                    "equipment_column": "equipment_id",
                    "extraction_fields": [],
                },
            ]
        }
    }


class PipelineJobStatus(BaseModel):
    """Status of a running or completed pipeline job."""

    pipeline_job_id: str
    stage: str
    plant_code_id: str
    status: str = Field(
        description="queued | running | completed | failed"
    )
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    output_tables: list[str] | None = None


class ValidationSummary(BaseModel):
    """Validation summary for a pipeline run."""

    plant_code_id: str
    stage: str
    total_checks: int
    passed: int
    failed: int
    warnings: int
    details: list[dict]