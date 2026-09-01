"""Pydantic models for connector endpoints (P&ID, Documents)."""

from __future__ import annotations

from pydantic import BaseModel, Field, ValidationInfo, field_validator


class CloudConnectorRequest(BaseModel):
    """S3, ADLS, or GCS connection parameters."""

    plant_code_id: str = Field(..., description="Plant code ID")
    cloud_type: str = Field(
        ..., pattern="^(s3|adls|gcs)$", description="Cloud provider type"
    )
    bucket: str | None = Field(default=None, description="S3/GCS bucket name")
    prefix: str | None = Field(default=None, description="Object key prefix filter")
    region: str | None = Field(default="us-east-1", description="AWS region")
    access_key: str | None = Field(default=None, description="AWS access key ID")
    secret_key: str | None = Field(default=None, description="AWS secret access key")
    container: str | None = Field(default=None, description="ADLS container name")
    account_name: str | None = Field(
        default=None, description="Azure storage account name"
    )
    account_key: str | None = Field(
        default=None, description="Azure storage account key"
    )
    sas_token: str | None = Field(default=None, description="Azure SAS token")
    tenant_id: str | None = Field(default=None, description="Azure AD tenant ID")
    client_id: str | None = Field(default=None, description="Azure AD app client ID")
    client_secret: str | None = Field(
        default=None, description="Azure AD app client secret"
    )
    project_id: str | None = Field(default=None, description="GCP project ID")
    service_account_json: str | None = Field(
        default=None, description="GCP service account JSON key (as string)"
    )

    @field_validator("cloud_type", "plant_code_id")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "cloud_type": "s3",
                    "bucket": "my-pnid-bucket",
                    "prefix": "raw/pnid",
                    "region": "us-east-1",
                    "access_key": "AKIAEXAMPLE",
                    "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                }
            ]
        }
    }


class OneDriveConnectorRequest(BaseModel):
    """OneDrive / SharePoint connection parameters."""

    plant_code_id: str = Field(..., description="Plant code ID")
    tenant_id: str = Field(..., description="Azure AD tenant ID")
    client_id: str = Field(..., description="Azure AD application client ID")
    client_secret: str = Field(..., description="Azure AD application client secret")
    source_path: str = Field(
        default="/Documents/CDM/pnid", description="OneDrive folder path to sync"
    )
    user_principal_name: str | None = Field(
        default=None, description="UPN for delegated access"
    )
    drive_id: str | None = Field(default=None, description="Specific OneDrive drive ID")
    site_id: str | None = Field(default=None, description="SharePoint site ID")

    @field_validator("plant_code_id", "tenant_id", "client_id", "client_secret")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value


class DocOneDriveRequest(OneDriveConnectorRequest):
    """OneDrive connection for document files (extends base with document_type)."""

    source_path: str = "/Documents/CDM/documents"
    document_type: str = Field(
        default="general", description="Document category (e.g. sop, rca, o_and_m)"
    )

    @field_validator("document_type")
    @classmethod
    def validate_required(cls, value, info: ValidationInfo):
        if not value:
            raise ValueError(f"{info.field_name} cannot be empty")
        return value


class ConnectorJobStatus(BaseModel):
    """Status of a connector job (upload or cloud sync)."""

    job_id: str
    status: str = Field(description="queued | uploading | running | completed | failed")
    source: str = Field(description="device | s3 | adls | gcs | onedrive")
    files: list[dict] = Field(default_factory=list)
    error: str | None = None
    log: str | None = None
    created: str | None = None
    data_type: str | None = None
    document_type: str | None = None
