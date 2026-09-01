"""HTTP request validation for the cloud and OneDrive connector endpoints."""

from __future__ import annotations

from http import HTTPStatus

from fastapi import HTTPException

from .schemas.connectors import (
    CloudConnectorRequest,
    DocOneDriveRequest,
    OneDriveConnectorRequest,
)


def _validate_cloud_connector_request(req: CloudConnectorRequest) -> HTTPException | None:
    plant_code_id = (req.plant_code_id or "").strip()
    if not plant_code_id:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "plant_code_id is required",
                "data": None,
            },
        )
    cloud_type = (req.cloud_type or "").strip()
    if not cloud_type:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "cloud_type is required",
                "data": None,
            },
        )
    if cloud_type not in ("s3", "adls", "gcs"):
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": f"Invalid cloud_type '{cloud_type}'. Must be s3, adls, or gcs",
                "data": None,
            },
        )
    if cloud_type == "s3":
        if not (req.bucket or "").strip():
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "bucket is required for S3",
                    "data": None,
                },
            )
        if not (req.access_key or "").strip():
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "access_key is required for S3",
                    "data": None,
                },
            )
        if not (req.secret_key or "").strip():
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "secret_key is required for S3",
                    "data": None,
                },
            )
    elif cloud_type == "gcs":
        if not (req.bucket or "").strip():
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "bucket is required for GCS",
                    "data": None,
                },
            )
        if not (req.project_id or "").strip():
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "project_id is required for GCS",
                    "data": None,
                },
            )
        if not (req.service_account_json or "").strip():
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "service_account_json is required for GCS",
                    "data": None,
                },
            )
    elif cloud_type == "adls":
        if not (req.container or "").strip():
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "container is required for ADLS",
                    "data": None,
                },
            )
        if not (req.account_name or "").strip():
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "account_name is required for ADLS",
                    "data": None,
                },
            )
        has_account_key = bool((req.account_key or "").strip())
        has_sas_token = bool((req.sas_token or "").strip())
        has_service_principal = bool(
            (req.tenant_id or "").strip()
            and (req.client_id or "").strip()
            and (req.client_secret or "").strip()
        )
        if not (has_account_key or has_sas_token or has_service_principal):
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail={
                    "success": False,
                    "message": "ADLS requires account_key, sas_token, or service principal credentials (tenant_id, client_id, and client_secret)",
                    "data": None,
                },
            )
    return None


def _validate_onedrive_connector_request(req: OneDriveConnectorRequest) -> HTTPException | None:
    plant_code_id = (req.plant_code_id or "").strip()
    if not plant_code_id:
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "plant_code_id is required",
                "data": None,
            },
        )
    if not (req.tenant_id or "").strip():
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "tenant_id is required",
                "data": None,
            },
        )
    if not (req.client_id or "").strip():
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "client_id is required",
                "data": None,
            },
        )
    if not (req.client_secret or "").strip():
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "client_secret is required",
                "data": None,
            },
        )
    if not (req.source_path or "").strip():
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "source_path is required",
                "data": None,
            },
        )
    return None


def _validate_doc_onedrive_request(req: DocOneDriveRequest) -> HTTPException | None:
    err = _validate_onedrive_connector_request(req)
    if err:
        raise err
    if not (req.document_type or "").strip():
        raise HTTPException(
            status_code=HTTPStatus.BAD_REQUEST,
            detail={
                "success": False,
                "message": "document_type is required",
                "data": None,
            },
        )
    return None


def _build_cloud_override(req: CloudConnectorRequest, default_prefix: str) -> dict:
    if req.cloud_type == "s3":
        if not req.bucket:
            raise HTTPException(
                HTTPStatus.UNPROCESSABLE_ENTITY, "bucket is required for S3"
            )
        return {
            "bucket": req.bucket,
            "prefix": req.prefix or default_prefix,
            "connection": {
                "region": req.region or "us-east-1",
                "access_key": req.access_key or "",
                "secret_key": req.secret_key or "",
            },
        }
    elif req.cloud_type == "gcs":
        if not req.bucket:
            raise HTTPException(
                HTTPStatus.UNPROCESSABLE_ENTITY, "bucket is required for GCS"
            )
        return {
            "bucket": req.bucket,
            "prefix": req.prefix or default_prefix,
            "connection": {
                "project_id": req.project_id or "",
                "service_account_json": req.service_account_json or "",
            },
        }
    else:
        if not req.container or not req.account_name:
            raise HTTPException(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "container and account_name required for ADLS",
            )
        return {
            "container": req.container,
            "prefix": req.prefix or default_prefix,
            "connection": {
                "account_name": req.account_name,
                "account_key": req.account_key or "",
                "sas_token": req.sas_token or "",
                "tenant_id": req.tenant_id or "",
                "client_id": req.client_id or "",
                "client_secret": req.client_secret or "",
            },
        }
