"""
Data Quality routes for analyzing IoTDB data quality.
"""

from __future__ import annotations

import dataclasses
import sys
import traceback
from http import HTTPStatus
from pathlib import Path

from fastapi import APIRouter, Cookie, Query
from fastapi.responses import JSONResponse

from p0.driver import get_iotdb_connection
from p0.dataquality.utils import fetch_all_data
from p0.dataquality.ts_quality_analyzer import (
    PipelineConfig,
    TimeSeriesQualityAnalyzer,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


router = APIRouter(prefix="/dataquality", tags=["Data Quality"])


@router.get(
    "/health",
    summary="Health check endpoint",
    description="Returns the health status of the data quality service.",
)
def healthCheck():
    """Health check endpoint."""
    return {
        "success": True,
        "message": "Data quality service is healthy",
        "data": {"status": "ok"},
    }


@router.get(
    "/analyze",
    summary="Analyze IoTDB data quality",
    description=(
        "Fetch ALL numeric data from IoTDB and run quality analysis. "
        "Requires authentication via access_token cookie."
    ),
)
def analyzeDataQuality(
    plant_code_id: str = Query(
        ..., description="Plant code ID for the request"
    ),
    access_token: str = Cookie(None),
):
    """
    Fetch ALL numeric data from IoTDB and run quality analysis.

    GET /dataquality/analyze
    """

    # -------------------------------
    # Authentication
    # -------------------------------
    if not access_token:
        return JSONResponse(
            status_code=HTTPStatus.UNAUTHORIZED,
            content={
                "success": False,
                "message": "Authentication required",
                "errors": [
                    {"field": "access_token", "message": "Authentication token is missing."}
                ],
            },
        )

    # -------------------------------
    # Input Validation
    # -------------------------------
    errors = []
    if not plant_code_id or not plant_code_id.strip():
        errors.append({"field": "plant_code_id", "message": "plant_code_id is required"})
    if errors:
        return JSONResponse(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            content={
                "success": False,
                "message": "Validation failed",
                "errors": errors,
            },
        )

    # -------------------------------
    # Analyze Data Quality
    # -------------------------------
    try:
        # Connect to IoTDB
        conn = get_iotdb_connection()

        if conn is None:
            return JSONResponse(
                status_code=HTTPStatus.SERVICE_UNAVAILABLE,
                content={
                    "success": False,
                    "message": "Data quality analysis failed",
                    "errors": [{"field": "server", "message": "IoTDB connection failed"}],
                },
            )

        # Fetch all data
        df = fetch_all_data(
            conn,
            pattern="root.plant.**",
        )

        # Check if data exists
        if df is None or df.empty:
            return JSONResponse(
                status_code=HTTPStatus.NOT_FOUND,
                content={
                    "success": False,
                    "message": "Data quality analysis failed",
                    "errors": [{"field": "plant_code_id", "message": "No data found in IoTDB for the available tags. Check IoTDB connection and verify tags have data."}],
                },
            )

        # Run quality analysis
        result = TimeSeriesQualityAnalyzer(
            df,
            PipelineConfig(),
        ).analyze()

        return {
            "success": True,
            "message": "Analysis completed successfully",
            "data": {
                "meta": {
                    "rowsFetched": len(df),
                    "sensorsFound": (
                        len(df.columns) - 1
                        if len(df.columns) > 0
                        else 0
                    ),
                },
                "analysis": dataclasses.asdict(result),
            },
        }

    except Exception as exc:
        return JSONResponse(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "message": "Internal server error",
                "errors": [{"field": "server", "message": f"Analysis failed: {str(exc)}"}],
            },
        )