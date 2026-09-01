from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from utility.logging.error_utils import attach_error_location


def register_observability(sub_app: FastAPI) -> None:
    @sub_app.exception_handler(Exception)
    async def _observability_exception_handler(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        attach_error_location(request, exc)
        return JSONResponse(
            status_code=500,
            content={
                "statusCode": 500,
                "message": "Internal Server Error – Something went wrong.",
                "data": None,
            },
        )