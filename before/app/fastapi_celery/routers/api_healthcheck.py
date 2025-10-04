# Standard Library Imports
import logging
import traceback

# Third-Party Imports
from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

# Local Application Imports
from utils import log_helpers

# ===
# Set up logging
logger_name = 'Health-check Routers'
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)
# ===

router = APIRouter()

def _internal_health_check():
    """
    Update the internal healthcheck aiming to replace by error simulation when running test case
    """
    logger.info("Health check passed successfully.")
    return {"status": "ok"}

@router.get("/healthz")
async def healthz():
    try:
        return _internal_health_check()
    except Exception as e:
        # Log the error details
        logger.error(f"Health check failed: {e} - {traceback.format_exc()}")
        return JSONResponse(
            status_code = status.HTTP_503_SERVICE_UNAVAILABLE,
            content     = {
                "status": "error",
                "details": f"{e} - {traceback.format_exc()}"
            },
        )
