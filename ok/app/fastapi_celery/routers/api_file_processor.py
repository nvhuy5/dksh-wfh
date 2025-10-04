import traceback
import logging
from fastapi import APIRouter, HTTPException, status, Request
from fastapi.responses import JSONResponse
from models.class_models import (
    FilePathRequest,
    StopTaskRequest,
    ApiUrl,
    StatusEnum,
)
from models.tracking_models import ServiceLog, LogType
from celery_worker import celery_task
from utils import log_helpers
from uuid import uuid4
from connections.redis_connection import RedisConnector
from celery_worker.celery_config import celery_app
from connections.be_connection import BEConnector
from typing import Dict, Any

# ===
# Set up logging
logger_name = "File Processing Router"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===

DISABLE_STOP_TASK_ENDPOINT = True  # Currently, disable stop_task endpoint
router = APIRouter()


@router.post("/file/process", summary="Process file and log task result")
async def process_file(data: FilePathRequest, http_request: Request) -> Dict[str, str]:
    try:
        # If run for the first time, it will create request_id (celery_id)
        # If run again, it will reuse request_id (celery_id)
        if not (data.celery_id and data.celery_id.strip()):
            data.celery_id = getattr(http_request.state, "request_id", str(uuid4()))

        celery_task.task_execute.apply_async(
            kwargs=data,
            task_id=data.celery_id,
        )

        logger.info(
            f"Submitted Celery task: {data.celery_id}",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ACCESS,
                "data": data,
            },
        )
        return {
            "celery_id": data.celery_id,
            "file_path": data.file_path,
        }

    except Exception as e:
        traceback.print_exc()
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.exception(
            f"Submitted Celery task failed, exception: \n{short_tb}",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ERROR,
                "data": data,
            },
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Submitted Celery task failed, exception: {str(e)}",
        )


# stop a stask
@router.post("/tasks/stop", summary="Stop a running task by providing the task_id")
async def stop(data: StopTaskRequest) -> Dict[str, Any]:
    """Stop a running task by revoking its Celery task and updating the workflow.

    Retrieves workflow and step details from Redis, revokes the Celery task if in progress,
    and notifies the backend API. Returns success status or error response.

    Args:
        request (StopTaskRequest): Pydantic model containing task_id and optional reason.

    Returns:
        Dict[str, Any]: Dictionary with 'status', 'task_id', and 'message' if successful.
        JSONResponse: Error response with status code 500 if the operation fails.

    Raises:
        HTTPException: If an unexpected error occurs during the process.
    """
    if DISABLE_STOP_TASK_ENDPOINT:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="This endpoint is temporarily disabled.",
        )
    redis_utils = RedisConnector()
    task_id = data.task_id
    reason = data.reason or "Stopped manually by user"

    redis_workflow = redis_utils.get_workflow_id(task_id)
    step_ids = redis_utils.get_step_ids(task_id)
    step_statuses = redis_utils.get_step_statuses(task_id)

    if not redis_workflow:
        logger.warning(
            f"Workflow not found for task_id: {task_id}",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ERROR,
                "traceability": task_id,
            },
        )
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "status_code": status.HTTP_404_NOT_FOUND,
                "error": "Workflow ID not found for task",
                "task_id": task_id,
            },
        )

    if redis_workflow["status"] != StatusEnum.PROCESSING:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "error": "Workflow has been done or stopped! Cannot stop the task",
                "task_id": task_id,
            },
        )

    try:
        for step_name, step_status in step_statuses.items():
            if step_status == "InProgress":
                step_id = step_ids.get(step_name)

                # Update step status to SKIPPED
                await BEConnector(
                    ApiUrl.WORKFLOW_STEP_FINISH.full_url(),
                    {
                        "workflowHistoryId": redis_workflow["workflow_id"],
                        "stepId": step_id,
                        "code": StatusEnum.SKIPPED,
                        "message": f"Step '{step_name}' was stopped manually.",
                        "dataInput": None,
                        "dataOutput": None,
                    },
                ).post()

        # Update session status to SKIPPED
        await BEConnector(
            ApiUrl.WORKFLOW_SESSION_FINISH.full_url(),
            {
                "id": task_id,
                "code": StatusEnum.SKIPPED,
                "message": f"Session stopped: {reason}",
            },
        ).post()

        # Kill the running process
        celery_app.control.revoke(task_id, terminate=True, signal="SIGKILL")
        logger.info(
            f"Revoked Celery task {task_id} with reason: {reason}",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.TASK,
                "traceability": task_id,
            },
        )

        return {
            "status": "Task stopped successfully",
            "task_id": task_id,
            "message": reason,
        }

    except Exception as e:
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"Failed to stop task {task_id}!\n{short_tb}",
            extra={
                "service": ServiceLog.API_GATEWAY,
                "log_type": LogType.ERROR,
                "traceability": task_id,
            },
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "status_code": status.HTTP_500_INTERNAL_SERVER_ERROR,
                "error": str(e),
                "traceback": traceback.format_exc(),
            },
        )
