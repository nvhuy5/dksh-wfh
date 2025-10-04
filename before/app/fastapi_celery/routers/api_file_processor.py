import traceback
import logging
from fastapi import APIRouter, HTTPException
from models.class_models import FilePathRequest, StopTaskRequest, ApiUrl, StatusEnum
from celery_worker import celery_task
from utils import log_helpers
from uuid import uuid4
from connections.redis_connection import get_step_ids, get_step_statuses, get_workflow_id
from celery_worker.celery_config import celery_app
from connections.be_connection import BEConnector

# ===
# Set up logging
logger_name = "File Processing Routers"
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)
# ===

router = APIRouter()


@router.post("/file/process", summary="Process file and log task result")
async def process_file(request: FilePathRequest):
    try:
        celery_id = str(uuid4())
        celery_task.task_execute.apply_async(
            kwargs={"file_path": request.file_path, "celery_id": celery_id},
            task_id=celery_id,
        )
        
        logger.info(f"Submitted Celery task: {celery_id}, file_path: {request.file_path}")
        return {
            "celery_id": celery_id,
            "file_path": request.file_path,
        }

    except Exception as e:
        traceback.print_exc()
        logger.info(f"Submitted Celery task failed, exception: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Submitted Celery task failed, exception: {str(e)}")


# stop a stask
@router.post("/tasks/stop", summary="Stop a running task by providing the task_id")
async def stop(request: StopTaskRequest):
    task_id = request.task_id
    reason = request.reason or "Stopped manually by user"
    
    workflow_id = get_workflow_id(task_id)
    step_ids = get_step_ids(task_id)
    step_statuses = get_step_statuses(task_id)
    
    if not workflow_id:
        return {
            "error": "Workflow ID not found for task",
            "task_id": task_id
        }
    try:
        for step_name, status in step_statuses.items():
            if status == "InProgress":
                step_id = step_ids.get(step_name)

                # Stop the Celery task
                celery_app.control.revoke(task_id, terminate=True, signal="SIGKILL")
                logger.info(f"Revoked Celery task {task_id} with reason: {reason}")
                await BEConnector(ApiUrl.workflow_step_finish, {
                    "workflowHistoryId": workflow_id,
                    "stepId": step_id,
                    "code": StatusEnum.SKIPPED,
                    "message": f"Step '{step_name}' was stopped manually.",
                    "dataInput": None,
                    "dataOutput": None
                }).post()

                # Update session status
                await BEConnector(ApiUrl.workflow_session_finish, {
                    "id": task_id,
                    "code": StatusEnum.SKIPPED,
                    "message": f"Session stopped: {reason}"
                }).post()

                return {
                    "status": "Task stopped successfully",
                    "task_id": task_id,
                    "message": reason
                }

    except Exception as e:
        logger.error(f"Failed to stop task {task_id}: {traceback.format_exc()}")
        return {
            "error": str(e),
            "traceback": traceback.format_exc()
        }
