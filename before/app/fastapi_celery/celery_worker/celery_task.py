import sys
import traceback
import asyncio
import logging
import json
from pathlib import Path

from celery import shared_task

from .step_handler import execute_step
from connections.be_connection import BEConnector
from template_processors.file_processor import FileProcessor
from models.class_models import WorkflowModel, ApiUrl, StatusEnum, WorkflowSession
from utils import log_helpers
import config_loader

# === Setup logging ===
logger_name = "Celery Task Execution"
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)

# === Load config ===
sys.path.append(str(Path(__file__).resolve().parent.parent))

types_list = json.loads(config_loader.get_config_value("support_types", "types"))


@shared_task(bind=True)
def task_execute(self, file_path: str, celery_id: str) -> str:
    """
    Entry point Celery task (sync). Internally executes async logic using asyncio.run.
    """
    try:
        logger.info(f"[{celery_id}] Starting task execution (sync wrapper)")
        asyncio.run(handle_task(file_path, celery_id))
        return "Task completed successfully"
    except Exception as e:
        logger.exception(f"[{celery_id}] Task execution failed: {str(e)}")
        return f"Task failed: {str(e)}"


async def handle_task(file_path: str, celery_id: str):
    """
    Actual async task logic
    """
    logger.info(f"[{celery_id}] Start processing file: {file_path}")

    file_processor = FileProcessor(bucket_name=config_loader.get_config_value("s3_buckets", "converted_files"), file_path=file_path)
    file_processor.extract_metadata()

    body_data = {
        "filePath": file_processor.file_record["file_path_parent"],
        "fileName": file_processor.file_record["file_name"],
        "fileExtension": file_processor.file_record["file_extension"],
    }

    # === Fetch workflow ===
    workflow = BEConnector(ApiUrl.workflow_filter, body_data=body_data)
    workflow_response = await workflow.post()

    if not workflow_response:
        logger.error(f"[{celery_id}] Failed to fetch workflow for file: {file_path}")
        return

    workflow_model = WorkflowModel(**workflow_response)

    # === Start session ===
    session_connector = BEConnector(ApiUrl.workflow_session_start, {
        "workflowId": workflow_model.id,
        "celeryId": celery_id,
        "filePath": file_path,
    })
    session_response = await session_connector.post()
    if not session_response:
        logger.error(f"[{celery_id}] Failed to create workflow session.")
        return

    workflow_session = WorkflowSession(**session_response)
    data_input = None

    for step in workflow_model.workflowSteps:
        logger.info(f"[{celery_id}] Starting step: {step.stepName}")
        
        # Start step
        await BEConnector(ApiUrl.workflow_step_start, {
            "sessionId": workflow_session.id,
            "stepId": step.workflowStepId
        }).post()

        # Execute step
        data_output = await execute_step(file_processor, step, data_input)

        # Finish step
        await BEConnector(ApiUrl.workflow_step_finish, {
            "workflowHistoryId": workflow_model.id,
            "code": StatusEnum.SUCCESS,
            "message": "",
            "dataInput": data_input,
            "dataOutput": data_output
        }).post()

        data_input = data_output

    # === Finish session ===
    await BEConnector(ApiUrl.workflow_session_finish, {
        "id": workflow_session.id,
        "code": StatusEnum.SUCCESS,
        "message": ""
    }).post()

    logger.info(f"[{celery_id}] Finished processing file: {file_path}")
