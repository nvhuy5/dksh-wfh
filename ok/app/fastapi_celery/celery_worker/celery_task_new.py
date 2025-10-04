import sys
import asyncio
import logging
import json
import traceback
from pathlib import Path
from typing import Dict, Any, Optional, List
import contextvars
from celery import shared_task
from celery.exceptions import Retry, MaxRetriesExceededError
from pydantic import BaseModel

from .step_handler import execute_step, raise_if_failed
from connections.be_connection import BEConnector
from connections.redis_connection import RedisConnector
from processors.processor_base import ProcessorBase
from processors.processor_nodes import STEP_DEFINITIONS
from processors.common import template_helper
from models.class_models import (
    ContextData,
    FilePathRequest,
    WorkflowModel,
    ApiUrl,
    StatusEnum,
    WorkflowSession,
    StartStep,
    DocumentType,
    StepOutput,
    StepDefinition,
    WorkflowStep,
)
from models.tracking_models import ServiceLog, LogType, TrackingModel
from utils import log_helpers, read_n_write_s3
import config_loader
from utils.middlewares.request_context import get_context_value, set_context_values
from utils.ext_extraction import FileExtensionProcessor

# === Setup logging ===
logger_name = "Celery Task Execution"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})

# === Load config ===
sys.path.append(str(Path(__file__).resolve().parent.parent))
types_list = json.loads(config_loader.get_config_value("support_types", "types"))


@shared_task(bind=True, retry_kwargs={"max_retries": 3})
def task_execute(self, data: FilePathRequest) -> str:
    try:
        tracking_model = TrackingModel.from_file_request(data)
        ctx = contextvars.copy_context()
        ctx.run(lambda: asyncio.run(handle_task(tracking_model)))

        logger.info(
            f"[{tracking_model.request_id}] Starting task execution",
            extra={
                "service": ServiceLog.TASK_EXECUTION,
                "log_type": LogType.TASK,
                "data": tracking_model,
            },
        )
        return "Task completed"
    except Retry:
        logger.warning(f"[{tracking_model.request_id}] Task is retrying...")
        raise
    except Exception as e:
        retry_count = self.request.retries
        max_retries = self.max_retries or 3
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"[{tracking_model.request_id}] Task execution failed (attempt {retry_count}/{max_retries}): {e}\n{short_tb}",
            extra={
                "service": ServiceLog.TASK_EXECUTION,
                "log_type": LogType.ERROR,
                "data": tracking_model,
            },
        )
        try:
            raise self.retry(exc=e, countdown=5)
        except MaxRetriesExceededError:
            logger.critical(
                f"[{tracking_model.request_id}] Maximum retries exceeded for task.",
                extra={
                    "service": ServiceLog.TASK_EXECUTION,
                    "log_type": LogType.ERROR,
                    "data": tracking_model,
                },
            )
            raise


async def handle_task(tracking_model: TrackingModel) -> Dict[str, Any]:
    # === Pre-Processing ===
    logger.info(f"[{tracking_model.request_id}] Start processing file")

    redis_connector = RedisConnector()
    file_processor = ProcessorBase(tracking_model)
    context_data = ContextData(tracking_model.request_id)

    # === Fetch workflow ===
    workflow_model = await get_workflow_filter(context_data=context_data, file_processor=file_processor, tracking_model=tracking_model)
    _ = await call_workflow_session_start(context_data=context_data, file_processor=file_processor, tracking_model=tracking_model)

    # === Process steps ===
    # Sort steps in ascending order by stepOrder
    data_input = None
    sorted_steps = sorted(workflow_model.workflowSteps, key=lambda step: step.stepOrder)
    for step in sorted_steps:
        logger.info(f"[{tracking_model.request_id}] Starting step: {step.stepName}")

        # Start step
        start_step_response = BEConnector(
            ApiUrl.WORKFLOW_STEP_START.full_url(),
            {"sessionId": workflow_session.id, "stepId": step.workflowStepId},
        ).post()

        # Execute step
        data_output = await execute_step(step, file_processor, context, data_input)

        # Finish step
        finish_step_response = await BEConnector(
            ApiUrl.WORKFLOW_STEP_FINISH.full_url(),
            {
                "workflowHistoryId": start_step_response.workflowHistoryId,
                "code": StatusEnum.SUCCESS,
                "message": "",
                "dataInput": "input_data",
                "dataOutput": step_output,
            },
        ).post()

        data_input = data_output


async def get_workflow_filter(context_data: ContextData, file_processor: ProcessorBase, tracking_model: TrackingModel):
    logger.info(f"[{tracking_model.request_id}] Start workflow filter")
    body_data = {
        "filePath": file_processor.file_record["file_path_parent"],
        "fileName": file_processor.file_record["file_name"],
        "fileExtension": file_processor.file_record["file_extension"],
        "project": tracking_model.project_name,
        "source": tracking_model.source_name,
    }
    workflow_connector = BEConnector(ApiUrl.WORKFLOW_FILTER.full_url(), body_data=body_data)
    workflow_response = await workflow_connector.post()
    if not workflow_response:
        raise RuntimeError(f"[{tracking_model.request_id}] Failed to fetch workflow")
    
    workflow_model = WorkflowModel(**workflow_response)
    context_data.workflow.filter_api.url = ApiUrl.WORKFLOW_FILTER.full_url()
    context_data.workflow.filter_api.request = body_data
    context_data.workflow.filter_api.response = workflow_model

    tracking_model.workflow_name = workflow_model.name,
    tracking_model.workflow_id = workflow_model.id,
    tracking_model.sap_masterdata = bool(workflow_model.sapMasterData)

    return workflow_model


async def call_workflow_session_start(context_data: ContextData, file_processor: ProcessorBase, tracking_model: TrackingModel):
    logger.info(f"[{tracking_model.request_id}] Start session")
    body_data = {
        "workflowId": context_data.workflow.filter_api.response.id,
        "celeryId": tracking_model.request_id,
        "filePath": tracking_model.file_path,
    }
    session_connector = BEConnector(ApiUrl.WORKFLOW_SESSION_START.full_url(), body_data=body_data)
    session_response = await session_connector.post()
    context_data.workflow.metadata_api.session_start_api.response = session_response


def call_workflow_session_finish(context_data: ContextData):
    pass


def call_workflow_step_start(context_data: ContextData):
    pass


def call_workflow_step_finish(context_data: ContextData):
    pass
