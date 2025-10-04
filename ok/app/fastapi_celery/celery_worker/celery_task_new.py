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
    Step,
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
    """
    Entry point Celery task (sync). Internally executes async logic using asyncio.run.
    Celery task entry point (synchronous wrapper).

    Args:
        self: The Celery task instance.
        data: FilePathRequest

    Returns:
        str: Status message indicating success or failure of the task.
    """
    try:
        tracking_model = TrackingModel.from_data_request(data)
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
    """
    Asynchronously handles the file processing workflow.

    Args:
        tracking_model (TrackingModel): The Celery task ID for logging and tracking.

    Returns:
        Dict[str, Any]: This function performs processing and return a dict of value extracted.
    """
    # === Pre-Processing ===
    logger.info(
        f"[{tracking_model.request_id}] Start processing file"
        + (f" | Rerun attempt: {tracking_model.rerun_attempt}" if tracking_model.rerun_attempt is not None else ""),
        extra={
            "service": ServiceLog.TASK_EXECUTION,
            "log_type": LogType.TASK,
            "data": tracking_model,
        },
    )

    redis_connector = RedisConnector()
    file_processor = ProcessorBase(tracking_model)
    context_data = ContextData(tracking_model.request_id)

    # === Fetch workflow ===
    workflow_model = await get_workflow_filter(
        context_data=context_data,
        file_processor=file_processor,
        tracking_model=tracking_model,
    )

    # === Start session ===
    start_session_model = await call_workflow_session_start(
        context_data=context_data,
        file_processor=file_processor,
        tracking_model=tracking_model,
    )

    # === Process steps ===
    # Sort steps in ascending order by stepOrder
    data_input = None
    sorted_steps = sorted(workflow_model.workflowSteps, key=lambda step: step.stepOrder)
    for step in sorted_steps:
        # Start step
        start_step_model = await call_workflow_step_start(
            context_data=context_data,
            file_processor=file_processor,
            tracking_model=tracking_model,
            step=step,
        )
        # Execute step
        data_output = await execute_step(file_processor, context_data, step, data_input)
        data_input = data_output


async def get_workflow_filter(
    context_data: ContextData,
    file_processor: ProcessorBase,
    tracking_model: TrackingModel,
):
    logger.info(f"[{tracking_model.request_id}] Start workflow filter")
    body_data = {
        "filePath": file_processor.file_record["file_path_parent"],
        "fileName": file_processor.file_record["file_name"],
        "fileExtension": file_processor.file_record["file_extension"],
        "project": tracking_model.project_name,
        "source": tracking_model.source_name,
    }
    workflow_connector = BEConnector(
        ApiUrl.WORKFLOW_FILTER.full_url(), body_data=body_data
    )
    workflow_response = await workflow_connector.post()
    if not workflow_response:
        raise RuntimeError(f"[{tracking_model.request_id}] Failed to fetch workflow")

    workflow_model = WorkflowModel(**workflow_response)
    context_data.workflow.filter_api.url = ApiUrl.WORKFLOW_FILTER.full_url()
    context_data.workflow.filter_api.request = body_data
    context_data.workflow.filter_api.response = workflow_model

    tracking_model.workflow_name = (workflow_model.name,)
    tracking_model.workflow_id = (workflow_model.id,)
    tracking_model.sap_masterdata = bool(workflow_model.sapMasterData)

    return workflow_model


async def call_workflow_session_start(
    context_data: ContextData,
    file_processor: ProcessorBase,
    tracking_model: TrackingModel,
):
    logger.info(f"[{tracking_model.request_id}] Start session")
    body_data = {
        "workflowId": context_data.workflow.filter_api.response.id,
        "celeryId": tracking_model.request_id,
        "filePath": tracking_model.file_path,
    }
    session_connector = BEConnector(
        ApiUrl.WORKFLOW_SESSION_START.full_url(), body_data=body_data
    )
    session_response = await session_connector.post()
    start_session_model = WorkflowSession(**session_response)

    context_data.workflow.metadata_api.session_start_api.url = ApiUrl.WORKFLOW_SESSION_START.full_url()
    context_data.workflow.metadata_api.session_start_api.request = body_data
    context_data.workflow.metadata_api.session_start_api.response = start_session_model
    return start_session_model


def call_workflow_session_finish(context_data: ContextData):
    pass


async def call_workflow_step_start(
    context_data: ContextData,
    file_processor: ProcessorBase,
    tracking_model: TrackingModel,
    step: WorkflowStep,
):
    # Start step
    logger.info(f"[{tracking_model.request_id}] Starting step: {step.stepName}")
    body_data = {
        "sessionId": context_data.workflow.metadata_api.session_start_api.response.id,
        "stepId": step.workflowStepId,
    }
    start_step_response = await BEConnector(ApiUrl.WORKFLOW_STEP_START.full_url(), body_data).post()
    start_step_model = StartStep(**start_step_response)

    step_item = Step()
    step_item.step = step
    step_item.config_api = {}
    step_item.metadata_api = {}
    context_data.step.append(step_item)
    context_data.step[step.stepOrder - 1].metadata_api.Step_start_api.url = ApiUrl.WORKFLOW_STEP_START.full_url()
    context_data.step[step.stepOrder - 1].metadata_api.Step_start_api.request = body_data
    context_data.step[step.stepOrder - 1].metadata_api.Step_start_api.response = start_step_model
    return start_step_model


async def call_workflow_step_finish(
    context_data: ContextData,
    file_processor: ProcessorBase,
    tracking_model: TrackingModel,
    step: WorkflowStep,
):
    # Finish step
    logger.info(f"[{tracking_model.request_id}] Finish step: {step.stepName}")
    body_data = {
        "workflowHistoryId": context_data.step[step.stepOrder - 1].metadata_api.Step_start_api.response.workflowHistoryId,
        "code": StatusEnum.SUCCESS,
        "message": "",
        "dataInput": "input_data",
        "dataOutput": "",
    }
    finish_step_response = await BEConnector(ApiUrl.WORKFLOW_STEP_FINISH.full_url(),body_data=body_data).post()
