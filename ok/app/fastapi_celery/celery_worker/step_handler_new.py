import json
import traceback
import logging
import asyncio
from pydantic import BaseModel
from models.body_models import MASTER_DATA_LOAD_DATA_BODY
from processors.processor_nodes import STEP_DEFINITIONS
from processors.processor_base import ProcessorBase
from processors.common import template_helper
from models.class_models import ApiUrl, WorkflowStep, StepDefinition, StatusEnum, StepOutput
from models.tracking_models import ServiceLog, LogType, TrackingModel
from typing import Dict, Any, Callable, Optional
from utils import log_helpers
import config_loader
from utils.middlewares.request_context import get_context_value, set_context_values

# ===
# Set up logging
logger_name = "Step Handler"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===

SUB_STEP_DEFINITIONS = {
    "TEMPLATE_FILE_PARSE": {
        "function_name": "parse_file_to_json",
        "data_input": False,
        "data_output": Any,
        "target_storage": "workflow-node-materialized",
        "s3_key_prefix": True
    },
    "MASTER_DATA_LOAD_DATA": {
        "function_name": "call_api",
        "data_input": "context_data.items",
        "data_output": Any,
        "target_storage": "workflow-node-materialized",
        "s3_key_prefix": True,
        "url": ApiUrl.MASTER_DATA_LOAD_DATA,
        "method": "post",
        "body": MASTER_DATA_LOAD_DATA_BODY
    },
    "write_json_to_s3": {
        "function_name": "write_json_to_s3",
        "data_input": True,
        "data_output": Any,
    },
}


async def execute_step(step: WorkflowStep, file_processor: ProcessorBase, context: Dict, data_input: Any | None = None):
    step_name = step.stepName
    logger.info(f"Starting step: [{step_name}]")

    try:
        step_config = STEP_DEFINITIONS.get(step_name)
        if not step_config:
            raise ValueError(f"The step [{step_name}] is not yet defined")

        alias_entry = STEP_DEFINITIONS.get(step_name, {"function_name": step_name})
        method_name = alias_entry["function_name"]
        requires_input = alias_entry.get("data_input", False)
        method = getattr(file_processor, method_name, None)

        if method is None or not callable(method):
            raise AttributeError(f"Function '{method_name}' not found in FileProcessor.")

        if asyncio.iscoroutinefunction(method):
            result = await method(data_input) if requires_input else await method()
        else:
            result = method(data_input) if requires_input else method()

        logger.info(f"Step '{step_name}' executed successfully.")
        return result

    except AttributeError as e:
        logger.error(f"[Missing step]: {str(e)}")
        return None
    except Exception as e:
        logger.exception(f"Exception during step '{step_name}': {str(e)}")
        return None
