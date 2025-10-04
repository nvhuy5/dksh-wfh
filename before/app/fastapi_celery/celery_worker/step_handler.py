import logging
import asyncio
from typing import Any
from template_processors.file_processor import FileProcessor
from models.class_models import WorkflowStep
from utils import log_helpers

# ===
# Set up logging
logger_name = "Step Handler"
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)
# ===

STEP_DEFINITIONS = {
    "file_parse": {
        "function_name": "parse_file_to_json",
        "data_input": False,
        "data_output": Any,
    },
    "validation": {
        "function_name": "validation",
        "data_input": True,
        "data_output": Any,
    },
    "write_json_to_s3": {
        "function_name": "write_json_to_s3",
        "data_input": True,
        "data_output": Any,
    },
}

async def execute_step(file_processor: FileProcessor, step: WorkflowStep, data_input: Any | None = None):
    step_name = step.stepName
    logger.info(f"Executing step: {step_name}")

    try:
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
