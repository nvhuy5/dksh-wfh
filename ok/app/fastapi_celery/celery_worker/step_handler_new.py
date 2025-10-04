import json
import traceback
import logging
import asyncio
from connections.be_connection import BEConnector
from pydantic import BaseModel
from models.body_models import MASTER_DATA_LOAD_DATA_BODY
from processors.processor_nodes import STEP_DEFINITIONS
from processors.processor_base import ProcessorBase
from processors.common import template_helper
from models.class_models import ApiUrl, ContextData, WorkflowStep, StepDefinition, StatusEnum, StepOutput
from models.tracking_models import ServiceLog, LogType, TrackingModel
from typing import Dict, Any, Callable, List, Optional
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


async def execute_step(file_processor: ProcessorBase, context_data: ContextData, step: WorkflowStep, data_input: Any | None = None):
    step_name = step.stepName
    logger.info(f"Starting execute step: [{step_name}]")

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



def call_api_to_handle_sub_step(
    step: str, context: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Resolves the API URL, HTTP method, query parameters, and request body
    based on the workflow step.

    Args:
        step (str): The workflow step name.
        context (dict): External data (expects keys like 'file_name', 'workflowStepId').

    Returns:
        dict: A dictionary with keys: url (ApiUrl), method (str), params (dict), body (dict|None),
              or None if no matching step is found or context is invalid.
    """
    step_name = step.upper()

    step_map = {
        "FILE_PARSE": {
            "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE,
            "method": "get",
            "required_context": ["workflowStepId"],
            "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
            "body": None,
        },
        "VALIDATE_HEADER": {
            "url": ApiUrl.MASTERDATA_HEADER_VALIDATION,
            "method": "get",
            "required_context": ["file_name"],
            "params": lambda ctx: {"fileName": ctx["file_name"]},
            "body": None,
        },
        "VALIDATE_DATA": {
            "url": ApiUrl.MASTERDATA_COLUMN_VALIDATION,
            "method": "get",
            "required_context": ["file_name"],
            "params": lambda ctx: {"fileName": ctx["file_name"]},
            "body": None,
        },
        "MASTER_DATA_LOAD": {
            "url": ApiUrl.MASTER_DATA_LOAD_DATA,
            "method": "post",
            "required_context": ["file_name", "items"],
            "params": lambda ctx: {"fileName": ctx["file_name"]},
            "body": lambda ctx: {
                "fileName": ctx["file_name"],
                "data": ctx["items"],
            },
        },
        "TEMPLATE_DATA_MAPPING": {
            # Instead of direct url, we provide a runner for multiple dependent requests
            "runner": lambda ctx: run_chain(
                ctx,
                [
                    {
                        "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE,
                        "method": "get",
                        "required_context": ["workflowStepId"],
                        "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                        "body": None,
                        # store templateFileParseId from first response
                        "extract": lambda resp, ctx: ctx.update(
                            {"templateFileParseId": resp[0]["templateFileParse"]["id"]}
                        ),
                    },
                    {
                        "url": lambda ctx: f"{ApiUrl.DATA_MAPPING.full_url()}?templateFileParseId={ctx['templateFileParseId']}",
                        "method": "get",
                        "required_context": ["templateFileParseId"],
                        "params": lambda ctx: {
                            "templateFileParseId": ctx["templateFileParseId"]
                        },
                        "body": None,
                    },
                ],
            )
        },
        "TEMPLATE_FORMAT_VALIDATION": {
            # Instead of direct url, we provide a runner for multiple dependent requests
            "runner": lambda ctx: run_chain(
                ctx,
                [
                    {
                        "url": ApiUrl.WORKFLOW_TEMPLATE_PARSE,
                        "method": "get",
                        "required_context": ["workflowStepId"],
                        "params": lambda ctx: {"workflowStepId": ctx["workflowStepId"]},
                        "body": None,
                        # store templateFileParseId from first response
                        "extract": lambda resp, ctx: ctx.update(
                            {"templateFileParseId": resp[0]["templateFileParse"]["id"]}
                        ),
                    },
                    {
                        "url": lambda ctx: f"{ApiUrl.TEMPLATE_FORMAT_VALIDATION.full_url()}/{ctx['templateFileParseId']}",
                        "method": "get",
                        "required_context": ["templateFileParseId"],
                        "params": lambda _: {},
                        "body": None,
                    },
                ],
            )
        },
    }

    for key, config in step_map.items():
        if key in step_name:
            # Case 1: multi-step runner
            if "runner" in config:
                return {"runner": config["runner"]}

            # Check required context keys
            missing_keys = [k for k in config["required_context"] if k not in context]
            if missing_keys:
                raise RuntimeError(
                    f"Missing context keys for step '{step}': {missing_keys}"
                )

            return {
                "url": (
                    config["url"](context)
                    if callable(config["url"])
                    else config["url"].full_url()
                ),
                "method": config["method"],
                "params": config["params"](context),
                "body": (
                    config["body"](context)
                    if callable(config["body"])
                    else config["body"]
                ),
            }

    return None

async def run_chain(context: Dict[str, Any], steps: List[Dict[str, Any]]):
    """
    Executes a sequence of dependent API calls asynchronously using BEConnector.
    Each step may update context with values needed by later steps.
    """
    results = []
    for step in steps:
        url = step["url"](context) if callable(step["url"]) else step["url"].full_url()
        missing_keys = [k for k in step["required_context"] if k not in context]
        if missing_keys:
            raise RuntimeError(f"Missing context keys: {missing_keys}")

        # Use your BEConnector instead of requests
        logger.info(f"Running chain step: {url} with step {step}\ncontext: {context}")
        connector = BEConnector(url, params=step["params"](context))
        resp = await connector.get()
        results.append(resp)
        logger.info(f"Chain step response from {url}:\n{resp}")

        if "extract" in step:
            step["extract"](resp, context)

    # return the last response
    return results[-1]
