import logging
import traceback
import pandas as pd
import json
from utils import log_helpers
from models.class_models import StatusEnum, StepOutput, ApiUrl, DocumentType
from connections.be_connection import BEConnector
from utils.middlewares.request_context import get_context_value
from models.traceability_models import ServiceLog, LogType

# === Logger setup ===
logger_name = f"Workflow Node - {__name__}"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ====================

FAILED_PARSE_API_MSG = "Failed to call template-parse API"


async def template_data_mapping(self, input_data: StepOutput) -> StepOutput:
    """
    Perform data mapping based on template mapping configuration.
    This step renames and reorders dataframe columns according to the mapping API response.
    """

    try:
        # === Traceability context (for logging and debugging) ===
        self.request_id = get_context_value("request_id")
        self.traceability_context_values = {
            key: val
            for key in [
                "file_path",
                "workflow_name",
                "workflow_id",
                "document_number",
                "document_type",
            ]
            if (val := get_context_value(key)) is not None
        }

        # Step 1: Call template-parse API to get template ID
        template_parse_resp = await BEConnector(
            ApiUrl.WORKFLOW_TEMPLATE_PARSE.full_url(),
            params={"workflowStepId": self.workflow_step_ids.get("TEMPLATE_FILE_PARSE")},
        ).get()

        if not template_parse_resp or not isinstance(template_parse_resp, list):
            raise RuntimeError(FAILED_PARSE_API_MSG)

        template_id = template_parse_resp[0]["templateFileParse"]["id"]

        
        # Step 2: Call data-mapping API to get mapping configuration
        mapping_resp = await BEConnector(
            ApiUrl.DATA_MAPPING.full_url(),
            params={"templateFileParseId": template_id},
        ).get()

        if not mapping_resp or "templateMappingHeaders" not in mapping_resp:
            raise RuntimeError(f"Mapping API did not return a valid response: {mapping_resp}")

        headers_sorted = sorted(
            mapping_resp["templateMappingHeaders"], key=lambda x: x["order"]
        )

        # Convert items to DataFrame
        df = pd.DataFrame(input_data.output.items)

        # Build mapping dictionary: only map when fromHeader is valid and different from header
        mapping_dict = {
            m["header"]: m["fromHeader"]
            for m in headers_sorted
            if m["fromHeader"] and m["fromHeader"] not in ("Unmapping", m["header"]) and m["header"] in df.columns
        }

        # Fail if no headers matched
        if not mapping_dict:
            error_msg = (
                f"Mapping failed: no headers matched. "
                f"Expected: {[m['fromHeader'] for m in headers_sorted if m['fromHeader'] not in ('Unmapping', m['header'])]}, "
                f"Found: {list(df.columns)}"
            )
            logger.error(error_msg)

            parsed_output = input_data.output.model_copy(
                update={
                    "step_status": StatusEnum.FAILED,
                    "messages": [error_msg],
                    "metadata": {"mapping_result": json.dumps({"error": error_msg})},
                }
            )
            return StepOutput(
                output=parsed_output,
                step_status=StatusEnum.FAILED,
                step_failure_message=[error_msg],
            )

        # Apply rename
        df = df.rename(columns=mapping_dict)

        # Update output with the mapped DataFrame
        updated_output = input_data.output.model_copy(
            update={"items": df.to_dict(orient="records")}
        )

        return StepOutput(
            output=updated_output,
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:  # pragma: no cover
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"[template_data_mapping] Failed to map data: {e}!\n{short_tb}",
            extra={
                "service": ServiceLog.MAPPING,
                "log_type": LogType.ERROR,
                **getattr(self, "traceability_context_values", {}),
                "traceability": getattr(self, "request_id", None),
                "document_type": DocumentType.ORDER,
            },
            exc_info=True,
        )

        failed_output = input_data.output.model_copy(
            update={"step_status": StatusEnum.FAILED, "messages": [str(e)]}
        )

        return StepOutput(
            output=failed_output,
            step_status=StatusEnum.FAILED,
            step_failure_message=[short_tb],
        )
