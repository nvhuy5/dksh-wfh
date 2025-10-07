import config_loader
from typing import Optional, Any, List
from models.class_models import (
    DocumentType,
    StatusEnum,
    StepOutput,
    MasterDataParsed,
    PODataParsed,
)
from models.traceability_models import ServiceLog, LogType
from utils import log_helpers, read_n_write_s3
import logging
import traceback
from utils.middlewares.request_context import get_context_value
from connections import aws_connection
from botocore.exceptions import ClientError
from template_processors.common import template_utils

# ===
# Set up logging
logger_name = f"Workflow Node - {__name__}"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


def write_json_to_s3(
    self,
    input_data: Any,
    s3_key_prefix: str = "",
    rerun_attempt: Optional[int] = None,
    **kwargs,
) -> StepOutput:
    """Writes JSON data to an S3 bucket based on document type.

    Selects the target S3 bucket based on `self.document_type` and writes `input_data`
    to S3 using `read_n_write_s3.write_json_to_s3`. Logs errors or warnings for invalid
    inputs or failures.

    Args:
        input_data (Any): JSON data to write to S3.
        s3_key_prefix (str, optional): Prefix for the S3 key. Defaults to "".

    Returns:
        Optional[str]: S3 key of the written file if successful, None otherwise.
    """
    # === Try to retrieve all traceability attributes when an object created
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
    logger.debug(
        f"Function: {__name__}\n"
        f"RequestID: {self.request_id}\n"
        f"TraceabilityContext: {self.traceability_context_values}"
    )

    # Define the target bucket based on the document type of PO and Master Data
    if self.document_type == DocumentType.MASTER_DATA:  # pragma: no cover
        # Update object_name attr for Masterdata
        customized_object_name = kwargs.get("customized_object_name")
        if customized_object_name:
            file_name = self.file_record["file_name"].removesuffix(
                self.file_record["file_extension"]
            )
            self.file_record["s3_key_prefix"] = f"process_data/{file_name}/"
            self.file_record["current_time"] = self.current_time

    elif self.document_type not in (
        DocumentType.ORDER,
        DocumentType.MASTER_DATA,
    ):
        logger.error(
            f"[write_json_to_s3] Unknown document type: {self.document_type}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **self.traceability_context_values,
                "traceability": self.request_id,
            },
        )
        return StepOutput(
            output=None,
            step_status=StatusEnum.FAILED,
            step_failure_message=[
                f"[write_json_to_s3] Unknown document type: {self.document_type}"
            ],
        )

    if not input_data:  # pragma: no cover
        logger.warning(
            "No input data provided to write_json_to_s3.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **self.traceability_context_values,
                "traceability": self.request_id,
            },
        )
        return StepOutput(
            output=None,
            step_status=None,
            step_failure_message=["No input data provided to write_json_to_s3."],
        )

    try:
        logger.info(
            "Preparing to write JSON to S3",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **self.traceability_context_values,
                "traceability": self.request_id,
            },
        )
        result = read_n_write_s3.write_json_to_s3(
            json_data=input_data,
            file_record=self.file_record,
            bucket_name=self.target_bucket_name,
            s3_key_prefix=s3_key_prefix,
            rerun_attempt=rerun_attempt,
        )

        logger.info(
            "write_json_to_s3 completed.",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.TASK,
                **self.traceability_context_values,
                "traceability": self.request_id,
            },
        )

        return StepOutput(
            output=result,
            step_status=StatusEnum.SUCCESS,
            step_failure_message=None,
        )

    except Exception as e:
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"Exception in write_json_to_s3: \n{short_tb}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **self.traceability_context_values,
                "traceability": self.request_id,
            },
            exc_info=True,
        )

        return StepOutput(
            output=None,
            step_status=StatusEnum.FAILED,
            step_failure_message=[short_tb],
        )


def check_step_result_exists_in_s3(
    self, task_id: str, step_name: str, rerun_attempt: Optional[int] = None
) -> Optional[MasterDataParsed | PODataParsed]:
    """
    Check S3 for step result.
    - If rerun_attempt is given, look for the previous attempt file.
    - If no rerun_attempt, look for the initial file.
    Returns True (skip) if found and step_status == "1", else False (needs rerun).
    """
    self.current_output_path = None
    self.request_id = get_context_value("request_id") or task_id
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

    try:
        # Build base prefix
        materialized_prefix = config_loader.get_config_value(
            "s3_buckets", "materialized_step_data_loc"
        )
        s3_key_prefix = f"{materialized_prefix}/{task_id}/{step_name}"

        # Determine base name exactly like write_json_to_s3 does
        if self.file_record.get("object_name"):
            base_name = self.file_record["object_name"].rsplit(".", 1)[0]
        else:
            base_name = self.file_record.get("file_name", step_name).rsplit(".", 1)[0]

        # Compute expected rerun attempt (always using "previous" attempt)
        if rerun_attempt is None or rerun_attempt == 1:
            # First attempt file
            s3_key = f"{s3_key_prefix.rstrip('/')}/{base_name}.json"
        else:
            all_possible_rerun_step_files = list_all_attempt_objects(
                client=aws_connection.S3Connector(
                    bucket_name=self.target_bucket_name
                ).client,
                bucket_name=self.target_bucket_name,
                base_path=s3_key_prefix,
                base_name=base_name,
                rerun_attempt=rerun_attempt,
            )
            # Add prefix path
            s3_key = all_possible_rerun_step_files[0]

        logger.debug(
            f"[check_step_result_exists_in_s3] Checking S3 key: {s3_key}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                **self.traceability_context_values,
            },
        )

        # Try to read JSON file
        self.current_output_path = s3_key
        data = read_n_write_s3.read_json_from_s3(self.target_bucket_name, s3_key)
        if data is None:
            logger.info(
                f"[check_step_result_exists_in_s3] No file found for '{step_name}' "
                f"attempt {rerun_attempt}. Will rerun.",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    **self.traceability_context_values,
                },
            )
            return None

        step_status = data.get("step_status")
        data.update(json_output=s3_key)
        if step_status == "1":
            logger.info(
                f"[check_step_result_exists_in_s3] Step '{step_name}' "
                f"attempt {rerun_attempt} has step_status=1. Skipping.",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    **self.traceability_context_values,
                },
            )
        else:
            logger.info(
                f"[check_step_result_exists_in_s3] Step '{step_name}' "
                f"attempt {rerun_attempt} has step_status={step_status}. Will rerun.",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    **self.traceability_context_values,
                },
            )

    except Exception as e:
        short_tb = "".join(
            traceback.format_exception(type(e), e, e.__traceback__, limit=3)
        )
        logger.error(
            f"[check_step_result_exists_in_s3] Exception: {e}\n{short_tb}",
            extra={
                "service": ServiceLog.FILE_STORAGE,
                "log_type": LogType.ERROR,
                **self.traceability_context_values,
            },
        )

    return template_utils.parse_data(document_type=self.document_type, data=data)


def list_all_attempt_objects(
    client,
    bucket_name: str,
    base_path: str,
    base_name: str,
    rerun_attempt: Optional[int] = None,
) -> List[str]:
    """
    Lists all rerun object keys in an S3 bucket in reverse order without stopping at first missing one.

    Parameters:
    ----------
    client : boto3 S3 client
    bucket_name : str
        Name of the S3 bucket.
    base_path : str
        The S3 prefix/folder where the objects are stored.
    base_name : str
        The base name of the file (without extension or rerun suffix).
    rerun_attempt : int, optional
        The current rerun attempt. Will check keys from (rerun_attempt - 1) to 0.

    Returns:
    -------
    List[str]
        A list of existing full S3 object keys in reverse attempt order.
    """
    all_possible_rerun_step_files = []
    base_path = base_path.rstrip("/")
    max_attempt = rerun_attempt or 1

    for attempt in reversed(range(max_attempt)):
        if attempt == 0:
            object_name = f"{base_name}.json"
        else:
            object_name = f"{base_name}_rerun_{attempt}.json"

        key = f"{base_path}/{object_name}"

        try:
            client.head_object(Bucket=bucket_name, Key=key)
            all_possible_rerun_step_files.append(key)
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise e
            # Do not break — continue checking earlier attempts

    return all_possible_rerun_step_files
