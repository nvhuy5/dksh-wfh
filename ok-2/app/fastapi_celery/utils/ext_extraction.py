import os
import json
import traceback
from pathlib import Path, PurePosixPath
import logging
from typing import Optional
from utils import log_helpers, read_n_write_s3
from connections import aws_connection
from models.class_models import SourceType, DocumentType
from models.traceability_models import ServiceLog, LogType

import config_loader
from utils.middlewares.request_context import get_context_value, set_context_values

# ===
# === Logging Setup ===
logger_name = "Extension Detection"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})

types_string = config_loader.get_config_value("support_types", "types")
types_list = json.loads(types_string)


class FileExtensionProcessor:
    """
    Processes file extensions and determines document types from file paths.

    Loads files from local or S3 sources, validates extensions, and identifies document types.

    Attributes:
        file_path (str): File path being processed.
        file_path_parent (str | None): Parent directory of the file.
        source (SourceType): File source (LOCAL or S3).
        object_buffer (bytes | None): File content buffer (S3 only).
        file_extension (str | None): File extension.
        file_name (str | None): File name.
        client (boto3.client | None): AWS S3 client.
        document_type (DocumentType): Determined document type.
    """

    def __init__(self, file_path: str, source: SourceType = SourceType.S3):
        """
        Initialize with file path and source type.

        Args:
            file_path (str): Path to the file.
            source (SourceType, optional): File source (LOCAL or S3). Defaults to S3.

        Raises:
            ValueError: If file_path is not a string or Path.
        """
        if not isinstance(file_path, (str, Path)):
            raise ValueError("file_path must be a string or Path.")

        self.request_id = get_context_value("request_id")
        self.file_path = str(file_path)
        self.file_path_parent = None
        self.source = source
        self.object_buffer = None
        self.file_extension = None
        self.file_name = None
        self.client = None
        self.document_type = self._get_document_type()
        self.target_bucket_name = self._get_target_bucket(self.document_type)

        self._prepare_object()

    def _prepare_object(self) -> None:
        """
        Load file from source and extract extension.

        Raises:
            FileNotFoundError: If file doesn't exist.
            ValueError: If file has no or unsupported extension.
        """
        logger.info(
            f"Loading file from {self.source}...",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.TASK,
                "file_path": self.file_path,
                "traceability": self.request_id,
            },
        )
        if self.source == SourceType.LOCAL:
            self._load_local_file()
        else:
            self._load_s3_file()

        self._extract_file_extension()

    def _load_local_file(self) -> None:
        """
        Load local file and set name and parent directory.

        Raises:
            FileNotFoundError: If local file doesn't exist.
        """
        if not os.path.isfile(self.file_path):
            raise FileNotFoundError(f"Local file '{self.file_path}' does not exist.")
        self.file_name = Path(self.file_path).name
        self.file_path_parent = str(Path(self.file_path).parent) + "/"

    def _load_s3_file(self) -> None:  # pragma: no cover  # NOSONAR
        """
        Load S3 file and set name, parent directory, and buffer.

        Raises:
            FileNotFoundError: If S3 object cannot be loaded.
        """
        try:
            raw_bucket = self._get_raw_bucket()
            s3_connector = aws_connection.S3Connector(bucket_name=raw_bucket)
            self.client = s3_connector.client
            self._get_file_capacity()

            buffer = read_n_write_s3.get_object(
                client=self.client,
                bucket_name=raw_bucket,
                object_name=self.file_path,
            )

            if not buffer:  # pragma: no cover  # NOSONAR
                logger.error(
                    f"S3 object '{self.file_path}' not found - {traceback.format_exc()}",
                    extra={
                        "service": ServiceLog.METADATA_EXTRACTION,
                        "log_type": LogType.ERROR,
                        "file_path": self.file_path,
                        "traceability": self.request_id,
                    },
                )
                raise FileNotFoundError(f"S3 object '{self.file_path}' not found.")

            self.object_buffer = buffer
            self.file_name = Path(self.file_path).name
            self.file_path_parent = str(Path(self.file_path).parent) + "/"

        except Exception:
            logger.error(
                f"Error accessing file '{self.file_path}': {traceback.format_exc()}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.ERROR,
                    "file_path": self.file_path,
                    "traceability": self.request_id,
                },
            )
            raise FileNotFoundError(
                f"File '{self.file_path}' could not be loaded from S3."
            )

    def _extract_file_extension(self) -> None:  # pragma: no cover  # NOSONAR
        """
        Extract and validate file extension.

        Raises:
            ValueError: If file has no extension.
            TypeError: If extension is unsupported.
        """
        suffix = Path(self.file_path).suffix.lower()
        if not suffix:
            logger.error(
                "The file does not have an extension.",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.ERROR,
                    "file_path": self.file_path,
                    "traceability": self.request_id,
                },
            )
            raise ValueError(f"File '{self.file_path}' has no extension.")

        if suffix not in types_list:
            logger.error(
                f"Unsupported file type: {suffix}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.ERROR,
                    "file_path": self.file_path,
                    "traceability": self.request_id,
                },
            )
            raise TypeError(
                f"Unsupported file extension '{suffix}'. Allowed types: {types_string}"
            )

        self.file_extension = suffix
        logger.info(
            f"File extension detected: {self.file_extension}",
            extra={
                "service": ServiceLog.METADATA_EXTRACTION,
                "log_type": LogType.TASK,
                "file_path": self.file_path,
                "traceability": self.request_id,
            },
        )

    # Function to calculate the size of a file
    def _get_file_capacity(self) -> str:  # pragma: no cover  # NOSONAR
        """
        Calculate file size in KB or MB.

        Returns:
            str: File size (e.g., "1.23 MB" or "456.78 KB").

        Raises:
            FileNotFoundError: If file cannot be accessed.
        """
        raw_bucket = self._get_raw_bucket()
        if self.source == SourceType.LOCAL:
            size_bytes = os.path.getsize(self.file_path)
        else:
            # Use head_object for file size info (standard in boto3)
            head = self.client.head_object(Bucket=raw_bucket, Key=self.file_path)
            size_bytes = head.get("ContentLength", 0)

        if size_bytes / (1024**2) >= 1:
            size_mb = size_bytes / (1024**2)
            logger.info(
                f"Size of the input file {self.file_path} in MB: {size_mb:.2f}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.TASK,
                    "file_path": self.file_path,
                    "traceability": self.request_id,
                },
            )
            return f"{size_mb:.2f} MB"
        else:
            size_kb = size_bytes / 1024
            logger.info(
                f"Size of the input file {self.file_path} in KB: {size_kb:.2f}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.TASK,
                    "file_path": self.file_path,
                    "traceability": self.request_id,
                },
            )
            return f"{size_kb:.2f} KB"

    # Extracts the root directory from a file path to determine the document type.
    def _get_document_type(self) -> DocumentType:
        """
        Extracts the root directory from a file path to determine the document type.
        :return: DocumentType indicating the document type based on the root directory.
        :raises ValueError: If the file path is invalid or cannot be parsed correctly.
        """
        try:
            if self.source == SourceType.LOCAL:
                parts = Path(os.path.normpath(self.file_path)).parts
            else:
                parts = PurePosixPath(self.file_path).parts

            if not parts:
                logger.error(
                    f"Invalid file path: '{self.file_path}'. No path components found.",
                    extra={
                        "service": ServiceLog.METADATA_EXTRACTION,
                        "log_type": LogType.ERROR,
                        "file_path": self.file_path,
                        "traceability": self.request_id,
                    },
                )
                raise ValueError(
                    f"Invalid file path: '{self.file_path}'. No path components found."
                )

            logger.info(
                f"The file_path's exposed: {parts}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.TASK,
                    "file_path": self.file_path,
                    "traceability": self.request_id,
                },
            )
            document_type = (
                DocumentType.MASTER_DATA
                if any(part.lower() == "master_data" for part in parts)
                else DocumentType.ORDER
            )

            # Safely publish the document_type only if request_id is present
            if self.request_id is not None:
                set_context_values(document_type=document_type)

            return document_type

        except Exception as e:
            logger.error(
                f"Error determining document type from path '{self.file_path}': {traceback.format_exc()}",
                extra={
                    "service": ServiceLog.METADATA_EXTRACTION,
                    "log_type": LogType.ERROR,
                    "file_path": self.file_path,
                    "traceability": self.request_id,
                },
            )
            raise ValueError(
                f"Error determining document type from path '{self.file_path}': {e}"
            )

    @staticmethod
    def _get_target_bucket(document_type: DocumentType) -> Optional[str]:
        request_id = get_context_value("request_id")
        project_name = get_context_value("project_name")
        traceability_context_values = {
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
        # Determine bucket name based on document type
        bucket_name = None

        if document_type == DocumentType.ORDER:
            if project_name and project_name.upper() == "DKSH_TW":
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "datahub_s3_process_data_tw"
                )
            elif project_name and project_name.upper() == "DKSH_VN":
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "datahub_s3_process_data_vn"
                )

        elif document_type == DocumentType.MASTER_DATA:
            sap_masterdata = get_context_value("sap_masterdata")
            if sap_masterdata:
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "sap_masterdata_bucket"
                )
            elif project_name and project_name.upper() == "DKSH_TW":
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "tw_masterdata_bucket"
                )
            elif project_name and project_name.upper() == "DKSH_VN":
                bucket_name = config_loader.get_config_value(
                    "s3_buckets", "vn_masterdata_bucket"
                )
        else:
            logger.warning(
                f"Unknown document type: {document_type}",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.WARNING,
                    **traceability_context_values,
                    "traceability": request_id,
                },
            )
            return None

        return bucket_name

    @staticmethod
    def _get_raw_bucket() -> Optional[str]:
        """
        Get raw_bucket base on project.
        """
        request_id = get_context_value("request_id")
        project_name = get_context_value("project_name")
        traceability_context_values = {
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
        bucket_name = None
        if project_name and project_name.upper() == "DKSH_TW":
            bucket_name = config_loader.get_config_value(
                "s3_buckets", "datahub_s3_raw_data_tw"
            )
        elif project_name and project_name.upper() == "DKSH_VN":
            bucket_name = config_loader.get_config_value(
                "s3_buckets", "datahub_s3_raw_data_vn"
            )
        else:
            logger.warning(
                f"Unknown project_name: {project_name}",
                extra={
                    "service": ServiceLog.FILE_STORAGE,
                    "log_type": LogType.WARNING,
                    **traceability_context_values,
                    "traceability": request_id,
                },
            )
        return bucket_name
