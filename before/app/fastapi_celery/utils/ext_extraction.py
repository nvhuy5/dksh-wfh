import os
import json
import traceback
from pathlib import Path
import logging

from utils import log_helpers, read_n_write_s3
from connections import aws_connection
from models.class_models import SourceType

import config_loader

# === Logging Setup ===
logger_name = 'Extension Detection'
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)

types_string = config_loader.get_config_value('support_types', 'types')
types_list = json.loads(types_string)
bucket_name = config_loader.get_config_value('s3_buckets', 'sftp_files')

class FileExtensionProcessor:
    def __init__(self, file_path: str, source: SourceType = SourceType.S3):
        if not isinstance(file_path, (str, Path)):
            raise ValueError("file_path must be a string or Path.")
        
        self.file_path = str(file_path)
        self.file_path_parent = None
        self.source = source
        self.object_buffer = None
        self.file_extension = None
        self.file_name = None

        self._prepare_object()

    def _prepare_object(self):
        logger.info(f"Loading file from {self.source}...")
        if self.source == SourceType.LOCAL:
            self._load_local_file()
        else:
            self._load_s3_file()
        
        self._extract_file_extension()

    def _load_local_file(self):
        if not os.path.isfile(self.file_path):
            raise FileNotFoundError(f"Local file '{self.file_path}' does not exist.")
        self.file_name = Path(self.file_path).name
        self.file_path_parent = str(Path(self.file_path).parent) + "/"

    def _load_s3_file(self):
        try:
            s3_connector = aws_connection.S3Connector(bucket_name=bucket_name)
            client = s3_connector.client

            # Use head_object for file size info (standard in boto3)
            head = client.head_object(Bucket=bucket_name, Key=self.file_path)
            file_size = head.get("ContentLength", 0)
            logger.info(f"File size: {file_size / (1024**2):.2f} MB")

            buffer = read_n_write_s3.get_object(
                client=client,
                bucket_name=bucket_name,
                object_name=self.file_path
            )

            if not buffer:
                logger.error(f"S3 object '{self.file_path}' not found - {traceback.format_exc()}")
                raise FileNotFoundError(f"S3 object '{self.file_path}' not found.")

            self.object_buffer = buffer
            self.file_name = Path(self.file_path).name
            self.file_path_parent = str(Path(self.file_path).parent) + "/"

        except Exception:
            logger.error(f"Error accessing file '{self.file_path}': {traceback.format_exc()}")
            raise FileNotFoundError(f"File '{self.file_path}' could not be loaded from S3.")

    def _extract_file_extension(self):
        suffix = Path(self.file_path).suffix.lower()
        if not suffix:
            logger.error("The file does not have an extension.")
            raise ValueError(f"File '{self.file_path}' has no extension.")

        if suffix not in types_list:
            logger.error(f"Unsupported file type: {suffix}")
            raise TypeError(
                f"Unsupported file extension '{suffix}'. Allowed types: {types_string}"
            )

        self.file_extension = suffix
        logger.info(f"File extension detected: {self.file_extension}")
