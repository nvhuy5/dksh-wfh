# Standard Library Imports
import time
import traceback
import logging
from datetime import datetime, timezone
from utils import log_helpers, ext_extraction, read_n_write_s3
from template_processors.template_processor import TemplateProcessor

# ===
# Set up logging
logger_name = "Template Manager Routers"
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)
# ===


class FileProcessor:

    PROCESSORS = {
        ".pdf": TemplateProcessor.PDF_0C_RL_H75_K0,
        ".txt": TemplateProcessor.TXT_PO202404007116,
        ".xlsx": TemplateProcessor.EXCELTEMPLATE,
        ".xls": TemplateProcessor.EXCELTEMPLATE,
    }

    def __init__(self, bucket_name: str, file_path: str):
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.file_record = {}

    def extract_metadata(self):
        try:
            file_processor = ext_extraction.FileExtensionProcessor(self.file_path)
            self.file_record = {
                "file_path": self.file_path,
                "file_path_parent": file_processor.file_path_parent,
                "file_name": file_processor.file_name,
                "file_extension": file_processor.file_extension,
                "proceed_at": datetime.fromtimestamp(
                    time.time_ns() / 1e9, timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S"),
            }
            return True
        except FileNotFoundError as e:
            logger.error(f"[extract_metadata] File not found: {e}", exc_info=True)
        except ValueError as e:
            logger.error(f"[extract_metadata] Value error: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"[extract_metadata] Unexpected error: {e}", exc_info=True)
        return False

    def parse_file_to_json(self):
        try:
            ext = self.file_record.get("file_extension")
            if ext not in self.PROCESSORS:
                logger.error(f"[parse_file_to_json] Unsupported file extension: {ext}")
                return None

            processor_class = self.PROCESSORS[ext]
            processor = processor_class.create_instance(file_path=self.file_path)
            json_data = processor.parse_file_to_json()
            return json_data

        except Exception as e:
            logger.error(
                f"[parse_file_to_json] Failed to parse file: {e}", exc_info=True
            )
            return None

    def write_json_to_s3(self, input_data):
        try:
            logger.info("Preparing to write JSON to S3")
            result = read_n_write_s3.write_json_to_s3(
                json=input_data,
                file_record=self.file_record,
                bucket_name=self.bucket_name,
            )

            logger.info("write_json_to_s3 completed.")
            return result

        except Exception as e:
            logger.error(f"Exception in write_json_to_s3: {e}", exc_info=True)
            return None

    def validation(self):
        pass
