from pathlib import Path
import traceback
import logging
from utils import log_helpers
from models.class_models import MasterDataParsed, SourceType, StatusEnum
from template_processors.common import excel_file_processor
import config_loader

METADATA_SEPARATOR = config_loader.get_env_variable("METADATA_SEPARATOR", "：")

# ===
# Set up logging
logger_name = "Excel Processor"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


class ExcelMasterdataProcessor(excel_file_processor.ExcelProcessor):
    """Processor for handling Excel file operations.

    Initializes with a file path and source type, reads rows from the file,
    and provides methods to parse the content into JSON format.
    """

    def __init__(self, file_path: Path, source: SourceType = SourceType.S3):
        """Initialize the Excel processor with a file path and source type.

        Args:
            file (Path): The path to the Excel file.
            source (SourceType, optional): The source type, defaults to SourceType.S3.
        """
        super().__init__(file_path=file_path, source=source)
        self.po_number = None

    def parse_file_to_json(self) -> MasterDataParsed:  # NOSONAR
        """Parse the Excel file content into a JSON-compatible dictionary.

        Extracts metadata and table data from rows, handling key-value pairs
        and table structures separated by METADATA_SEPARATOR.

        Returns:
            MasterDataParsed: A dictionary with 'metadata' (Dict[str, str]) and 'items' (List[Dict[str, str]]).
        """
        try:
            metadata = {}
            items = []
            headers = []
            i = 0

            while i < len(self.rows):
                row = self._clean_row(self.rows[i])

                # Extract metadata
                key_value_pairs = self.extract_metadata(row)
                if key_value_pairs:
                    metadata.update(key_value_pairs)
                    i += 1
                    continue

                # Try to extract table block
                headers = row
                table_block, next_index, updated_metadata = self._extract_table_block(
                    i + 1, headers
                )

                if updated_metadata:
                    metadata.update(updated_metadata)

                for row_data in table_block:
                    items.append(dict(zip(headers, row_data)))
                i = next_index if table_block else i + 1

            return MasterDataParsed(
                original_file_path=self.file_path,
                headers=headers,
                document_type=getattr(self, "document_type", None),
                items=items,
                step_status=StatusEnum.SUCCESS,
                message=None,
                capacity=getattr(self, "capacity", None),
            )

        except Exception as e:
            short_tb = "".join(
                traceback.format_exception(type(e), e, e.__traceback__, limit=3)
            )
            print(f"Error while parsing file to JSON: {e}")
            return MasterDataParsed(
                original_file_path=self.file_path,
                headers=[],
                document_type=getattr(self, "document_type", None),
                items=[],
                step_status=StatusEnum.FAILED,
                message=short_tb,
                capacity=getattr(self, "capacity", None),
            )

    def _clean_row(self, row):
        return [str(cell).strip() for cell in row]

    def _extract_table_block(self, start_index: int, header_row: list) -> tuple:
        table_block = []
        updated_metadata = {}
        i = start_index

        while i < len(self.rows):
            current_row = self._clean_row(self.rows[i])
            key_value_pairs = self.extract_metadata(current_row)

            if key_value_pairs:
                updated_metadata.update(key_value_pairs)
                break

            if len(current_row) == len(header_row):
                table_block.append(current_row)
                i += 1
            else:
                break

        return table_block, i, updated_metadata
