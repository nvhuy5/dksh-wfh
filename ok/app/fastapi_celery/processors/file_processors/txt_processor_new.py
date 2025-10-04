from typing import List
from pathlib import Path
import logging
import re

from models.class_models import SourceType, PODataParsed
from app.fastapi_celery.processors.common.txt_helper import TxtHelper
from utils import log_helpers

# ===
# Set up logging
logger_name = "TXT Processor"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


class Txt001Template(TxtHelper):
    """
    Processor for file '0809-1.TXT' with double-space-separated columns.
    """

    def parse_space_separated_lines(self, lines: List[str]) -> List[dict]:
        items = []
        for line in lines:
            values = re.split(r"\s{2,}", line.strip())
            item = {f"col_{i + 1}": value for i, value in enumerate(values)}
            items.append(item)
        return items

    def parse_file_to_json(self) -> PODataParsed:
        return super().parse_file_to_json(self.parse_space_separated_lines)


class Txt002Template(TxtHelper):
    """
    Processor for file '20240726-131542-w25out20240726å…¨è¯.TXT' with tab-separated columns.
    """

    def __init__(self, file_path: Path, source: SourceType = SourceType.S3):
        super().__init__(file_path, source, encoding="big5")

    def parse_tab_separated_lines(self, lines: List[str]) -> List[dict]:
        items = []
        for line in lines:
            if not line.strip():
                continue
            fields = line.strip().split("\t")
            item = {f"col_{i + 1}": field.strip() for i, field in enumerate(fields)}
            items.append(item)
        return items

    def parse_file_to_json(self) -> PODataParsed:
        return super().parse_file_to_json(self.parse_tab_separated_lines)


class Txt003Template(TxtHelper):
    """
    Processor for file 'DELV082001.TXT' with single-space-separated values.
    """

    def __init__(self, file_path: Path, source: SourceType = SourceType.S3):
        super().__init__(file_path, source, encoding="big5")

    def parse_space_separated_lines(self, lines: List[str]) -> List[dict]:
        items = []
        for line in lines:
            values = line.strip().split()
            item = {f"col_{i + 1}": value for i, value in enumerate(values)}
            items.append(item)
        return items

    def parse_file_to_json(self) -> PODataParsed:
        return super().parse_file_to_json(self.parse_space_separated_lines)


class Txt004Template(TxtHelper):
    """
    Processor for TXT file '20240711-143536-w25in20240711.TXT'.
    """

    def parse_tabular_data_with_headers(self, lines: List[str]) -> List[dict]:
        """
        Parse lines using the header row as keys and tab-separated values as data.
        Skips non-data lines above the header.
        """
        items = []
        headers = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Identify and extract headers
            if not headers and "HeaderText" in line and "Batch" in line:
                headers = [h.strip() for h in line.split("\t")]
                continue

            # Skip the decorative first line like "2024.07.11  Dynamic List Display"
            if not headers:
                continue

            # Parse data rows
            fields = [f.strip() for f in line.split("\t")]
            # Fill missing values with empty strings if needed
            while len(fields) < len(headers):
                fields.append("")

            row = dict(zip(headers, fields))
            items.append(row)

        return items

    def parse_file_to_json(self) -> PODataParsed:
        """
        Extract text, parse using header mapping, and return structured output.
        """
        return super().parse_file_to_json(self.parse_tabular_data_with_headers)
