from io import BytesIO
from pathlib import Path
import pandas as pd
import re
from typing import List, Dict, Any

import logging
from utils import log_helpers, ext_extraction
from models.class_models import SourceType
import config_loader

METADATA_SEPARATOR = config_loader.get_env_variable("METADATA_SEPARATOR", "：")

# ===
# Set up logging
logger_name = 'Excel Processor'
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)
# ===

class ExcelProcessor:
    def __init__(self, file: Path, source: SourceType = SourceType.S3):
        self.file_path = file
        self.source = source
        self.rows = self.read_rows()

    def read_rows(self) -> List[List[str]]:
        file_object = ext_extraction.FileExtensionProcessor(file_path=self.file_path, source=self.source)
        file_object._extract_file_extension()
        ext = file_object.file_extension

        # Load file from local or S3
        if file_object.source == "local":
            file_input = file_object.file_path  # this is Path
        else:
            file_object.object_buffer.seek(0)
            file_input = BytesIO(file_object.object_buffer.read())  # this is BytesIO

        # Choose engine based on extension
        if ext == ".xls":
            df_dict = pd.read_excel(file_input, sheet_name=None, header=None, dtype=str, engine="xlrd")
        else:
            df_dict = pd.read_excel(file_input, sheet_name=None, header=None, dtype=str, engine="openpyxl")

        # Extract all rows from all sheets
        all_rows = []
        for sheet_name, sheet_df in df_dict.items():
            sheet_df.fillna("", inplace=True)
            all_rows.extend(sheet_df.astype(str).values.tolist())

        return [row for row in all_rows if any(cell.strip() for cell in row)]

    def parse_file_to_json(self) -> Dict[str, Any]:
        metadata = {}
        items = []
        i = 0

        while i < len(self.rows):
            row = [str(cell).strip() for cell in self.rows[i]]
            key_value_pairs = self.extract_metadata(row)

            if key_value_pairs:
                metadata.update(key_value_pairs)
                i += 1
                continue

            # Start checking for table data
            header_row = row
            table_block = []
            j = i + 1

            while j < len(self.rows):
                current_row = [str(cell).strip() for cell in self.rows[j]]
                kv_pairs = self.extract_metadata(current_row)

                if kv_pairs:
                    metadata.update(kv_pairs)
                    break

                if len(current_row) == len(header_row):
                    table_block.append(current_row)
                    j += 1
                else:
                    break

            if table_block:
                headers = header_row
                for row_data in table_block:
                    items.append(dict(zip(headers, row_data)))
                i = j
            else:
                i += 1

        return {"metadata": metadata, "items": items}

    def extract_metadata(self, row: List[str]) -> Dict[str, str]:
        metadata = {}
        cells = [cell.strip() for cell in row if cell.strip()]
        if not cells:
            return metadata

        handled_cells = set()

        # Special case: key-value inside parentheses
        for cell in cells:
            match = re.search(
                r"(.*)\(([^()]*?" + re.escape(METADATA_SEPARATOR) + r"[^()]*)\)", cell
            )
            if match:
                metadata["header"] = cell
                inner = match.group(2)
                if METADATA_SEPARATOR in inner:
                    key, value = map(str.strip, inner.split(METADATA_SEPARATOR, 1))
                    if key:
                        metadata[key] = value if value else None
                handled_cells.add(cell)

        for idx, cell in enumerate(cells):
            if cell in handled_cells:
                continue
            if METADATA_SEPARATOR in cell:
                if METADATA_SEPARATOR == ":" and re.match(r"https?://", cell):
                    continue
                parts = cell.split(METADATA_SEPARATOR, 1)
                key = parts[0].strip()
                value = parts[1].strip()
                if not value and idx + 1 < len(cells):
                    value = cells[idx + 1].strip()
                if key:
                    metadata[key] = value if value else None

        return metadata
