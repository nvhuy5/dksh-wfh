import json
from pathlib import Path
import io

import logging
from models.class_models import SourceType
from utils import log_helpers, ext_extraction

# ===
# Set up logging
logger_name = "TXT Processor"
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)
# ===


class TXTProcessor:
    """
    PO file: PO202404007116.txt
    """

    def __init__(self, file: Path, source: SourceType = SourceType.S3):
        self.file_path = file
        self.source = source

    def extract_text(self) -> str:
        """
        Extracts and returns the text content of the file.
        Works for both local and S3 sources.
        """
        file_object = ext_extraction.FileExtensionProcessor(
            file_path=self.file_path, source=self.source
        )

        if file_object.source == "local":
            with open(file_object.file_path, "r", encoding="utf-8") as f:
                text = f.read()
        else:
            # S3: read from in-memory buffer
            file_object.object_buffer.seek(0)
            text = file_object.object_buffer.read().decode("utf-8")

        return text

    def json_buffer(self):
        logger.info("Write file content after conversion to buffer!")
        json_data = json.dumps(self.process(), ensure_ascii=False)
        json_buffer = io.BytesIO()
        json_buffer.write(json_data.encode("utf-8"))
        json_buffer.seek(0)
        return json_buffer

    def parse_file_to_json(self):
        text = self.extract_text()
        lines = text.split("\n")
        json_data = {}
        products = []
        column = None
        logger.info(f"Start processing for file: {self.file_path}")

        for line in lines:
            line = line.strip()

            if not line or line.startswith("---"):
                continue

            if "PO" in line:
                key, value = line.split("-", 1)
                json_data[key.strip()] = value.strip()
                continue

            count = line.count("：")

            if count >= 2 and "\t" in line:
                parts = line.split("\t")
                for part in parts:
                    if "：" in part:
                        key, value = part.split("：", 1)
                        json_data[key.strip()] = value.strip()
            elif count == 1 and "\t" not in line:
                key, value = line.split("：", 1)
                json_data[key.strip()] = value.strip()
            elif line.startswith("料品代號"):
                column = [h.strip() for h in line.split("\t") if h.strip()]
            elif column and "\t" in line:
                values = [v.strip() for v in line.split("\t")]
                while len(values) < len(column):
                    values.append("")
                product = dict(zip(column, values))
                products.append(product)

        if products:
            json_data["products"] = products
        logger.info("File has been proceeded successfully!")

        return json_data
