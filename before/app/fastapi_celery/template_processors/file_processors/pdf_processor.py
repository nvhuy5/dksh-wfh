import re
from pathlib import Path
import pymupdf as fitz

import logging
from utils import ext_extraction
from utils import log_helpers
from models.class_models import SourceType

# ===
# Set up logging
logger_name = 'PDF Processor'
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)
# ===

class PDFProcessor:
    """
    PO file: 0C-RLBH75-K0.pdf
    """
    def __init__(self, file: Path, source: SourceType = SourceType.S3):
        self.file_path  = file
        self.source     = source

    def extract_po_fields(self, text_lines):
        data = {
            "訂購編號": None,
            "交貨地點": None,
            "交貨地址": None,
            "聯絡電話": None,
            "採購主辦": None,
            "民國年": None,
            "訂購日期": None,
            "未稅總金額": None,
            "品項": [],
            "備註": []
        }

        idx = 0
        while idx < len(text_lines):
            line = text_lines[idx].strip()

            if "訂購編號" in line:
                po_number_match = re.search(r'訂購編號：([A-Za-z0-9-]+)', line)
                if po_number_match:
                    data["訂購編號"]    = po_number_match.group(1)

            elif "交貨地點" in line:
                data["交貨地點"]        = line.split(":", 1)[-1].strip()

            elif "交貨地址" in line:
                data["交貨地址"]        = line.split(":", 1)[-1].strip()

            elif "TEL" in line or "聯絡電話" in line:
                data["聯絡電話"]        = line.split(":", 1)[-1].strip()

            elif "採購主辦" in line:
                match = re.search(r'採購主辦\s*:\s*(.+?)\((\d+)\)', line)
                if match:
                    data["採購主辦"]    = match.group(1)
                    data["民國年"]      = match.group(2)
                
                date_match = re.search(r'(?P<year>\d{1,4})年(?P<month>\d{1,2})月(?P<day>\d{1,2})日', line)
                if date_match:
                    data["訂購日期"]    = date_match.group()
            
            elif re.search(r'未稅[^金額\d]{0,50}?金額[^\d]{0,20}?\d+', line):
                data["未稅總金額"]      = line

            elif re.fullmatch(r'\d{4}', line) and idx + 8 < len(text_lines):
                product_block   = text_lines[idx:idx+9]
                product         = self.parse_product_block(product_block)
                if product:
                    data["品項"].append(product)
                idx += 9
                continue

            else:
                data["備註"].append(line)

            idx += 1

        return data

    def parse_product_block(self, lines):
        if len(lines) < 9:
            return None

        return {
            "項次": lines[0].strip(),
            "料號": lines[1].strip(),
            "品名及規格": lines[2].strip(),
            "數量": lines[3].strip(),
            "廠牌型號": lines[4].strip(),
            "單價": lines[5].strip(),
            "預交日期": lines[6].strip(),
            "單位": lines[7].strip(),
            "備註": lines[8].strip()
        }

    def parse_file_to_json(self):
        try:
            file_object = ext_extraction.FileExtensionProcessor(
                file_path   = self.file_path,
                source      = self.source
            )
            if file_object.source == "local":
                doc = fitz.open(file_object.file_path)
            else:
                # For S3: open from buffer
                file_object.object_buffer.seek(0)
                doc = fitz.open(
                    stream      = file_object.object_buffer.read(),
                    filetype    = "pdf"
                )
            
            all_data = []
            logger.info(f"Start processing for file: {self.file_path}")

            for page_num in range(len(doc)):
                page                        = doc[page_num]
                text                        = page.get_text()
                logger.info(f"\n--- Page {page_num + 1} ---\n{text}")

                lines                       = [
                    line.strip()
                    for line in text.split("\n")
                    if line.strip()
                ]
                extracted                   = self.extract_po_fields(lines)
                extracted["page_number"]    = page_num + 1
                all_data.append(extracted)

            doc.close()
            logger.info("File has been processed successfully!")

            return all_data

        except Exception as e:
            logger.error(f"Error generating JSON from PDF: {e}")
            raise
