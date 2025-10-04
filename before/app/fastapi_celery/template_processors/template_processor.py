# Third-Party Imports
from enum import Enum
from typing import Type
from dataclasses import dataclass

# Local Application Imports
from template_processors import file_processors

@dataclass
class ProcessorMeta:
    cls         : Type
    description : str
    input_type  : str
    output_type : str

class TemplateProcessor(Enum):
    """
    Template Registry
    """
    def create_instance(self, file_path):
        return self.value.cls(file_path)
    
    @property
    def description(self):
        return self.value.description

    def __repr__(self):
        return f"{self.name} ({self.input_type} → {self.output_type})"

    # ======================================================== #
    # === Registry the template to specific file processor === #
    PDF_0C_RL_H75_K0 = ProcessorMeta(
        cls         = file_processors.pdf_processor.PDFProcessor,
        description = "PDF layout processor for PO template - 0C_RL_H75_K0.pdf",
        input_type  = "pdf",
        output_type = "dataframe"
    )

    TXT_PO202404007116 = ProcessorMeta(
        cls         = file_processors.txt_processor.TXTProcessor,
        description = "TXT layout processor for PO template - PO202404007116.txt",
        input_type  = "txt",
        output_type = "dataframe"
    )

    EXCELTEMPLATE = ProcessorMeta(
        cls         = file_processors.excel_processor.ExcelProcessor,
        description = "Excel layout processor for all PO template",
        input_type  = "xls or xlsx",
        output_type = "dataframe"
    )