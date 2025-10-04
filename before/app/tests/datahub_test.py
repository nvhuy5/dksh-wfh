import pytest
from pathlib import Path

from fastapi_celery.template_processors.file_processors import excel_processor, pdf_processor, txt_processor
from fastapi_celery.utils.ext_extraction import FileExtensionProcessor


# Pytest fixture to get the base path pointing to samples directory
@pytest.fixture
def base_path():
    return Path(__file__).parent / "samples"

def test_valid_extension(base_path):
    """
    Test a file with a valid extension.
    """
    file_path = base_path / "0C-RLBH75-K0.pdf"
    file_processor = FileExtensionProcessor(file_path, source="local")
    result = file_processor.file_extension
    assert result == ".pdf"

# === PDF format files === #
@pytest.mark.parametrize(
    "file_name, field_name, expected_value",
    [
        ("0C-RLBH75-K0.pdf", "訂購編號", "0C-RLBH75-K0"),
        ("0C-RLBH75-K0.pdf", "訂購日期", "113年08月20日"),
        ("0C-RLBH75-K0.pdf", "採購主辦", "曾瀞盈"),
        ("0C-RLBH75-K0.pdf", "民國年", "66"),
        ("0C-RLBH75-K0.pdf", "未稅總金額", "以上未稅訂購總金額合計：新台幣6204.71元整")
    ]
)
def test_po_ocrlbh75k0_pdf(base_path, file_name, field_name, expected_value):
    """
    Test extraction of dynamic fields from the extracted PO data.
    """
    file_path = base_path / file_name
    result = pdf_processor.PDFProcessor(file=file_path, source="local").parse_file_to_json()
    assert result, "Extraction result is empty."

    # Retrieve the first PO data
    first_po = result[0]
    assert field_name in first_po, f"Field '{field_name}' not found in extracted result."
    assert first_po[field_name] == expected_value, f"Mismatch for {field_name}: {first_po[field_name]} != {expected_value}"

# === TXT format files === #
@pytest.mark.parametrize(
    "file_name, field_name, expected_value",
    [
        ("PO202404007116.txt", "傳真號碼", "02 -87526100"),
        ("PO202404007116.txt", "需求單號", "MR202404015887"),
    ]
)
def test_po202404007116_txt(base_path, file_name, field_name, expected_value):
    """
    Test extraction of dynamic fields from the extracted PO data (傳真號碼, 需求單號).
    """
    file_path = base_path / file_name
    result = txt_processor.TXTProcessor(file=file_path, source="local").parse_file_to_json()

    assert result, "Extraction result is empty."
    assert field_name in result, f"Field '{field_name}' not found in extracted result."
    assert result[field_name] == expected_value, f"Mismatch for {field_name}: {result[field_name]} != {expected_value}"

# === Excel format files === #
@pytest.mark.parametrize(
    "file_name, expected_order_id, expected_store_name",
    [
        ("0808三友WX.xls", "1411733659", "7117南港中信店"),
        ("0808三友WX.xls", "1415308842", "7153澎湖澎坊三號港店"),
    ]
)
def test_excel_itemvalue(base_path, file_name, expected_order_id, expected_store_name):
    file_path = base_path / file_name
    result = excel_processor.ExcelProcessor(file=file_path, source="local").parse_file_to_json()
    items = result["items"]

    matched_item = next((item for item in items if item["訂單編號"] == expected_order_id), None)
    assert matched_item is not None, f"Expected to find item with 訂單編號={expected_order_id}, but not found."
    assert matched_item["店號/店名"] == expected_store_name


