from models.class_models import StepDefinition

PROCESS_FUNCTIONS = [
    "extract_metadata",
    "template_mapping",
    "parse_file_to_json",
    "publish_data",
    "template_validation",
    "write_json_to_s3",
    # === Masterdata only ===
    "master_validation",
    "write_raw_to_s3",
    # === Masterdata only ===
]

STEP_DEFINITIONS = {
    "TEMPLATE_FILE_PARSE": StepDefinition(
        function_name="parse_file_to_json",
        data_input=None,
        data_output="parsed_data",
        extract_to={
            "document_number": "po_number",  # PO_NUMBER
            "document_type": "document_type",  # master_data or order
        },
        # Write data to S3 after the step
        store_materialized_data=True,
        target_folder="workflow-node-materialized"
    ),
    "TEMPLATE_DATA_MAPPING": StepDefinition(
        function_name="template_data_mapping",
        data_input="parsed_data",
        data_output="mapped_data",
        store_materialized_data=True,
    ),
    "TEMPLATE_FORMAT_VALIDATION": StepDefinition(
        function_name="template_format_validation",
        data_input="parsed_data",
        data_output="data_validation",
        store_materialized_data=True,
    ),
    "write_json_to_s3": StepDefinition(
        function_name="write_json_to_s3",
        data_input="parsed_data",
        data_output="s3_result",
        store_materialized_data=True,
    ),
    "publish_data": StepDefinition(
        function_name="publish_data",
        data_output="publish_data_result",
        store_materialized_data=True,
    ),
    # === Masterdata only ===
    "MASTER_DATA_FILE_PARSER": StepDefinition(
        function_name="parse_file_to_json",
        data_input=None,
        data_output="master_data_parsed",
        store_materialized_data=True,
    ),
    "MASTER_DATA_VALIDATE_HEADER": StepDefinition(
        function_name="masterdata_header_validation",
        data_input="master_data_parsed",
        data_output="masterdata_header_validation",
        store_materialized_data=True,
    ),
    "MASTER_DATA_VALIDATE_DATA": StepDefinition(
        function_name="masterdata_data_validation",
        data_input="master_data_parsed",
        data_output="masterdata_data_validation",
        store_materialized_data=True,
    ),
    "MASTER_DATA_LOAD_DATA": StepDefinition(
        function_name="write_json_to_s3",
        data_input="masterdata_data_validation",
        data_output="s3_result",
        # Custom flag to customize the object path to S3
        # The value will be handled in the function via **kwargs arg
        kwargs={"customized_object_name": True},
        store_materialized_data=True,
    ),
    # === Masterdata only ===
}
