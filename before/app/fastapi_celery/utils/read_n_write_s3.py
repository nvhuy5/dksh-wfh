# Standard Library Imports
import io
import json
import logging
import traceback

# Third-Party Imports
from botocore.exceptions import BotoCoreError, ClientError

# Local Application Imports
from utils import log_helpers
from connections import aws_connection

# === Set up logging ===
logger_name = 'Read and Write to S3'
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)

# === Upload to S3 ===
def put_object(client, bucket_name, object_name, uploading_data, **kwargs):
    try:
        if isinstance(uploading_data, (io.BytesIO, io.StringIO)):
            uploading_data.seek(0)
            client.upload_fileobj(
                uploading_data,
                Bucket=bucket_name,
                Key=object_name
            )
            logger.info(f"Buffer uploaded successfully to {bucket_name}/{object_name}.")
            return {"status": "Success"}
        elif isinstance(uploading_data, str):
            client.upload_file(
                Filename=uploading_data,
                Bucket=bucket_name,
                Key=object_name
            )
            logger.info(f"File '{uploading_data}' uploaded successfully to {bucket_name}/{object_name}.")
            return {"status": "Success"}
        else:
            msg = "uploading_data must be either a buffer-like object or a file path"
            logger.error(msg)
            return {"status": "Failed", "error": msg}

    except (ClientError, BotoCoreError, TypeError) as e:
        logger.error(f"Error uploading to S3: {type(e).__name__} - {e}")
        return {"status": "Failed", "error": str(e)}

# === Download from S3 ===
def get_object(client, bucket_name, object_name, **kwargs):
    try:
        response = client.get_object(Bucket=bucket_name, Key=object_name)
        data = response['Body'].read()
        buffer = io.BytesIO(data)
        logger.info(f"Object '{object_name}' read successfully from bucket '{bucket_name}'.")
        # return {"status": "Success", "data": buffer}
        return buffer
    except (ClientError, BotoCoreError) as e:
        logger.error(f"Error reading object '{object_name}/{bucket_name}': {type(e).__name__} - {e}")
        # return {"status": "Failed", "error": str(e)}
        return None

# === Check if object exists or get metadata ===
def object_exists(client, bucket_name, object_name):
    try:
        response = client.head_object(Bucket=bucket_name, Key=object_name)
        logger.info(f"Metadata for object '{object_name}' retrieved successfully from bucket '{bucket_name}'.")
        return True, response
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.warning(f"Object '{object_name}' not found in bucket '{bucket_name}'.")
            return False, None
        logger.error(f"Error accessing object metadata: {type(e).__name__} - {e}")
        return False, None


def write_json_to_s3(json_data, file_record, bucket_name):
    try:
        logger.info(f"Start uploading JSON to S3.\nFile record: {file_record}")
        object_name = f"{file_record['file_name'].rsplit('.', 1)[0]}.json"
        s3_connector = aws_connection.S3Connector(bucket_name=bucket_name)
        client = s3_connector.client
        bucket = s3_connector.bucket_name
    except Exception:
        logger.error(f"Error when creating S3 connection:\n{traceback.format_exc()}")
        return {
            'status': 'Failed',
            'error': 'S3 connection failed',
            'file_info': file_record
        }

    try:
        buffer = io.BytesIO()
        buffer.write(
            json.dumps(
                json_data,
                indent=2,
                ensure_ascii=False
            ).encode('utf-8')
        )
        buffer.seek(0)

        upload_result = put_object(client, bucket, object_name, buffer)
        if upload_result.get("status") == "Failed":
            return {
                "status": "Failed",
                "error": upload_result.get("error"),
                "file_info": file_record
            }

        logger.info(f"JSON uploaded to {bucket}/{object_name}")
        return {
            'json_data': json_data,
            'file_info': file_record,
            'convert_file_info': {
                'dest_bucket': bucket,
                'dest_object_name': object_name
            },
            'status': 'Success'
        }

    except Exception:
        logger.error(f"Upload failed:\n{traceback.format_exc()}")
        return {
            'status': 'Failed',
            'error': 'Upload failed',
            'file_info': file_record
        }
