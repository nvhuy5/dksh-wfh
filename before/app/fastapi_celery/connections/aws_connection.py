# Standard Library Imports
import json

# Third-Party Imports
import traceback
import boto3
from botocore.exceptions import ClientError
import config_loader

aws_region = config_loader.get_env_variable("s3_buckets", "default_region")

# Logging Setup
import logging
from utils import log_helpers

logger_name = "AWS Connection"
log_helpers.logging_config(logger_name)
logger = logging.getLogger(logger_name)


# === S3 Connector using boto3 ===
class S3Connector:
    """
    AWS S3 Connector using boto3
    """

    def __init__(self, bucket_name: str, region_name: str = None):
        self.bucket_name = bucket_name.strip()
        self.region_name = (
            region_name or config_loader.get_env_variable("AWS_REGION", "ap-southeast-1")
        ).strip()

        # Initialize boto3 client with region and optional credentials
        self.client = boto3.client("s3", region_name=self.region_name)

        # Check if bucket exists or try to create it
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' already exists.")
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                logger.warning(
                    f"Bucket '{self.bucket_name}' does not exist. Creating..."
                )
                self._create_bucket()
            else:
                logger.error(
                    f"Error checking bucket '{self.bucket_name}': {type(e).__name__} - {e}"
                )
                raise

    def _create_bucket(self):
        try:
            if self.region_name == "us-east-1":
                # us-east-1 does not support LocationConstraint
                self.client.create_bucket(Bucket=self.bucket_name)
            else:
                self.client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region_name},
                )
            logger.info(f"Bucket '{self.bucket_name}' created successfully.")
        except ClientError as e:
            logger.error(
                f"Error creating bucket '{self.bucket_name}': {type(e).__name__} - {e}"
            )
            raise


# === Retrieve Secrets using boto3 ===
class AWSSecretsManager:
    """
    AWS Secrets Connector using boto3
    """

    def __init__(self, region_name: str = None):
        self.region_name = region_name or config_loader.get_env_variable("AWS_REGION", "ap-southeast-1")

        # Initialize boto3 client with region and optional credentials
        self.client = boto3.client("secretsmanager", region_name=self.region_name)

    def get_secret(self, secret_name: str) -> dict:
        try:
            response = self.client.get_secret_value(SecretId=secret_name)

            # Secret is either string or binary
            if "SecretString" in response:
                return json.loads(response["SecretString"])
            else:
                return json.loads(response["SecretBinary"].decode("utf-8"))

        except self.client.exceptions.ResourceNotFoundException:
            logger.error("Secret not found.")
        except Exception as e:
            logger.error(f"Error retrieving secret: {e} - {traceback.format_exc()}")
