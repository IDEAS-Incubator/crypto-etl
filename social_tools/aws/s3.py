import json
import os
import requests
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

class AwsS3Manager:
    def __init__(self):
        """Initialize the S3 manager with environment variables."""
        self.region = self._get_env_variable("REGION")
        self.bucket_name = self._get_env_variable("BUCKET_NAME")
        self.s3_client = boto3.client(
            "s3",
            region_name=self.region,
            aws_access_key_id=self._get_env_variable("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=self._get_env_variable("AWS_SECRET_ACCESS_KEY"),
        )
        self._create_bucket_if_not_exists()

    def _get_env_variable(self, name: str) -> str:
        """Retrieve environment variable or raise an error if missing."""
        result = os.getenv(name)
        if not result:
            raise EnvironmentError(f"Please set the {name} environment variable.")
        return result

    def _create_bucket_if_not_exists(self):
        """Create the S3 bucket if it doesn't already exist."""
        try:
            if self.region is None:
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                location = {"LocationConstraint": self.region}
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name, CreateBucketConfiguration=location
                )

            # Set public access block configuration
            self.s3_client.put_public_access_block(
                Bucket=self.bucket_name,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": False,
                    "RestrictPublicBuckets": True,
                },
            )

            # Set public read access policy
            bucket_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "PublicReadGetObject",
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": "s3:GetObject",
                        "Resource": f"arn:aws:s3:::{self.bucket_name}/*",
                    }
                ],
            }
            self.s3_client.put_bucket_policy(
                Bucket=self.bucket_name, Policy=json.dumps(bucket_policy)
            )
            logger.info(f"Bucket {self.bucket_name} created successfully.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
                logger.info(f"Bucket {self.bucket_name} already exists.")
            else:
                logger.error(f"Error creating bucket: {e}")
                raise

    def folder_exists(self, folder_name: str) -> bool:
        """Check if a folder exists in the bucket."""
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name, Prefix=f"{folder_name}/"
        )
        return "Contents" in response

    def create_subfolder(self, folder_name: str):
        """Create a folder in the S3 bucket."""
        if not self.folder_exists(folder_name):
            self.s3_client.put_object(Bucket=self.bucket_name, Key=f"{folder_name}/")
            logger.info(f"Subfolder {folder_name}/ created in bucket {self.bucket_name}.")
        else:
            logger.info(f"Subfolder {folder_name}/ already exists.")


    def upload_file(self, file_path: str, file_name: str, key: str):
        """Upload a file to a specific folder in the S3 bucket."""
        try:
            logger.info(f"Uploading {file_name} from {file_path} to S3 bucket {self.bucket_name} at {key}.")
            self.s3_client.upload_file(
                Filename=file_path,
                Bucket=self.bucket_name,
                Key=key,
            )
            logger.info(f"Uploaded {file_name} successfully to S3 bucket {self.bucket_name} at {key}.")
        except ClientError as e:
            logger.error(f"Failed to upload {file_name} to S3: {e}")
            raise
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Please check the path.")
            raise

    def list_files(self, prefix: str = "") -> list:
        """List files in the bucket with an optional prefix."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            files = [obj["Key"] for obj in response.get("Contents", [])]
            logger.info(f"Files in bucket {self.bucket_name} with prefix '{prefix}': {files}")
            return files
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            raise

    def delete_file(self, file_name: str):
        """Delete a file from the S3 bucket."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=file_name)
            logger.info(f"Deleted file {file_name} from bucket {self.bucket_name}.")
        except ClientError as e:
            logger.error(f"Error deleting file: {e}")
            raise
        
    def download_file(self, file_name: str, download_path: str):
        """Download a file from the S3 bucket."""
        try:
            logger.info(f"Downloading {file_name} from bucket {self.bucket_name}.")
            self.s3_client.download_file(
                Bucket=self.bucket_name,
                Key=file_name,
                Filename=download_path,
            )
            logger.info(f"Downloaded {file_name} to {download_path}.")
        except ClientError as e:
            logger.error(f"Error downloading file from S3: {e}")
            raise
    
    def file_exists(self, file_name: str) -> bool:
        """Check if a file exists in the S3 bucket."""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=file_name)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Error checking file existence: {e}")
            raise
