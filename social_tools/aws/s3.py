import json
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

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
        self._initialize_bucket()

    def _get_env_variable(self, name: str) -> str:
        """Retrieve environment variable or raise an error if missing."""
        value = os.getenv(name)
        if not value:
            raise EnvironmentError(f"Missing environment variable: {name}")
        return value

    def _bucket_exists(self) -> bool:
        """Check if the bucket already exists."""
        try:
            response = self.s3_client.head_bucket(Bucket=self.bucket_name)
            return response["ResponseMetadata"]["HTTPStatusCode"] == 200
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Error checking bucket existence: {e}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _create_bucket(self):
        """Create the bucket with the appropriate settings."""
        try:
            if self.region == "us-east-1":
                self.s3_client.create_bucket(Bucket=self.bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )
            logger.info(f"Bucket {self.bucket_name} created successfully.")

            # Set public access block
            self.s3_client.put_public_access_block(
                Bucket=self.bucket_name,
                PublicAccessBlockConfiguration={
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": False,
                    "RestrictPublicBuckets": True,
                },
            )

            # Apply bucket policy
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
        except ClientError as e:
            logger.error(f"Error creating bucket: {e}")
            raise

    def _initialize_bucket(self):
        """Ensure the bucket exists, or create it if it doesn't."""
        if not self._bucket_exists():
            logger.info(f"Bucket {self.bucket_name} does not exist. Creating it.")
            self._create_bucket()
        else:
            logger.info(f"Bucket {self.bucket_name} already exists.")

    def upload_file(self, file_path: str, key: str) -> str:
        """Upload a file to the S3 bucket and return the S3 path."""
        try:
            logger.info(f"Uploading {file_path} to S3 bucket {self.bucket_name} at {key}.")
            self.s3_client.upload_file(file_path, self.bucket_name, key)
            logger.info(f"File uploaded successfully to {key}.")
            return f"s3://{self.bucket_name}/{key}"
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
            raise
        except ClientError as e:
            logger.error(f"Failed to upload file: {e}")
            raise

    def list_files(self, prefix: str = "") -> list:
        """List files in the bucket with an optional prefix."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except ClientError as e:
            logger.error(f"Error listing files: {e}")
            raise

    def delete_file(self, key: str):
        """Delete a file from the S3 bucket."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"Deleted {key} from bucket {self.bucket_name}.")
        except ClientError as e:
            logger.error(f"Error deleting file: {e}")
            raise

    def download_file(self, key: str, download_path: str):
        """Download a file from the S3 bucket."""
        try:
            logger.info(f"Downloading {key} to {download_path}.")
            self.s3_client.download_file(self.bucket_name, key, download_path)
            logger.info(f"File downloaded successfully to {download_path}.")
        except ClientError as e:
            logger.error(f"Error downloading file: {e}")
            raise

    def file_exists(self, key: str) -> bool:
        """Check if a file exists in the S3 bucket."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            logger.error(f"Error checking file existence: {e}")
            raise


if __name__ == "__main__":
    aws = AwsS3Manager()
    # Example usage:
    # aws.upload_file("local_file.txt", "folder/remote_file.txt")
