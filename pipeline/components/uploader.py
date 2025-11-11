from pipeline.base import Component, DataWrapper
import boto3
import os
import logging
from botocore.exceptions import NoCredentialsError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WriteToS3(Component):
    def __init__(self, name, bucket_name: str):
        """
        Initializes the WriteToS3 component.

        Parameters
        ----------
        name : str
            The name of this component in the pipeline.
        bucket_name : str
            The name of the S3 bucket where files will be uploaded.
        """
        super().__init__(name)
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")

    def upload_file(self, file_path: str, s3_key: str) -> bool:
        """
        Uploads a file from the local filesystem to an S3 bucket.

        Parameters
        ----------
        file_path : str
            Local path to the file to upload.
        s3_key : str
            The destination key/path in the S3 bucket.

        Returns
        -------
        bool
            True if upload succeeded, False otherwise.
        """
        # Ensure file exists locally
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return False

        try:
            logger.info(f"Uploading {file_path} to s3://{self.bucket_name}/{s3_key} ...")
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            logger.info("Upload successful ✅")
            return True

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}")
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure them properly.")
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")

        return False

    def run(self, data: DataWrapper, local_file_path: str, s3_key: str):
        """
        Main entry point for this pipeline component.
        Uploads the processed metrics file to S3.

        Parameters
        ----------
        data : DataWrapper
            The data passed from the previous component (e.g., metrics output).
        local_file_path : str
            Path to the processed CSV file stored locally on the GitHub runner.
        s3_key : str
            The S3 destination key for the uploaded file.

        Returns
        -------
        DataWrapper
            Returns the same data wrapper object for downstream pipeline components.
        """
        success = self.upload_file(local_file_path, s3_key)

        if not success:
            logger.error(f"Upload failed for {local_file_path}")
        else:
            logger.info(f"Successfully uploaded {local_file_path} to {self.bucket_name}")

        # Continue the pipeline by returning the same wrapped data
        return data
