from pipeline.base import Component, DataWrapper
import boto3
import os
import logging
from botocore.exceptions import NoCredentialsError, ClientError
from io import StringIO
from datetime import date

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

    def upload_dataframe(self, df, s3_key: str) -> bool:
        """
        Converts a dataframe to CSV in-memory and uploads it to S3.

        Parameters
        ----------
        df : pd.DataFrame
            The dataframe to upload.
        s3_key : str
            The destination key/path in the S3 bucket.

        Returns
        -------
        bool
            True if upload succeeded, False otherwise.
        """
        try:
            # Convert DF → CSV in memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            logger.info(f"Uploading dataframe to s3://{self.bucket_name}/{s3_key} ...")

            # Upload without creating a temp file
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType="text/csv",
            )

            logger.info("Upload successful ✅")
            return True

        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure them properly.")
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")

        return False

    def run(self, data: DataWrapper, s3_prefix: str = ""):
        """
        Takes the dataframe from upstream (via DataWrapper),
        uploads it to S3, and returns a new DataWrapper indicating the result.
        """

        # Extract dataframe from DataWrapper
        historical_df = data.data[0]    # Used for aggregating historical metrics (WIP)
        export = data.data[1]           # Cleaned version for dashboard that uses rounded values

        # Get state code from metadata and normalize
        state = data.metadata.get("s3_prefix", "").upper().rstrip("/").strip()

        # Build filename
        filename = f"{state}_DATA.csv"

        # Build S3 key
        s3_key = filename

        # Upload the dataframe
        success = self.upload_dataframe(export, s3_key)

        if not success:
            logger.error(f"Failed to upload to s3://{self.bucket_name}/{s3_key}")

        # Prepare result for next pipeline step
        payload = {
            "uploaded": success,
            "s3_bucket": self.bucket_name,
            "s3_key": s3_key,
            "rows": export.shape[0],
            "columns": export.shape[1],
        }
        payload = DataWrapper(payload)

        return payload
