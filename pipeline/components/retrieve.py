from pipeline.base import Component, DataWrapper
import boto3
import pandas as pd
import io
from datetime import datetime, timedelta, timezone
import argparse
import re
import os

"""
The following constants define the parameters for the partial download optimization. Instead of downloading
every large file in its entirety, the pipeline fetches only the first HEAD_BYTES (the header row) and the 
last TAIL_BYTES of the file, reassembling them into a valid CSV. This also defines a STALE_THRESHOLD, so we
ignore files that have not been updated with the most recent data.

TODO: implement dynamic buffer size logic
"""
HEAD_BYTES = 500
TAIL_BYTES = 2.5 * 1024 * 1024      # Tune this to change the amount of bytes being read per truncated file
THRESHOLD_BYTES = 4 * 1024 * 1024   # Tune this to change what file size to start truncating (e.g. max file size to download)
USE_OPTIMIZATION = True             # Set to False when testing using full downloads, set to True when wanting to truncate large files
STALE_THRESHOLD = 4                 # Ignore files not updated in the past X days

class DataRetrievalS3(Component):
    """
    Initializes a connection to AWS S3 via a boto3 client, retrieving all of a state's data. 

    Returns both a state's combined DataFrame with all providers and a per provider list of DataFrames
    so downstream components have access to both views of data. 
    """

    def __init__(self, name, state, date=None, bucket="urg-power-outage"):
        """
        Defines the state to process, the date we are filtering for (when defaulting to None, we compute
        yesterday's date at runtime), and the bucket to pull from. 
        """
        super().__init__(name)
        self.state = state
        self.bucket = bucket
        self.date = date

    def retrieve(self, days: int = 1) -> list[pd.DataFrame]:
        """
        Retrieve per-county outage data for a given state from S3 by:
            1) Initializing a connection with boto3
            2) Resolving the target date
            3) Resolving the download path
            4) Looping through each file in a state, following the download optimization outlined in L11-14
            5) Ensuring timestamp exists and standardizing its data type for filtering
        
        Once all files are downloaded, combine them into DataFrames for each provider.

        Args:
            days (int): How many past days of data to retrieve.
                        Default = 1 (yesterday only)

        Returns:
            list[pd.DataFrame]: List of dataframes, each corresponding to a provider
        """

        # Initialize S3 client
        s3 = boto3.client("s3")

        # Figure out the time window to pull from
        target_date = None
        if self.date:
            target_date = pd.to_datetime(self.date).date()
        else:
            target_date = (datetime.now(timezone.utc) - timedelta(days=days)).date()

        # List all objects in the given state's folder from S3
        prefix = f"{self.state}/"
        response = s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

        if "Contents" not in response:
            print(f"No files found for state: {self.state}")
            return []

        dfs = []
        bytes_available = 0
        bytes_downloaded = 0

        # Loop through each file in state folder
        for obj in response["Contents"]:
            key = obj["Key"]

            # Match any variation of "per_county" in the file name (case-insensitive)
            if key.lower().endswith(".csv") and re.search(r"per[_]?count(y|ies)?", key, re.IGNORECASE):
                meta = s3.head_object(Bucket=self.bucket, Key=key)
                f_size = meta["ContentLength"]
                last_modified = meta["LastModified"]

                bytes_available += f_size

                cutoff = datetime.now(timezone.utc) - timedelta(days=STALE_THRESHOLD)
                if last_modified < cutoff:
                    continue

                if USE_OPTIMIZATION and f_size > THRESHOLD_BYTES:
                    bytes_downloaded += int(HEAD_BYTES) + int(TAIL_BYTES)

                    head_resp = s3.get_object(Bucket=self.bucket, Key=key, Range=f"bytes=0-{HEAD_BYTES}")
                    header_line = head_resp["Body"].read().decode("utf-8", errors="replace").splitlines()[0]

                    tail_resp = s3.get_object(Bucket=self.bucket, Key=key, Range=f"bytes=-{TAIL_BYTES}")
                    tail_content = tail_resp["Body"].read().decode("utf-8", errors="replace")

                    lines = tail_content.splitlines()
                    safe_content = header_line + "\n" + "\n".join(lines[1:])

                    df = pd.read_csv(io.StringIO(safe_content), low_memory=False)
                else:
                    bytes_downloaded += f_size
                    s3_obj = s3.get_object(Bucket=self.bucket, Key=key)
                    df = pd.read_csv(io.BytesIO(s3_obj["Body"].read()), low_memory=False)

                # Ensure 'timestamp' column exists
                if "timestamp" not in df.columns:
                    continue

                # Convert timestamp to datetime
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

                # Keep rows starting at the target_date until now
                df = df[df["timestamp"].dt.date == target_date]

                if not df.empty:
                    df["source_file"] = key
                    dfs.append(df)

        if not dfs:
            return []

        combined = pd.concat(dfs, ignore_index=True)

        # Group by provider column -> return as list of DataFrames
        provider_col = "EMC"
        if "EMC" not in combined.columns:
            raise ValueError(f"Expected column '{provider_col}' not found in data")

        provider_dfs = [group for _, group in combined.groupby(provider_col)]
        
        mb = lambda b: round(b / (1024 * 1024), 2)
        print(f"[{self.state.upper()}] Files scanned: {bytes_available / (1024*1024):.1f} MB available")
        print(f"[{self.state.upper()}] Actually downloaded: {mb(bytes_downloaded)} MB")
        print(f"[{self.state.upper()}] Reduction: {100 - (bytes_downloaded / bytes_available * 100):.1f}%")

        return provider_dfs
    
    def generate_nan_report(self, provider_dfs: list[pd.DataFrame], output_file="nan_report.xlsx"):
        """
        Unused artifact from before. Likely will not implement or use but will keep around in case 
        it is needed in the future.
        """
        report_rows = []

        for df in provider_dfs:
            provider = df["EMC"].iloc[0] if "EMC" in df.columns else "Unknown"
            source = df["source_file"].iloc[0] if "source_file" in df.columns else "Unknown"
            total_rows = df.shape[0]  # total number of rows in this dataframe

            # Count NaNs per column
            nan_counts = df.isna().sum()
            for col, count in nan_counts.items():
                if count > 0:
                    report_rows.append({
                        "Provider": provider,
                        "Source File": source,
                        "Column": col,
                        "Total Entries": total_rows,
                        "NaN Count": count
                    })

        if not report_rows:
            print("No NaNs found in any provider data")
            return

        report_df = pd.DataFrame(report_rows)

        # Save to Excel
        try:
            with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
                report_df.to_excel(writer, index=False, sheet_name="NaN Report")
            print(f"NaN report saved to {output_file}")
        except ImportError:
            # Fallback to CSV if openpyxl not installed
            csv_file = output_file.replace(".xlsx", ".csv")
            report_df.to_csv(csv_file, index=False)
            print(f"'openpyxl' not installed. NaN report saved to {csv_file}")

    def retrieve_all(self):
        """
        Retrieve's all raw data from the s3 bucket without any optimizations or restrictions. 
        This is used to help backfill historical data for new states or states that we have 
        not yet created historical metrics for. 
        """
        s3 = boto3.client("s3")
        prefix = f"{self.state}/"
        response = s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

        if "Contents" not in response:
            print(f"No files found for state: {self.state}")
            return []

        dfs = []

        for obj in response["Contents"]:
            key = obj["Key"]

            if key.lower().endswith(".csv") and re.search(r"per[_]?count(y|ies)?", key, re.IGNORECASE):
                s3_obj = s3.get_object(Bucket=self.bucket, Key=key)
                df = pd.read_csv(io.BytesIO(s3_obj["Body"].read()), low_memory=False)
                
                if "timestamp" not in df.columns:
                    continue

                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", format="mixed")
                if not df.empty:
                    df["source_file"] = key
                    dfs.append(df)

        if not dfs:
            return []
        
        combined = pd.concat(dfs, ignore_index=True)
        return combined

    def run(self, data):
        """
        data is None here since it is the first component in the pipeline, no prior stage exists to pass 
        in a DataWrapper. 
        
        Returns a DataWrapper with:
            data[0]:    All providers concatendated into a single DataFrame
            data[1]:    A list of DataFrames, each representing a unique provider
            metadata:   The state being processed, passed downstream for uploader 
        """

        s3_data_list = self.retrieve()
        combined = pd.concat(s3_data_list, ignore_index=True)
        
        # Add metadata so downstream components know the prefix to use
        metadata = {
            "s3_prefix": self.state.lower().strip()
        }

        per_provider_data = DataWrapper(data=[combined, s3_data_list], metadata=metadata)
        return per_provider_data