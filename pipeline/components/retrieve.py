from pipeline.base import Component, DataWrapper
import boto3
import pandas as pd
import io
from datetime import datetime, timedelta, timezone
import argparse
import re
import os

class DataRetrievalS3(Component):
    def __init__(self, name, state, bucket="urg-power-outage"):
        super.__init__(name)
        self.state = state
        self.bucket = bucket

    def retrieve(self) -> list[pd.DataFrame]:
        """
        Retrieve per-county outage data for a given state from S3.

        Args:
            state (str): Two-letter state code (e.g., 'al', 'ga')
            bucket (str): S3 bucket name

        Returns:
            list[pd.DataFrame]: List of dataframes, each corresponding to a provider
        """
        """
        Retrieve per-county outage data for a given state from S3.

        Returns:
            list[pd.DataFrame]: List of dataframes, each corresponding to a provider
        """
        # Initialize s3 client
        s3 = boto3.client("s3")

        # Yesterday’s date string (adjust format if needed)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        # List all objects in the given state's folder from S3
        prefix = f"{self.state}/"
        response = s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)

        if "Contents" not in response:
            print(f"No files found for state: {self.state}")
            return []

        dfs = []
        # Loop through each file in state folder
        for obj in response["Contents"]:
            key = obj["Key"]

            # Match any variation of "per_county" in the file name (case-insensitive)
            if key.lower().endswith(".csv") and re.search(r"per[_]?count(y|ies)?", key, re.IGNORECASE):
                s3_obj = s3.get_object(Bucket=self.bucket, Key=key)
                df = pd.read_csv(io.BytesIO(s3_obj["Body"].read()))

                # Ensure 'timestamp' column exists
                if "timestamp" not in df.columns:
                    continue

                # Convert timestamp to datetime and filter by yesterday's date
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                df = df[df["timestamp"].dt.strftime("%Y-%m-%d") == yesterday]

                if not df.empty:
                    df["source_file"] = key  # optional: track source file
                    dfs.append(df)

        if not dfs:
            return []

        # Combine all into one big DataFrame
        combined = pd.concat(dfs, ignore_index=True)

        # Group by provider column → return as list of DataFrames
        provider_col = "EMC"  # adjust if your column name is different
        if provider_col not in combined.columns:
            raise ValueError(f"Expected column '{provider_col}' not found in data")

        provider_dfs = [group for _, group in combined.groupby(provider_col)]
        combined.to_csv(f"{self.state}_outages.csv", index=False)

        return provider_dfs
    
    def generate_nan_report(provider_dfs: list[pd.DataFrame], output_file="nan_report.xlsx"):
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
    

    def run(self, data):
        # this is the main function to be implemented, use your helper functions here 
        s3_data = self.retrieve()

        # Generate the NaN report for the retrieved data
        self.generate_nan_report(s3_data, output_file=f"{self.state}_nan_report.xlsx")

        # once you have data ready for the next step, now we wrap it using the DataWrapper Class
        per_provider_data = DataWrapper(s3_data)

        # we can return this data to the next component of the pipeline
        return per_provider_data