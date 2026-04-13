from pipeline.base import Component, DataWrapper
import boto3
import logging
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from io import StringIO
from datetime import date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Matches backfillHistorical.py monthly exports (no LOWER/UPPER SAIFI)
HISTORICAL_COLS = ["State", "County", "FIPS", "SAIDI", "SAIFI", "Date"]


class UploadHistorical(Component):
    """
    Merges the raw cleaner export into monthly historical CSVs on S3 under
    historical/{STATE}/{year}_{month}.csv, then passes through the incoming
    DataWrapper unchanged for WriteToS3.
    """

    def __init__(self, name: str, bucket_name: str):
        super().__init__(name)
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")

    def _s3_key(self, state_upper: str, d: date) -> str:
        return f"historical/{state_upper}/{d.year}_{d.month:02d}.csv"

    def _run_date(self, historical_new: pd.DataFrame) -> date:
        return pd.to_datetime(historical_new["Date"].iloc[0], errors="coerce").date()

    def _build_historical_df(self, raw_export: pd.DataFrame) -> pd.DataFrame:
        return raw_export[HISTORICAL_COLS].copy()

    def _download_existing(self, key: str) -> pd.DataFrame | None:
        try:
            resp = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            body = resp["Body"].read().decode("utf-8")
            return pd.read_csv(StringIO(body), low_memory=False)
        
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            
            if code in ("404", "NoSuchKey", "NotFound"):
                return None
                
            logger.error(f"Failed to download s3://{self.bucket_name}/{key}: {e}")
            raise

    def _strip_date_rows(self, df: pd.DataFrame, run_date: date) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=HISTORICAL_COLS)
        
        out = df.copy()
        out["_d"] = pd.to_datetime(out["Date"], errors="coerce").dt.date
        out = out[out["_d"] != run_date].drop(columns=["_d"])
        return out

    def _upload(self, df: pd.DataFrame, key: str) -> bool:
        try:
            buf = StringIO()
            out = df.sort_values(["Date", "County"])
            out.to_csv(buf, index=False)
            buf.seek(0)
            logger.info(f"Uploading historical metrics to s3://{self.bucket_name}/{key} ...")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buf.getvalue(),
                ContentType="text/csv",
            )
            
            logger.info("Historical upload successful")
            return True
        
        except NoCredentialsError:
            logger.error("AWS credentials not found.")
        
        except ClientError as e:
            logger.error(f"Failed to upload historical CSV: {e}")
        
        return False

    def run(self, data: DataWrapper) -> DataWrapper:
        raw_export = data.data[0]
        dashboard_export = data.data[1]

        historical_new = self._build_historical_df(raw_export)
        run_date = self._run_date(historical_new)

        state = data.metadata.get("s3_prefix", "").upper().strip()
        key = self._s3_key(state, run_date)

        existing = self._download_existing(key)
        if existing is None:
            merged = historical_new
        
        else:
            existing = self._strip_date_rows(existing, run_date)
            merged = pd.concat([existing, historical_new], ignore_index=True)
            merged = merged[HISTORICAL_COLS]

        self._upload(merged, key)

        return DataWrapper(
            data=[raw_export, dashboard_export],
            metadata=dict(data.metadata or {}),
        )
