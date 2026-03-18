import boto3
import pandas as pd
import io
import os
import argparse
from dotenv import load_dotenv

# Load AWS credentials from .env
load_dotenv()

def download_state_data(state: str):
    state = state.upper()
    bucket = "state-metrics"
    key = f"{state}_DATA.csv"

    local_path = os.path.join("public", "states", state, f"{state}_DATA.csv")

    print(f"Attempting to download {key} from {bucket}...")

    s3 = boto3.client("s3")

    try:
        # Get file from S3
        obj = s3.get_object(Bucket=bucket, Key=key)

        # Read into pandas
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))

        # test if s3 file is empty
        if df.empty:
            print(f"{state} file is empty. Not replacing local file.")
            return

        # Make sure directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # replaces the existing file
        df.to_csv(local_path, index=False)

        print(f"Successfully updated {local_path}")

    except Exception as e:
        print(f"Error downloading {state} data: {e}")
        print("Local file was not replaced.")


def main():
    parser = argparse.ArgumentParser(description="Download state metrics data from S3")
    parser.add_argument("state", type=str, help="State abbreviation (sc, ga, al, ex.)")
    args = parser.parse_args()

    download_state_data(args.state)


if __name__ == "__main__":
    main()