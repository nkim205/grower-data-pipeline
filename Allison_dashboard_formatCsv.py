import os
import pandas as pd
from datetime import datetime
import argparse

# Import the pipeline execution functions
from main import execute_pipeline
from pipeline.components.retrieve import DataRetrievalS3
from pipeline.components.process import Processing
from pipeline.components.metrics import Metrics

def run_export():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run SC pipeline and export dashboard CSV")
    parser.add_argument("state", type=str, help="Two-letter state code, e.g., sc")
    parser.add_argument("--date", type=str, help="Date to process (YYYY-MM-DD)")
    args = parser.parse_args()

    state = args.state.lower()
    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else datetime.now().date()

    retrieve = DataRetrievalS3(name="Retrieve Outage Data from S3", state=state, date=target_date)
    process = Processing(name="Standardize Outage Data", state=state, date=target_date)
    metrics = Metrics(name="Calculate SAIDI and SAIFI", state=state)

    components = [retrieve, process, metrics]

    # Execute pipeline
    result = execute_pipeline(components)

    df_pipeline = result.data[1]

    # Load FIPS CSV
    fips_file = "cleaned_FIPS.csv"  
    df_fips = pd.read_csv(fips_file, dtype=str)

    # Keep only rows for the target state
    df_fips = df_fips[df_fips["state_abbr"].str.lower() == state]

    # Clean FIPS CSV county names to match pipeline (lowercase, no "county")
    df_fips["county_name_clean"] = df_fips["county_name"].str.strip().str.lower()

    # Clean pipeline county names
    df_pipeline["county_clean"] = df_pipeline["county"].str.strip().str.lower()

    # Merge pipeline output with FIPS file
    merged = pd.merge(
        df_pipeline,
        df_fips,
        left_on="county_clean",
        right_on="county_name_clean",
        how="left"
    )


    # Build final csvs
    final_df = pd.DataFrame({
        "County": merged["county"].str.title(),
        "SAIDI": merged["saidi"],
        "SAIFI": merged["middle_saifi"],  # middle_saifi is SAIFI
        "FIPS": merged["fips"],
        "State": merged["state_name"].str.title()
    })

    # Output CSV
    output_file = f"{state.upper()}_DATA_{target_date}.csv"
    final_df.to_csv(output_file, index=False)
    print(f"Export complete: {output_file}")

if __name__ == "__main__":
    run_export()