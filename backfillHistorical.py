"""
backfillHistorical.py is an offline script meant for backfilling historical metrics.

PURPOSE:
    Generates monthly historical metrics CSVs (SAIDI/SAIFI per county per day) for a given state and 
    uploads them to s3 under historical/[STATE]/[year]_[month].csv. This is intended to only be run
    locally once when adding a new state.

HOW TO RUN & ARGUMENTS:
    python backfillHistorical.py <state> [--start-date YYYY-MM-DD] [--dry-run] [--dev]
    
    state           Two letter lowercase state code (e.g. ga, nc)
    --start-date    Date to begin backfilling from. The default value (2025-08-01) is recommended, as 
                    it reflects the point after which coverage became more consistent. 
    --prod-run      Upload to production s3 bucket (state-metrics). Default set to false.
    --dev-run       Upload to state-metrics-dev instead of state-metrics. Default set to false.

    In general, you want to first do a --dry-run or --dev run to verify results, then do a production
    run to upload results to s3. 

NOTES:
    - Uses retrieve_all() from DataRetrievalS3, which bypasses all optimizations and stale thresholds 
      in order to get past metrics as well. All source files get downloaded in full.
    - Because we retrieve all and process many days, it is normal for the backfill to take several
      minutes to run per state.
    - Dates with no source data are skipped and logged. 
    - STOP_DAYS controls how recent the backfill goes. This value will depend on when the AWS scrapers
      finalize data for a given date.
"""
from pipeline.base import DataWrapper
from pipeline.components.retrieve import DataRetrievalS3
from pipeline.components.process import Processing
from pipeline.components.metrics import Metrics
from pipeline.components.cleaner import Cleaner

from dotenv import load_dotenv
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import argparse, os, boto3
import contextlib, io

load_dotenv()

STOP_DAYS = 2   # The number of days before today to stop parsing historical metrics for (i.e. today - STOP_DAYS)

def retrieve_args():
    """
    Parses and returns command line arguments. Run with --help to see all options
    """
    parser = argparse.ArgumentParser(description="Retrieve all state data to backfill historical metrics")
    parser.add_argument("state", type=str, default="ga", help="Two letter state code (e.g. al, ga)")
    parser.add_argument("--start-date", type=str, default='2025-08-01', help="The date to begin pulling historic metrics from")
    parser.add_argument("--prod-run", action="store_true", default=False, help="Upload historical backfill to production (state-metrics)")
    parser.add_argument("--dev-run", action="store_true", default=False, help="Upload results to dev bucket")
    args = parser.parse_args()

    return args


def retrieve_data(state, start, end):
    """
    Performs a single bulk retrieve of all source data for the given state from s3, then filters the 
    result to rows within the [start, end] date range.

    Args:
        state: Two letter state code
        start: Start of the date range (inclusive)
        end:   End of the date range (inclusive)

    Returns:
        pd.DataFrame: All rows for the state within the date range with parsed timestamps
    """
    retriever = DataRetrievalS3(name="", state=state)
    combined = retriever.retrieve_all()

    combined = combined[
        (combined["timestamp"].dt.date >= start) & 
        (combined["timestamp"].dt.date <= end)
    ]

    return combined

# Initialize command line arguments
args        = retrieve_args()
state       = args.state
start_date  = datetime.strptime(args.start_date, "%Y-%m-%d").date()
prod_run    = args.prod_run
dev_run     = args.dev_run

if prod_run:
    print(f"Beginning a production historical backfill run for {state}")
elif dev_run:
    print(f"Beginning a dev historical backfill run for {state}")
else:
    print(f"Beginning a dry historical backfill run for {state}")

end_date = (datetime.now() - timedelta(days=STOP_DAYS)).date()
combined = retrieve_data(state=state, start=start_date, end=end_date)  

monthly = {}    # { (year, month): [df, df, ...] }
skipped = []

# For each date in the date range, process that day's data and append to monthly
for cur in pd.date_range(start_date, end_date):
    day_slice = combined[combined["timestamp"].dt.date == cur.date()]

    if day_slice.empty:
        skipped.append(cur)
        continue

    provider_dfs = [grp for _, grp in day_slice.groupby("EMC")]
    
    # Match DataWrapper shape for what Processing.run() reads:
    #   data[0] = combined day slice used for SAIFI
    #   data[1] = list of per provider DFs for SAIDI
    wrapper = DataWrapper(
        data=[day_slice, provider_dfs],
        metadata={"s3_prefix": state.lower()}
    )

    try:
        # Create fresh instance to not have corrupt data
        process = Processing(name="", state=state, date=cur)
        metrics = Metrics(name="", state=state, date=cur)
        cleaner = Cleaner(name="", state=state, date=cur)

        with contextlib.redirect_stdout(io.StringIO()):
            res = cleaner.run(metrics.run(process.run(wrapper)))
        
        # Cleaner.get_final() contains unused LOWER and UPPER SAIFI columns we want to drop
        row = res.data[0][["State", "County", "FIPS", "SAIDI", "SAIFI", "Date"]]
        key = (cur.year, cur.month)
        monthly.setdefault(key, []).append(row)
        print(f"Added data for {state} on {cur}")
    
    except Exception as e:
        print(f"Error on {cur}: {e}")
        skipped.append(cur)

# For each year, month combination, create a new DataFrame. Aggregate all data within the same 
# year, month into a single DataFrame
for (year, month), frames in monthly.items():
    df = pd.concat(frames, ignore_index=True).sort_values(["Date", "County"])
    s3_key = f"historical/{state.upper()}/{year}_{month:02d}.csv"

    if prod_run or dev_run:
        buffer = StringIO()
        df.to_csv(buffer, index=False)

        bucket = "state-metrics-dev" if dev_run else "state-metrics"
        boto3.client("s3").put_object(
            Bucket=bucket, 
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="text/csv"
        )
    else:
        path = os.path.join("testing", "historical", state.upper(), f"{year}_{month:02d}.csv")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False)
        
if prod_run:
    print(f"✅ Successfully uploaded backfilled historical data for {state} to state-metrics")
elif dev_run:
    print(f"✅ Successfully uploaded backfilled historical data for {state} to state-metrics-dev")
else:
    print(f"✅ Dry run test to backfill for {state} complete")

if len(skipped) > 0:
    print(f"The following date(s) were skipped: {skipped}")