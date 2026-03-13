import argparse
import os
from pipeline.components.retrieve import DataRetrievalS3
from pipeline.components.process import Processing
from pipeline.components.metrics import Metrics
from pipeline.components.cleaner import Cleaner
from pipeline.components.uploader import WriteToS3
from pipeline.base import DataWrapper
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

def retrieve_state_arg() -> str:
    parser = argparse.ArgumentParser(description="Retrieve per-county outage data from S3")
    parser.add_argument("state", type=str, help="Two-letter state code (e.g., al, ga)")
    parser.add_argument("--dry-run", action="store_true", help="Run pipeline without uploading to s3")
    parser.add_argument("--date", type=str, help="Specific date to process (YYYY-MM-DD)")
    parser.add_argument("--full-test", action="store_true", help="Include uploading to S3 using test bucket (state-metrics-dev)")
    args = parser.parse_args()
    return args

def execute_pipeline(components) -> DataWrapper:
    data = None
    component_result = None
    for c in components:
        # execute_component was implemented in the base_component class, and it calls run for each component and updates corresponding metadata
        component_result = c.execute_component(data)
        # print(component_result.metadata) # print metadata for each component to get relavant metrics (duration, start time)
        data = component_result # update data attribute so that the next component can us
        
    # we are returning DataWrapper Object here
    return component_result 


def pipeline() -> DataWrapper:
    args = retrieve_state_arg()
    state = args.state
    dry_run = args.dry_run
    target_date = None

    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = (datetime.now() - timedelta(days=1)).date()

    # Initialize different components
    retrieve = DataRetrievalS3(name="Retrieve Outage Data from S3", state=state, date=target_date) # for retrieve we would need to pass in state parameter
    process = Processing(name="Standardize Outage Data", state=state, date=target_date)
    metrics = Metrics(name="Calculate SAIDI and SAIFI", state=state, date=target_date)
    cleaner = Cleaner(name="Add FIPs codes to each county and format data for final output", state=state, date=target_date)
    

    components = [retrieve, process, metrics, cleaner]

    if not dry_run:
        branch = os.environ.get("BRANCH_NAME", "dev")
        bucket_name = "state-metrics" if branch == "main" else "state-metrics-dev"
        upload = WriteToS3(name='Upload Processed Data to S3', bucket_name=bucket_name)
        components.append(upload)

    # call execute_pipeline, which will call the execute method for each component
    result = execute_pipeline(components) # the final_result will be DataWrapper object containing data and metadata
    return result
    

if __name__ == "__main__":
    result = pipeline() # the final_result will be DataWrapper object
    args = retrieve_state_arg()

    target_date = None
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        target_date = (datetime.now() - timedelta(days=1)).date()

    if args.dry_run:       
        formatted_df = result.data[1]
        # print(formatted_df)
        formatted_df.to_csv(os.path.join("testing", f"{args.state}_baseline.csv"), index=False)
        print(f"Dry run complete for {args.state}, {target_date}")

    if args.full_test:
        print(f"✅ Full test run complete for {args.state}, {target_date}. Metrics have been uploaded to state-metrics-dev. ✅")
