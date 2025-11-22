import argparse
from pipeline.components.retrieve import DataRetrievalS3
from pipeline.components.process import Processing
from pipeline.components.metrics import Metrics
from pipeline.components.uploader import WriteToS3
from pipeline.base import DataWrapper

def retrieve_state_arg() -> str:
    parser = argparse.ArgumentParser(description="Retrieve per-county outage data from S3")
    parser.add_argument("state", type=str, help="Two-letter state code (e.g., al, ga)")
    args = parser.parse_args()
    return args.state

def execute_pipeline(components) -> DataWrapper:
    data = None
    component_result = None
    for c in components:
        # execute_component was implemented in the base_component class, and it calls run for each component and updates corresponding metadata
        component_result = c.execute_component(data)
        print(component_result.metadata) # print metadata for each component to get relavant metrics (duration, start time)
        data = component_result.data # update data attribute so that the next component can us
    
    # we are returning DataWrapper Object here
    return component_result 


def pipeline() -> DataWrapper:
    state = retrieve_state_arg()
    # Initialize different components
    retrieve = DataRetrievalS3(name="Retrieve Outage Data from S3", state=state) # for retrieve we would need to pass in state parameter
    process = Processing(name="Standardize Outage Data")
    metrics = Metrics(name="Calculate SAIDI and SAIFI")
    upload = WriteToS3(name='Upload Processed Data to S3', bucket_name='state-metrics')
    # create a list of all the components in order of pipeline steps
    components = [retrieve, process, metrics, upload]
    # call execute_pipeline, which will call the execute method for each component
    result = execute_pipeline(components) # the final_result will be DataWrapper object containing data and metadata
    return result
    

if __name__ == "__main__":
    result = pipeline() # the final_result will be DataWrapper object
    print(result.data) # here we are printing data (most likely a df) but we would need to append this data to some csv etc.
