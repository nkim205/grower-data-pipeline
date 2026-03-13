import time
import pandas as pd
from datetime import datetime

# this a wrapper class where we hold the ouput (pandas dataframe) for each component
# this allows us to store metadata about each df
class DataWrapper:
    def __init__(self, data, metadata=None):
        self.data = data
        self.metadata = metadata if metadata is not None else {}

# this is our superclass for each of our pipeline components
# pipeline components will inherit from this class
class Component:
    def __init__(self, name):
        self.component_name = name

    def start_time(self) -> str:
        # this is the format of time (Tue Oct 21 18:09:32 2025)
        return datetime.now().strftime("%c")

    def time_elapsed(self, start: float, end: float) -> float:
        return end - start
    
    # Subclasses need to override this, all components will need to implement this method
    def run(self, data) -> DataWrapper:
        raise NotImplementedError
    
    # Metadata we are collecting for every component
    # 1) when component execution begins
    # 2) the duration of execution
    # we can add more metadata to the dictionary as needed
    def execute_component(self, data) -> DataWrapper:
        # log start time and start stopwatch
        component_start_time = self.start_time()
        t0 = time.perf_counter()

        # call the subclasses run implementation, result type will be of DataWrapper
        component_result = self.run(data)

        # we've finished component execution, now stop the timer
        t1 = time.perf_counter()

        # calculate how component execution took and convert into readable format
        seconds = self.time_elapsed(t0, t1)
        m, s = divmod(seconds, 60)
        h, m = divmod(int(m), 60)
        duration = f"{h}h {m:02d}m {s:04.1f}s"

        component_result.metadata.update(
            {"component_name" : self.component_name,
             "start_time": component_start_time,
             "duration" : duration,
            })
        
        # we return the result so the next pipeline component can use the previous result
        return component_result




    


