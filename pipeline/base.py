import time
from datetime import datetime

class DataWrapper:
    """
    Wraps the output of a pipeline component, bundling the data payload 
    together with a metadata dictionary for tracking execution information.
    """

    def __init__(self, data, metadata=None):
        """
        Args:
            data:       The output from a pipeline component. Can be a DataFrame,
                        a list of DataFrams, or a dictionary depending on the stage.
            metadata:   Optional dictionary for tracking execution information 
                        (component name, start time, duration). Defaults to an 
                        empty dictionary. 
        """
        self.data = data
        self.metadata = metadata if metadata is not None else {}



class Component:
    """
    Base class for all pipeline components. Subclasses must implement run().
    
    Each component receives a DataWrapper from the previous stage, performs
    work, and returns a new DataWrapper. Call execute_component() rather 
    than run() directly.
    """

    def __init__(self, name):
        self.component_name = name

    def start_time(self) -> str:
        return datetime.now().strftime("%c")    # e.g. 'Tue Oct 21 18:09:32 2025`

    def time_elapsed(self, start: float, end: float) -> float:
        return end - start
    
    # Subclasses need to override this, all components will need to implement this method
    def run(self, data) -> DataWrapper:
        """
        Executes this component's logic. Must be overriden by all components.

        Args:
            data: A DataWrapper containing the output from the previous pipeline 
            stage. Defaults to None for the first component.

        Returns:
            DataWrapper: The processed output to pass onto the next stage.
        """
        raise NotImplementedError
    


    def execute_component(self, data) -> DataWrapper:
        """
        Wraps run() with timing and metadata tracking. main.py calls this on 
        each component. Do not call run() directly.

        Tracks the following to the result's metadata dict:
            - component_name: the name passed into __init__
            - start_time: human readable timestamp when execution began
            - duration: formatted elapsed time string (e.g. '0h 00m 1.3s')
        """

        component_start_time = self.start_time()
        t0 = time.perf_counter()

        # Call the subclasses run implementation, result type will be of DataWrapper
        component_result = self.run(data)

        # Stop the timer
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
        
        return component_result