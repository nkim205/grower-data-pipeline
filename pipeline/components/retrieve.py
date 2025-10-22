from pipeline.base import Component, DataWrapper

class DataRetrievalS3(Component):
    def __init__(self, name, state):
        super.__init__(name)
        self.state = state

    def retrieve(self):
        # example helper function 
        return None
    
    def run(self, data):
        # this is the main function to be implemented, use your helper functions here 
        s3_data = self.retrieve()
        # once you have data ready for the next step, now we wrap it using the DataWrapper Class
        per_provider_data = DataWrapper(s3_data)
        # we can return this data to the next component of the pipeline
        return per_provider_data



