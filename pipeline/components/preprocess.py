from pipeline.base import Component, DataWrapper

class Preprocess(Component):
    def __init__(self, name):
        super.__init__(name)
        # initialze everything else

    def standardize(self, data):
        # example helper function 
        return None
    
    def run(self, data):
        # this is the main function to be implemented, use your helper functions here 
        normalized_data = self.standardize(data)
        # once you have data ready for the next step, now we wrap it using the DataWrapper Class
        per_county_data = DataWrapper(normalized_data)
        # we can return this data to the next component of the pipeline
        return per_county_data