from pipeline.base import Component, DataWrapper

class Metrics(Component):
    def __init__(self, name):
        super.__init__(name)
        # initialze everything else

    def calculate_metric(self):
        # example helper function 
        return None
    
    def run(self, data):
        # this is the main function to be implemented, use your helper functions here 
        saidi = self.calculate_metric(data)
        # once you have data ready for the next step, now we wrap it using the DataWrapper Class
        saidi_per_county = DataWrapper(saidi)
        # we can return this data to the next component of the pipeline
        return saidi_per_county