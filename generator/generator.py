# generator.py


class Generator():
    """ Base class for generating events to Kafka Topics"""
    def __init__(self,):
        pass

    def generate_events(self,filename):
        #  First we need to get the file_object and start converting the data on the csv to events.

