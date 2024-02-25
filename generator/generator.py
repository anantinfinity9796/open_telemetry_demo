# generator.py

import pandas as pd
import os
from producer.producer import KafkaProducer
import sys
import json

class Generator():
    """ Base class for generating events to Kafka Topics"""
    def __init__(self,):
        pass

    
    def process_data(self, filename):
        #  First we need to get the file_object and start converting the data on the csv to events.
        try:
            data = pd.read_csv(f"{os.getcwd()}/data/{filename}", iterator=True)
        except Exception as e:
            print(e)
            sys.exit(1)
        return data.get_chunk(size=10)
    
    def generate_events(self,filename, topic_name):

        data_iterator = self.process_data(filename=filename)

        try:
            kafka_producer = KafkaProducer(topic_name=topic_name)
        except Exception as e:
            print(e)
        
        for index,row in data_iterator.head().iterrows():
            event = row.to_json(orient='index')
            kafka_producer.produce_events_to_topic(event=event)
        return

