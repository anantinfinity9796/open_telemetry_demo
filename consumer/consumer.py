# consumer.py

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.error import KafkaError, KafkaException
import json
import pandas as pd
import os

class KafkaConsumer():
    """ Class for the kafka consumer which consumes messages by a topic name"""

    def __init__(self, topic_names):
        self._topic_names = topic_names
        self._min_commit_count = 5
        self._output_filename = "data_output.csv"

        conf = {'bootstrap.servers': 'localhost:9092',
                    'group.id': 'open-telemetry-consumer-group',
        'auto.offset.reset': 'earliest'}

        self._consumer = Consumer(conf)

    def write_to_file(self,message_list):
        print(message_list)
        try:
            df = pd.DataFrame.from_records(message_list)
            print(df)
            df.to_csv(f"{os.getcwd()}/data/{self._output_filename}",mode='a')
        except Exception as e:
            print(e)
        return

    def consume_messages_from_topic(self,):
        try:
            message_queue = []
            running = True
            self._consumer.subscribe(self._topic_names)
            message_count = 0
            while running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None: continue

                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Topic: {msg.topic()} | Partition: {msg.partition()} Reached End at Offset: {msg.offset()}")
                        break

                    elif msg.error():
                        raise KafkaException(msg.error())
                    
                else:
                    message_count +=1
                    message_queue.append(json.loads(msg.value()))
                    if message_count % self._min_commit_count == 0:
                        self.write_to_file(message_queue)
                        self._consumer.commit(asynchronous=False)
                        message_queue=[]
        finally:
            self._consumer.close()
        return