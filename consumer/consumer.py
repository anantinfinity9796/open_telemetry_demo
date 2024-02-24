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
        self._min_commit_count = 2
        self._output_filename = "data_output.csv"

        conf = {'bootstrap.servers': 'localhost:9092',
                    'group.id': 'open-telemetry-consumer-group',
        'auto.offset.reset': 'earliest'}

        self._consumer = Consumer(conf)

    def reset_offset(consumer, partitions):
        
        for p in partitions:
            p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    def write_to_file(self,message_list):
        print("INSIDE THE MESSAGE LIST")
        try:
            df = pd.DataFrame.from_records(message_list)
            df.to_csv(f"{os.getcwd()}/data/{self._output_filename}",mode='a+')
        except Exception as e:
            print(e)
        return

    def consume_messages_from_topic(self,):
        try:
            message_queue = []
            running = True
            self._consumer.subscribe(self._topic_names, on_assign=self.reset_offset)
            message_count = 0
            while running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None: 
                    print("Waiting......")
                    print(msg)
                    continue

                

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Topic: {msg.topic()} | Partition: {msg.partition()} Reached End at Offset: {msg.offset()}")

                    elif msg.error():
                        raise KafkaException(msg.error())
                    else:
                        print("Inside message counter")
                        message_count +=1
                        message_queue.append(msg.value())
                        if message_count % self._min_commit_count == 0:
                            self.write_to_file(message_queue)
                            self._consumer.commit(asynchronous=False)
                            message_queue=[]
        # except Exception as e:
        #     print("Inside the Exception")
        #     print(msg)
        #     print(e)
        finally:
            self._consumer.close()
        return