# producer.py

from confluent_kafka import Producer
import socket

class KafkaProducer():
    """Producer class for producing events to a specific topic"""

    def __init__(self,topic_name):
        self._topic_name = topic_name
        self.conf =  {'bootstrap.servers': 'localhost:9092',
                                'client.id': socket.gethostname()}

        self._producer = Producer(self.conf)

    def message_callback(self,err, msg):
        if err is not None:
            print(f"Failed to produce message to topic {self._topic_name}: Message= {str(msg)} | Error= {str(err)}")
        else:
            # continue
            # print(f"message produced to topic {self._topic_name}: Message= {str(msg.partition())} | {str(msg.offset())} | {str(msg.value())}")
            print(f"SUCCESS : message produced to topic {self._topic_name}")
            
    def produce_events_to_topic(self,event):
        if event is not None:
            self._producer.produce(self._topic_name, value=event, callback=self.message_callback)
            self._producer.poll(1)
        else:
            raise ValueError("Message should Not be None")

        self._producer.flush()
        

