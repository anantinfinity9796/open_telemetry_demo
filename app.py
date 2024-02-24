# app.py
from generator.generator import Generator
from consumer.consumer import KafkaConsumer
import time

def main():
    """Function to create and Process events"""
    # Initializa the Generator object
    # generator = Generator()

    # generator.generate_events('applicant-details.csv', 'test_topic')

    print("Sleeping for 10s......")
    # time.sleep()

    consumer = KafkaConsumer(topic_names=['test_topic'])

    consumer.consume_messages_from_topic()


if __name__=="__main__":
    main()