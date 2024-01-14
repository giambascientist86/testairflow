# Importing necessary libraries and module
import kafka
import pandas as pd
import numpy as np
import json
import time
# import hashlib
from kafka import KafkaProducer


# Constants and configuration

KAFKA_BOOTSTRAP_SERVERS = ['kafka1:19092', 'kafka2:19093','kafka3:19094']
KAFKA_TOPIC = "movies_rec"  
PAUSE_INTERVAL = 10  
STREAMING_DURATION = 120
# indicate the volume where the file is present into the the Docker Container in docker-airflow/dags
CSV_PATH = '/home/giambar/Git/docker-airflow/dags/movie_short.csv'

def retrieve_movie_data(path):

    """Fetches user data from the provided .csv movielens file"""
    df = pd.read_csv(path)
    df_json = pd.read_json("df_movies.json")
    df_movie = df_json.to_dict(orient="records")
    return df_movie

""""
def encrypt_user(user,df):  
    Hashes the userID using MD5 and returns its integer representation, thus preserving privacy and faking the PII
    user_str = str(df.user)
    return int(hashlib.md5(user_str.encode()).hexdigest(), 16)
"""
def configure_kafka(servers = KAFKA_BOOTSTRAP_SERVERS):
    """Creates and returns a Kafka producer instance."""
    settings = {
        'bootstrap.servers': ','.join(servers) 
    }
    return KafkaProducer(settings)

def publish_to_kafka(producer, topic, message_l):
    """Sends data to a Kafka topic."""
    # I have created an iteration loop that implements two APIs of the KafkaProducer Class:
    # send and flush; flush forces the sending even if we have no aknowledgment guaranteed, and I have implemented it
    # to have the delivery of messages to the partitions avoiding asyncornous delays;
    
    kafka_dict = dict()

    for message in message_l:
        print("Message to be send : ", message)
        kafka_dict['userId'] = message['userId']
        kafka_dict['movieId'] = message['movieId']
        kafka_dict['rating'] = message['rating']
        kafka_dict['timestamp'] = message['timestamp']
        # this time API is invoked to simulate the streaming source of a web App; we create the delay
        # and every 1/10th of second an input vector of information is sent to the Topic and partition of destination
        #time.sleep(0.1)
        producer.send(topic, value=json.dumps(kafka_dict), callback=delivery_check)
        time.sleep(0.1)
        producer.flush()

def delivery_check(err, msg):
    """Reports the delivery status of the message to Kafka and print an error message for not delivering it"""
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(), '[Partition: {}]'.format(msg.partition()))

def initiate_stream():
    """Initiates the process to stream user data to Kafka"""
    kafka_producer = configure_kafka()
    for _ in range(STREAMING_DURATION // PAUSE_INTERVAL):
        raw_data = retrieve_movie_data(CSV_PATH)
        publish_to_kafka(kafka_producer, KAFKA_TOPIC, raw_data)
        time.sleep(PAUSE_INTERVAL)

if __name__ == "__main__":
    initiate_stream()
