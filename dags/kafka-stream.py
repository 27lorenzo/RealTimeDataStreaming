from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
import time
import logging
import uuid

producer = None
kafka_topic = 'users_created'

default_args = {
    'owner': '27lorenzo',
    'start_date': datetime(2023, 12, 18, 10, 15)
}


def extrac_data():
    response = requests.get("https://randomuser.me/api/")
    json_res = response.json()['results'][0]
    #json_res = json.dumps(response, indent=3)
    return json_res


def format_data(json_data):
    data = {}
    data['id'] = str(uuid.uuid4())
    data['firstname'] = json_data['name']['first']
    data['lastname'] = json_data['name']['last']
    data['gender'] = json_data['gender']
    data['address'] = str(json_data['location']['street']['number']) + ' ' + \
        str(json_data['location']['street']['name']) + ', ' + \
        str(json_data['location']['city']) + ', ' + \
        str(json_data['location']['country'])
    data['postcode'] = json_data['location']['postcode']
    data['email'] = json_data['email']
    data['username'] = json_data['login']['username']
    data['dob'] = json_data['dob']['date']
    data['registered_date'] = json_data['registered']['date']
    data['phone'] = json_data['phone']
    data['picture'] = json_data['picture']
    return data
    # print(json.dumps(data, indent=3))


def create_producer():
    global producer
    # kafka_bootstrap_servers = 'localhost:9092'
    kafka_bootstrap_servers = 'broker:29092' or '172.18.0.2:29092'
    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers, max_block_ms=5000)


def send_to_kafka():
    current_time = time.time()
    create_producer()
    # Send requests to the API during the next 60 seconds from the start of the call
    while True:
        if time.time() > current_time + 60:
            break
        try:
            json_data = extrac_data()
            data = format_data(json_data)
            producer.send(kafka_topic, json.dumps(data).encode('utf-8'))
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            continue
    producer.close()


def main():
    send_to_kafka()


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=send_to_kafka
    )


if __name__ == '__main__':
    main()
