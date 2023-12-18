from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests

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
    print(json.dumps(data, indent=3))


def main():
    json_data = extrac_data()
    format_data(json_data)


if __name__ == '__main__':
    main()
