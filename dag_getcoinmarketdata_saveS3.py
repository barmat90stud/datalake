import datetime
import logging
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
import credentials as cr

cr_s3 = cr.S3_credential()
cr_api = cr.marketcap_api_credential()
def start():
    logging.info('Starting the DAG')


def load_data_to_s3(*args, **kwargs):
    s3 = boto3.resource("s3",
                        aws_access_key_id= cr_s3.access_key_id,
                        aws_secret_access_key= cr_s3.secret_access_key,
                        aws_session_token= cr_s3.session_token
                        )
    data = get_coinmarketcap_data()
    json_object = json.dumps(data)
    object = s3.Object(cr_s3.bucket_name, data["status"]["timestamp"])
    object.put(Body=json_object)

def get_coinmarketcap_data():
    url = cr_api.url
    parameters = {
      'symbol':"BTC,ADA",
      'convert':'USD'
    }
    headers = {
      'Accepts': 'application/json',
      'X-CMC_PRO_API_KEY': cr_api.api_key,
    }

    session = Session()
    session.headers.update(headers)
    try:
      response = session.get(url, params=parameters)
      global data
      data = json.loads(response.text)
    except (ConnectionError, Timeout, TooManyRedirects) as e:
      print(e)
    return data

dag = DAG(
    'load_coin_data_to_S3_2',
    start_date=datetime.datetime(2021, 11, 18),
    schedule_interval="*/5 * * * *",
    catchup=False
)

greet_task = PythonOperator(
    task_id="start_task",
    python_callable=start,
    dag=dag
)


save_toS3 = PythonOperator(
    task_id="save_toS3",
    python_callable=load_data_to_s3,
    dag=dag
)

greet_task >> save_toS3