import datetime
import logging
import psycopg2
import sqlalchemy
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
import credentials as cr
import botocore

cr_s3 = cr.S3_credential()
cr_db = cr.postgres_credetential()

def start():
    logging.info('Starting the DAG')


def DB_Connection(**kwargs):
    conn = psycopg2.connect(
        host= cr_db.host,
        dbname= cr_db.dbname,
        user= cr_db.user,
        password= cr_db.password
    )
    print("Connected successfully!")
    # try:
    #     cur = conn.cursor()
    # except psycopg2.Error as e:
    #     print("Error: Could not get curser to dthe Database")
    #     print(e)
    # print("Cursor built")
    conn.set_session(autocommit=True)
    print("Autocommit set to TRUE")

def close_connection_to_DB(**kwargs):
    conn = psycopg2.connect(
        host= cr_db.host,
        dbname= cr_db.dbname,
        user= cr_db.user,
        password= cr_db.password
    )
    conn.close()
    print("Connection closed")

def get_most_recent_s3_object(bucket_name):
    s3 = boto3.client("s3",
                      aws_access_key_id=cr_s3.access_key_id,
                      aws_secret_access_key=cr_s3.secret_access_key,
                      aws_session_token=cr_s3.session_token)
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name)
    latest = None
    for page in page_iterator:
        if 'Contents' in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    s3_obj = s3.get_object(Bucket = bucket_name, Key = latest["Key"])
    return s3_obj
def get_latest_data_from_s3():
    obj = get_most_recent_s3_object(cr_s3.bucket_name)
    data_string = obj["Body"].read().decode('utf-8')
    data_dict = json.loads(data_string)
    print(data_dict)
    df_1 = pd.DataFrame(data_dict["data"]["ADA"]["quote"]).T
    time_ada = datetime.datetime.strptime(data_dict["data"]["ADA"]["last_updated"],"%Y-%m-%dT%H:%M:%S.%fZ")
    df_1["timestamp"] = time_ada
    df_2 = pd.DataFrame(data_dict["data"]["BTC"]["quote"]).T
    time_btc = datetime.datetime.strptime(data_dict["data"]["BTC"]["last_updated"],"%Y-%m-%dT%H:%M:%S.%fZ")
    df_2["timestamp"] = time_btc
    engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:5432/{}".format(
        cr_db.user,
        cr_db.password,
        cr_db.host,
        cr_db.dbname))

    df_1.to_sql('ada_quote', engine, if_exists='append', index=False)
    df_2.to_sql('btc_quote', engine, if_exists='append', index=False)
    print('data loaded to data warehouse')

dag = DAG(
    'S3_to_DB',
    start_date=datetime.datetime(2021, 11, 18),
    schedule_interval="*/10 * * * *",
    catchup=False
)

greet_task = PythonOperator(
    task_id="start_task",
    python_callable=start,
    dag=dag
)

db_connection = PythonOperator(
    task_id="DB_connection",
    python_callable=DB_Connection,
    dag=dag
)

save_toS3 = PythonOperator(
    task_id="load_fromS3",
    python_callable=get_latest_data_from_s3,
    dag=dag
)

close_db_connection = PythonOperator(
    task_id="close_DB_connection",
    python_callable=close_connection_to_DB,
    dag=dag
)

greet_task >> db_connection
db_connection >> save_toS3
save_toS3 >> close_db_connection
