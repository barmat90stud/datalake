from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import tweepy
import pandas as pd
import psycopg2
import sqlalchemy
import Twitter_Credentials
import DB_Credentials
print("All Dag modules are ok")

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def Twitter_API():
    auth = tweepy.OAuthHandler(Twitter_Credentials.CONSUMER_KEY, Twitter_Credentials.CONSUMER_SECRET)
    auth.set_access_token(Twitter_Credentials.ACCESS_TOKEN, Twitter_Credentials.ACCESS_TOKEN_SECRET)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    print("API successfully build")
    return api

def Get_Twitter_Data(**kwargs):
    ti = kwargs['ti']
    api = ti.xcom_pull(key = "return_value", task_ids='Twitter_API')
    lst_time = []
    lst_text = []
    lst_like = []
    lst_retweet = []

    for i in tweepy.Cursor(api.search_tweets, q = 'Cardano -filter:retweets OR ADA -filter:retweets').items(150):
        lst_text.append(i.text)
        lst_like.append(i.favorite_count)
        lst_retweet.append(i.retweet_count)
        lst_time.append(i.created_at)

    df = pd.DataFrame(list(zip(lst_time, lst_text, lst_like, lst_retweet)), columns=['time', 'text', 'like', 'retweet'])

    df['date_new'] = pd.to_datetime(df['time']).dt.date
    df['time_new'] = pd.to_datetime(df['time']).dt.time
    df = df.drop(['time'], axis=1)
    ti.xcom_push(key = "df", value = df)

def DB_Connection(**kwargs):
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={}".format(DB_Credentials.Endpoint,
                                                           DB_Credentials.DBname, DB_Credentials.User,
                                                           DB_Credentials.Password))
    print("Connected successfully!")

    # try:
    #     cur = conn.cursor()
    # except psycopg2.Error as e:
    #     print("Error: Could not get curser to dthe Database")
    #     print(e)
    # print("Cursor built")

    conn.set_session(autocommit=True)
    print("Autocommit set to TRUE")

def load_data_to_DB(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(key = "df", task_ids = "Get_Twitter_Data")
    engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:5432/{}".format(DB_Credentials.User,
                                                                             DB_Credentials.Password,
                                                                             DB_Credentials.Endpoint,
                                                                             DB_Credentials.DBname))
    df.to_sql('twitter_ada', engine, if_exists='append', index=False)
    print('Data load to DB')

def close_connection_to_DB(**kwargs):
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={}".format(DB_Credentials.Endpoint,
                                                       DB_Credentials.DBname, DB_Credentials.User,
                                                       DB_Credentials.Password))

    conn.close()
    print("Connection closed")

    # cron-Expretion for every 2 Minutes: */2****
with DAG(
    dag_id = "ADA_dag",
    schedule_interval = "*/10 * * * *",
    default_args = default_args,
    catchup = False) as f:


    Twitter_API = PythonOperator(
        task_id = "Twitter_API",
        python_callable = Twitter_API
    )

    Get_Twitter_Data = PythonOperator(
        task_id="Get_Twitter_Data",
        python_callable=Get_Twitter_Data,
        provide_context=True
    )

    DB_Connection = PythonOperator(
        task_id = "DB_Connection",
        python_callable = DB_Connection,
        provide_context=True
    )

    load_data_to_DB = PythonOperator(
        task_id = "load_data_to_DB",
        python_callable = load_data_to_DB,
        provide_context=True
    )

    close_connection_to_DB = PythonOperator(
        task_id = "close_connection_to_DB",
        python_callable = close_connection_to_DB,
        provide_context=True
    )

    Twitter_API >> Get_Twitter_Data >> DB_Connection >> load_data_to_DB >> close_connection_to_DB


