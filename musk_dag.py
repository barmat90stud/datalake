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
    api = ti.xcom_pull(key="return_value", task_ids='Twitter_API')
    tweetList = api.user_timeline(screen_name='elonmusk', tweet_mode="extended", count=3)

    actualTextList = []
    actualLikesList = []
    actualRetweetList = []
    actualTimeList = []

    for x in tweetList:
        # remove any instances of @ and RT
        text = x.full_text.replace("@", "").replace("RT", "")
        likes = x.favorite_count
        retweets = x.retweet_count
        time = x.created_at
        # removes non alphanumeric characters such as emojis
        for elm in text:
            if not (elm.isalnum()) and elm != " ":
                text = text.replace(elm, "")
        actualTextList.append(text)
        actualLikesList.append(likes)
        actualRetweetList.append(retweets)
        actualTimeList.append(time)

    df_musk = pd.DataFrame(
        {'tweets': actualTextList, 'likes': actualLikesList, 'retweet': actualRetweetList, 'time': actualTimeList})

    df_musk['date_new'] = pd.to_datetime(df_musk['time']).dt.date
    df_musk['time_new'] = pd.to_datetime(df_musk['time']).dt.time
    df_musk.drop(['time'], axis=1, inplace=True)

    df = df_musk
    ti.xcom_push(key="df", value=df)

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
    df.to_sql('twitter_musk', engine, if_exists='append', index=False)
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
    dag_id = "musk_dag",
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


