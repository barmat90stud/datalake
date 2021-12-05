from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import time
from googleapiclient.discovery import build
from datetime import datetime
from cred import api_key
import psycopg2
import sqlalchemy
import DB_Credentials

print("All Dag modules are ok")


search_term = "Cardano"
api_key = api_key


def get_video_list():
    """With this function the Video ID of the 50 most recent published will be accessed"""
    youtube = build("youtube", "v3", developerKey=api_key)


    video_list = []
    request = youtube.search().list(q=search_term, part="snippet", type="video", maxResults=50,
                                    order="date")  ## order --> Resources are sorted in reverse chronological order based on the date they were created
    response = request.execute()

    time.sleep(5)

    for video in response["items"]:
        video_list.append(video["id"]["videoId"])
        video_title = video["snippet"]["title"]  ##video title
        upload_date = video["snippet"]["publishedAt"]

    return video_list


def get_video_details(**kwargs):
    """With this funciton additional details are received, based on the unique video ID"""
    ti = kwargs['ti']
    video_list = ti.xcom_pull(key="return_value", task_ids='get_video_list')
    youtube = build("youtube", "v3", developerKey=api_key)
    stats_list = []
    for i in range(0, len(video_list), 50):  ## range(start, end, steps)
        request = youtube.videos().list(
            part="snippet, contentDetails, statistics",
            id=video_list[i:i + 50])

        data = request.execute()

        for video in data["items"]:
            title = video["snippet"]["title"]
            published = video["snippet"]["publishedAt"]
            view_count = video["statistics"].get("viewCount", 0)
            like_count = video["statistics"].get("likeCount", 0)
            dislike_count = video["statistics"].get("dislikeCount", 0)
            comment_count = video["statistics"].get("commentCount", 0)

            stats_dictionary = dict(title=title,
                                    published=published,
                                    view_count=view_count,
                                    like_count=like_count,
                                    dislike_count=dislike_count,
                                    comment_count=comment_count)

            stats_list.append(stats_dictionary)

    df_youtube = pd.DataFrame(stats_list, columns=["title", "published", "view_count", "like_count", "dislike_count", "comment_count"])
    ti.xcom_push(key="df_youtube", value=df_youtube)


def DB_Connection(**kwargs):
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={}".format(DB_Credentials.Endpoint,
                                                           DB_Credentials.DBname, DB_Credentials.User,
                                                           DB_Credentials.Password))
    print("Connected successfully!")

    # try:
    #     cur = conn.cursor()
    # except psycopg2.Error as e:
    #     print("Error: Could not get curser to the Database")
    #     print(e)
    # print("Cursor built")

    conn.set_session(autocommit=True)
    print("Autocommit set to TRUE")

def load_data_to_DB(**kwargs):
    ti = kwargs['ti']
    df_youtube = ti.xcom_pull(key = "df_youtube", task_ids = "get_video_details")
    engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:5432/{}".format(DB_Credentials.User,
                                                                             DB_Credentials.Password,
                                                                             DB_Credentials.Endpoint,
                                                                             DB_Credentials.DBname))
    df_youtube.to_sql('youtube_cardano', engine, if_exists='append', index=False)
    print('Data load to DB')

def close_connection_to_DB(**kwargs):
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={}".format(DB_Credentials.Endpoint,
                                                       DB_Credentials.DBname, DB_Credentials.User,
                                                       DB_Credentials.Password))

    conn.close()
    print("Connection closed")


default_args = {
    #"owner": "airflow",
    #"depends_on_past": False,
    #"retries": 1, ## how many retries if it fails
    #"retry_delay": timedelta(minutes=5),
    "start_date": datetime(2021, 10, 29)
    }

with DAG(
    dag_id="youtube_cardano_dag",
    schedule_interval = "*/30 * * * *",
    default_args=default_args,
    description="First DAG with ELT process",
    catchup=False
    ) as f:


    get_video_list = PythonOperator(
        task_id = "get_video_list",
        python_callable=get_video_list
    )

    get_video_details = PythonOperator(
        task_id="get_video_details",
        python_callable=get_video_details,
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



    get_video_list >> get_video_details >> DB_Connection >> load_data_to_DB >> close_connection_to_DB
