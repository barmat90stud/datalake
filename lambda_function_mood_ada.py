import pandas as pd 
import psycopg2
import sqlalchemy
import os
from datetime import datetime

def lambda_handler(event, context):
    
    now = datetime.now()
    
    
    endpoint_dl = os.environ["Endpoint_DL"]
    name_dl = os.environ["Name_DL"]
    user_dl = os.environ["User_DL"]
    password_dl = os.environ["Password_DL"]

    endpoint_dw = os.environ["Endpoint_DW"]
    name_dw = os.environ["Name_DW"]
    user_dw = os.environ["User_DW"]
    password_dw = os.environ["Password_DW"]
    
    conn_dl=psycopg2.connect("host={} dbname={} user={} password={}".format(endpoint_dl, name_dl, user_dl, password_dl))
    cur_dl=conn_dl.cursor()
    conn_dl.set_session(autocommit=True)
    
    #Load last 60 entrys from table (t)
    
    cur_dl.execute("SELECT * FROM twitter_ada")
    df_ada = pd.DataFrame(cur_dl.fetchall(), columns=["Text", "Likes", "Retweets", "Date", "Time"])
    df_ada.sort_values(["Date", "Time"], inplace=True)
    df_ada_last_60 = df_ada.tail(60)
    
    #Load entries 60 to 120 (t-1)
    df_ada_60_to_120 = df_ada.tail(120)
    df_ada_60_to_120 = df_ada_60_to_120.head(60)
    
    # Calculate Twitter_Mood
    retweets_t = df_ada_last_60["Retweets"]
    retweets_t_minus_1 = df_ada_60_to_120["Retweets"]
    likes_t = df_ada_last_60["Likes"]
    likes_t_minus_1 = df_ada_60_to_120["Likes"]

    sum_retweet = sum(retweets_t) / (sum(retweets_t_minus_1)+1)
    
    sum_likes = sum(likes_t) / (sum(likes_t_minus_1)+1)

    sum_twitter = sum_retweet + sum_likes
    
    df_ada_last_60.loc[:,'Date'] = pd.to_datetime(df_ada_last_60.Date.astype(str)+' '+df_ada_last_60.Time.astype(str))
    date_last_60 = list(df_ada_last_60['Date'])

    df_ada_60_to_120.loc[:,'Date'] = pd.to_datetime(df_ada_60_to_120.Date.astype(str)+' '+df_ada_60_to_120.Time.astype(str))
    date_60_to_120 = list(df_ada_60_to_120['Date'])
    
    difference_last_60 = date_last_60[0] - date_last_60[-1]
    dif_sec_last_60 = difference_last_60.total_seconds()

    difference_60_to_120 = date_60_to_120[0] - date_60_to_120[-1]
    dif_sec_60_to_120 = difference_60_to_120.total_seconds()
    
    dif_time_twitter = dif_sec_last_60 / dif_sec_60_to_120
    
    mood_twitter_ada = sum_twitter / dif_time_twitter
    
    # Calculate Youtube_mood
    
    cur_dl.execute("SELECT * FROM youtube_cardano")
    df_y_ada = pd.DataFrame(cur_dl.fetchall(), columns=["Title", "Published", "View_Count", "Like_Count", "Dislike_Count", "Comment_Count"])
    df_y_ada.sort_values("Published", inplace=True)
    df_y_ada_last_50 = df_y_ada.tail(50)
    
    df_y_ada_50_to_100 = df_y_ada.tail(100)
    df_y_ada_50_to_100 = df_y_ada_50_to_100.head(50)
    
    view_y_ada_t = df_y_ada_last_50["View_Count"].astype(int)
    view_y_ada_t_minus_1 = df_y_ada_50_to_100["View_Count"].astype(int)
    
    likes_y_ada_t = df_y_ada_last_50["Like_Count"].astype(int)
    likes_y_ada_t_minus_1 = df_y_ada_50_to_100["Like_Count"].astype(int)
    
    dislikes_y_ada_t = df_y_ada_last_50["Dislike_Count"].astype(int)
    dislikes_y_ada_t_minus_1 = df_y_ada_50_to_100["Dislike_Count"].astype(int)
    
    sum_view_y_ada = sum(view_y_ada_t) / (sum(view_y_ada_t_minus_1)+1)
    sum_likes_y_ada = sum(likes_y_ada_t) / (sum(likes_y_ada_t_minus_1)+1)
    sum_dislikes_y_ada = sum(dislikes_y_ada_t) / (sum(dislikes_y_ada_t_minus_1)+1)
    sum_youtube_ada = sum_view_y_ada + sum_likes_y_ada - sum_dislikes_y_ada
    
    date_y_ada_last_50 = list(pd.to_datetime(df_y_ada_last_50["Published"]))
    date_y_ada_50_to_100 = list(pd.to_datetime(df_y_ada_50_to_100["Published"]))
    
    difference_y_ada_last_50 = date_y_ada_last_50[-1] - date_y_ada_last_50[0]
    dif_sec_y_ada_last_50 = difference_y_ada_last_50.total_seconds()
    
    difference_y_ada_50_to_100 = date_y_ada_50_to_100[0] - date_y_ada_50_to_100[-1]
    dif_sec_y_ada_50_to_100 = difference_y_ada_50_to_100.total_seconds()
    
    dif_time_youtube_ada = dif_sec_y_ada_last_50 / dif_sec_y_ada_50_to_100
    
    mood_youtube_ada = sum_youtube_ada / dif_time_youtube_ada
    
    mood_coefficiant = (mood_twitter_ada*0.5 + mood_youtube_ada*0.5)
   
    #Create Dictionarry with Timestamp and Difference in Seconds
    
    dict_dw = {1: [now, mood_coefficiant]}
    df_load = pd.DataFrame.from_dict(dict_dw, orient='index')
   
    engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:5432/{}".format(user_dw, password_dw, endpoint_dw, name_dw))
   
    df_load.to_sql('mood_ada', engine, if_exists = 'append', index = False)
   
    print("Successfully loaded into Data Warehouse")
   
    cur_dl.close()
    conn_dl.close()