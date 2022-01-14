import pandas as pd 
import psycopg2
import sqlalchemy
import os
from datetime import datetime

def lambda_handler(event, context):
    
    now = datetime.now()
    
    #Build Connection to DL
    
    DB_Endpoint_DL = os.environ["DB_Endpoint_DL"]
    DB_Name_DL = os.environ["DB_Name_DL"]
    DB_User_DL = os.environ["DB_User_DL"]
    DB_Password_DL = os.environ["DB_Password_DL"]
    
    conn_dl=psycopg2.connect("host={} dbname={} user={} password={}".format(DB_Endpoint_DL , DB_Name_DL, DB_User_DL, DB_Password_DL))
    cur_dl=conn_dl.cursor()

    #Load last 10 Musk Tweets
    cur_dl.execute("SELECT * FROM twitter_musk LIMIT 10")
    list_musk = cur_dl.fetchall()
    
    musk_timeline = []
    trash = []

    
    #Scann Tweet Text if Keyword is in Text. Keyword: Bitcoin, BTC, Cardano, ADA, Crypto 
    for i in list_musk:
        if "Bitcoin" in i[0]: 
            musk_timeline.append(i)
        elif "BTC" in i[0]:
            musk_timeline.append(i)
        elif "Cardano" in i[0]:
            musk_timeline.append(i)
        elif "ADA" in i[0]:
            musk_timeline.append(i)
        elif "Crytpo" in i[0]:
            musk_timeline.append(i)
        else:
            trash.append(i)
    
    #Create Pandas DataFrame    
    df_m_timeline = pd.DataFrame(musk_timeline, columns=["Text", "Likes", "Retweets", "Date", "Time"])
    
    #Add Column with actual Timestamp
    df_m_timeline["Timestamp_upload"] = now 
    
    #Load DataFrame to DataWarehouse
    
    user_dw = os.environ["DB_User_DW"]
    password_dw = os.environ["DB_Password_DW"]
    endpoint_dw = os.environ["DB_Endpoint_DW"]
    name_dw = os.environ["DB_Name_DW"]
    
    engine = sqlalchemy.create_engine("postgresql://{}:{}@{}:5432/{}".format(user_dw, password_dw, endpoint_dw, name_dw))
    df_m_timeline.to_sql('twitter_musk_dw', engine, if_exists = 'append', index = False)
    print("Successfully loaded into Data Warehouse")
   
    #Close Connection to DL
    cur_dl.close()
    conn_dl.close()

