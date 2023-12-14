from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API key connection
def api_connect():
    Api_Id="AIzaSyBxsBaEO7hvN37Mu0APnGhv-E68IdXz0X8"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=api_connect()

#get channels information

def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
    )
    response=request.execute()
    for i in response["items"]:
        data=dict(Channel_Name=i["snippet"]["title"],
                    Channel_Id=i["id"],
                    Subscriber_Count=i["statistics"]["subscriberCount"],
                    total_videos=i["statistics"]["videoCount"],
                    Channel_Views=i["statistics"]["viewCount"],
                    Channel_Description=i["snippet"]["description"],
                    Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
                    )
            
    return data


#get video ids

def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(
        id=channel_id,
        part="contentDetails"
    )
    response=response.execute()
    Playlist_id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"] 
    next_page_token=None
    while True:
    
        response1=youtube.playlistItems().list(
            part='snippet',
            playlistId=Playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token=response1.get("nextPageToken")
        if next_page_token is None:
            break
    return video_ids

#get video_information

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        ).execute()
        for item in request["items"]:
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                      Channel_Id=item["snippet"]["channelId"],
                      Video_Id=item["id"],
                      Title=item["snippet"]["title"],
                      Tags=item["snippet"].get('tags'),
                      Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                      Video_Description=item["snippet"].get("description"),
                      Published_Date=item["snippet"]["publishedAt"],
                      Duration=item["contentDetails"]["duration"],
                      Views_Count=item["statistics"].get("viewCount"),
                      likes=item["statistics"].get("likeCount"),
                      Comments=item["statistics"].get('commentCount'),
                      Favourite_count=item['statistics']["favoriteCount"],
                      Definition=item["contentDetails"]["definition"],
                      Caption_Status=item["contentDetails"]["caption"]
                      )
            video_data.append(data)
    return video_data 


#get comment information

def get_comment_info(video_Ids):
    comment_data=[]
    try:

        for video_id in video_Ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()
            for item in response["items"]:
                data=dict(comment_Id=item["snippet"]["topLevelComment"]["id"],
                        Video_Id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        CommeNt_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comment_data.append(data)


              
    except:
        pass
    return comment_data 


#connection to mongodb

client=pymongo.MongoClient("mongodb+srv://preethi:preethi@cluster0.ladiyt1.mongodb.net/?retryWrites=true&w=majority")
db=client["Youtube"]

#combining all function to get youtube information and to upload on mongodb

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"video_infromation":vi_details,
                      "comment_information":com_details})
                      
    return "upload completed successfully"


#table creation for channel,video,comments in postgresmysql

#channel_table

def channel_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Preethi@1804",
                        database="youtube_data",
                        port="5432" )
    cursor=mydb.cursor() 

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscriber_Count bigint,
                                                        total_videos int,
                                                        Channel_Views bigint,
                                                        Channel_Description text,
                                                        Playlist_Id varchar(80))'''
        cursor.execute(create_query)   
        mydb.commit()
    except:
        print("channels table already created") 


    ch_list=[]
    db=client["Youtube"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)  

    for index,row in df.iterrows():
        insert_query='''insert into channels(Channel_Name ,
                                                Channel_Id,
                                                Subscriber_Count,
                                                total_videos,
                                                Channel_Views,
                                                Channel_Description,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscriber_Count'],
                row['total_videos'],
                row['Channel_Views'],
                row['Channel_Description'],
                row['Playlist_Id'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("channels values are already inserted")    
 

#video_table
def video_table():
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Preethi@1804",
                            database="youtube_data",
                            port="5432" )
    cursor=mydb.cursor() 

    drop_query='''drop table if exists video'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists video(
                        Channel_Name varchar(100),
                        Channel_Id varchar(100),
                        Video_Id varchar(30) primary key,
                        Title varchar(150),
                        Tags text,
                        Thumbnail varchar(200),
                        Video_Description text,
                        Published_Date timestamp,
                        Duration interval,
                        Views_Count bigint,
                        likes bigint,
                        Comments int,
                        Favourite_count int,
                        Definition varchar(10),
                        Caption_Status varchar(50))'''
        cursor.execute(create_query)   
        mydb.commit()
    except:
        print("video table already created") 


    vi_list=[]
    db=client["Youtube"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_infromation":1}):
        for i in range(len(vi_data["video_infromation"])):
            vi_list.append(vi_data["video_infromation"][i])
    df1=pd.DataFrame(vi_list)  



    for index,row in df1.iterrows():
        insert_query='''insert into video(Channel_Name,
                        Channel_Id,
                        Video_Id,
                        Title,
                        Tags,
                        Thumbnail,
                        Video_Description,
                        Published_Date,
                        Duration,
                        Views_Count,
                        likes,
                        Comments,
                        Favourite_count,
                        Definition,
                        Caption_Status)
                                                
                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Video_Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views_Count'],
                row['likes'],
                row['Comments'],
                row['Favourite_count'],
                row['Definition'],
                row['Caption_Status'],
                )

        
        cursor.execute(insert_query,values)
        mydb.commit()
    
#comments_table
def comments_table():
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Preethi@1804",
                            database="youtube_data",
                            port="5432" )
    cursor=mydb.cursor() 

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists comments(comment_Id varchar(50) primary key,
                            Video_Id varchar(50),
                            Comment_Text text,
                            CommeNt_Author varchar(50),
                            Comment_Published timestamp)'''
        cursor.execute(create_query)   
        mydb.commit()
    except:
        print("comments table already created") \

    cm_list=[]
    db=client["Youtube"]
    coll1=db["channel_details"]
    for cm_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(cm_data["comment_information"])):
            cm_list.append(cm_data["comment_information"][i])
    df2=pd.DataFrame(cm_list)         

    for index,row in df2.iterrows():
        insert_query='''insert into comments(comment_Id,
                            Video_Id,
                            Comment_Text,
                            CommeNt_Author,
                            Comment_Published)
                            values(%s,%s,%s,%s,%s)'''
        values=(row['comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['CommeNt_Author'],
                row['Comment_Published'] 
                )

        cursor.execute(insert_query,values)
        mydb.commit()

#combinig the table function
def tables():
    channel_table()
    video_table()
    comments_table()
    return "table created successfully"


def show_video_table():
    vi_list=[]
    db=client["Youtube"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_infromation":1}):
        for i in range(len(vi_data["video_infromation"])):
            vi_list.append(vi_data["video_infromation"][i])
    df1=st.dataframe(vi_list)
    return df1

def show_comment_table():
    cm_list=[]
    db=client["Youtube"]
    coll1=db["channel_details"]
    for cm_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(cm_data["comment_information"])):
            cm_list.append(cm_data["comment_information"][i])
    df2=st.dataframe(cm_list)
    return df2
    
def show_channel_table():
    ch_list=[]
    db=client["Youtube"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    return df  

#streamlit part
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("skill take Away")
    st.caption("MongoDB")
    st.caption("python scripting")
    st.caption("data collection")
    st.caption("data management using MongoDb and sql")
    st.caption("API INTEGRATION")

channel_id=st.text_input("ENTER CHANNEL ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["Youtube"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
    if channel_id in ch_ids:
        st.success("channel details of the given channel id already exits")
    else:
        insert=channel_details(channel_id)   
        st.success(insert)

        
if st.button("Migrate to SQL"):
    table=tables()     
    st.success(table)

#showing table in streamlit
show_table=st.radio("SELECT THE TABLE FOR VIEW",("channels","video","comment"))
if show_table =="channels" :
    show_channel_table()
elif show_table =="video" :
    show_video_table()
elif show_table =="comment" :
    show_comment_table()     


#sql connection
mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="Preethi@1804",
                      database="youtube_data",
                      port="5432")
cursor=mydb.cursor()

#executing query

question=st.selectbox("SELECT YOUR QUESTION",("1.What are the names of all the videos and their corresponding channels?",
                    "2.Which channels have the most number of videos, and how many videos do they have?",
                    "3.What are the top 10 most viewed videos and their respective channels?",
                    "4.How many comments were made on each video, and what are their corresponding video names?",
                    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                    "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                    "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                    "8.What are the names of all the channels that have published videos in the year 2022?",
                    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"
))
if question=="1.What are the names of all the videos and their corresponding channels?":

    query1='''select title as videos,channel_name as channelname from video'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)


elif question=="2.Which channels have the most number of videos, and how many videos do they have?":

    query2='''select channel_name as channelname,total_videos as no_videos from channels
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","no of videos"])
    st.write(df2)    

elif question=="3.What are the top 10 most viewed videos and their respective channels?":

    query3='''select views_count as views,channel_name as channelname,title as videotitle from video
                where views_count is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","video title"])
    st.write(df3)


elif question=="4.How many comments were made on each video, and what are their corresponding video names?":

    query4='''select comments as no_comments,title as videotitle from video where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no_comments","videotitle"])
    st.write(df4)    

elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":

    query5='''select title as videotitle,channel_name as channelname,likes as likescount
            from video where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["video title","channel name","likes"])

    st.write(df5)

elif question=="6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":

    query6='''select likes as likecount,title as videotitle from video'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likes","video title"])

    st.write(df6)  


elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":

    query7='''select channel_name as channelname,channel_views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","views"])

    st.write(df7)   

elif question=="8.What are the names of all the channels that have published videos in the year 2022?":

    query8='''select title as video_title,published_date as videorelease,channel_name as channelnaem from video
            where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video title","published date","channel_name"])
    
    st.write(df8)   

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":

    query9='''select channel_name as channelnaem,AVG(duration) as averageduration
            from video group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channel name","avg duration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channel name"]
        average_duration=row["avg duration"]
        averageduration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=averageduration_str))
    df11=pd.DataFrame(T9)    
    st.write(df11)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":

    query10='''select title as videotitle,comments as comments,channel_name as channelname
            from video where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","comments","channel_name"])

    st.write(df10)    
