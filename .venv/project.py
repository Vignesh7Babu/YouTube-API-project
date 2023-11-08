#Importing the necessary Packages
from googleapiclient.discovery import build
from pymongo import MongoClient
import urllib
import ssl
from pymongo.server_api import ServerApi
import sqlite3
import pandas as pd
import streamlit as st

#Build connection
def api_connection():
    api_key = "AIzaSyDZXmcjGq-Y_a8WC-D7I7a0r2Dv2IfLxD8"
    youtube = build('youtube', 'v3', developerKey=api_key)
    return youtube

youtube = api_connection()

#Getting Channels Informations
def get_channel_info(channel_id):
  response = youtube.channels().list(
    id=channel_id,
    part='snippet,statistics,contentDetails')

  channel_data = response.execute()
  channel_informations = {
    'channel_Id' : channel_data['items'][0]['id'],
    'channel_name' : channel_data['items'][0]['snippet']['title'],
    'channel_description' : channel_data['items'][0]['snippet']['description'],
    'subscriberCount' : channel_data['items'][0]['statistics']['subscriberCount'],
    'totalviewcount' : channel_data['items'][0]['statistics']['viewCount'],
    'videocount' : channel_data['items'][0]['statistics']['videoCount'],
    'playlist_id' : channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    }
  return channel_informations

#Getting Video_Id's
def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id,part='contentDetails').execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            playlistId=playlist_id,
            part='snippet',
            maxResults=50,
            pageToken=next_page_token
        )

        video_data = response1.execute()

        if 'items' in video_data:
            video_ids.extend([item['snippet']['resourceId']['videoId'] for item in video_data['items']])

        if 'nextPageToken' in video_data:
            # Set the next_page_token for the next iteration
            next_page_token = video_data['nextPageToken']
        else:
            # No more pages, break out of the loop
            break
    return video_ids

#Getting Videos Information
def get_video_info(video_id):
    video_details = []
    for video_ids in video_id:
        request = youtube.videos().list(
            part = "snippet,ContentDetails,statistics",
            id = video_ids)
        response2 = request.execute()
        try:
            for item in response2['items']:
                data = dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Title = item['snippet']['title'],
                        VideoId = item['id'],
                        Duration = item['contentDetails']['duration'],
                        View_count = item['statistics']['viewCount'],
                        Like_count = item['statistics']['likeCount'],
                        Comment_count = item['statistics']['commentCount'],
                        Published_at = item['snippet']['publishedAt'],
                        Video_resolution=item['contentDetails']['definition'])
                video_details.append(data)
        except Exception as e:
            print(f"Error processing video {video_ids}: {str(e)}")
    return video_details

#Get Comment Information
def get_commentinfo(videoids):
    video_comments = []
    try:
        for video_id in videoids:
            comments_response = youtube.commentThreads().list(
                    part='id,snippet',
                    videoId= video_id,
                    maxResults = 50)
            response3 = comments_response.execute()
            for comment in response3['items']:
                data1 = dict(Video_Id = comment['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Id = comment['id'],
                            Author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_like_count = comment['snippet']['topLevelComment']['snippet']['likeCount'],
                            Comment_publisheddate = comment['snippet']['topLevelComment']['snippet']['publishedAt'])
                video_comments.append(data1)
    except:
        pass
    return video_comments

#Connecting with MongoDB
username = "vigneshbabu37"
password = "Z@123@xcv"
# Encode the username and password
encoded_username = urllib.parse.quote_plus(username)
encoded_password = urllib.parse.quote_plus(password)
# Construct the URI with encoded credentials
uri = f"mongodb+srv://{encoded_username}:{encoded_password}@cluster0.e7whuud.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'), tz_aware=False, connect=True)
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(f"Connection failed: {e}")

#Create a Database and Collection in MongoDb
collection = client["YouTube_Data"]["YouTube_Channels"]

conn = sqlite3.connect("youtubedata.db")
cursor = conn.cursor()

def all_data(channel_id):
    Channel_Data = get_channel_info(channel_id)
    Video_IDs = get_video_ids(channel_id)
    Video_Data = get_video_info(Video_IDs)
    Comment_Data = get_commentinfo(Video_IDs)

    collection = client["YouTube_Data"]["YouTube_Channels"]
    collection.insert_one({"Channel_info":Channel_Data,"Video_info":Video_Data,"Comment_info":Comment_Data})

    return "Upload successfully"

#Creating a channel table

def channels_table():
    conn = sqlite3.connect("youtubedata.db")
    cursor = conn.cursor()

    #try:
    Create_table = '''CREATE TABLE IF NOT EXISTS Channels(
                    channel_Id TEXT PRIMARY KEY,
                    channel_name TEXT,
                    channel_description TEXT,
                    subscriberCount BIGINTEGER,
                    totalviewcount BIGINTEGER,
                    videocount INTEGER,
                    playlist_id TEXT)'''
    cursor.execute(Create_table)
    conn.commit()
    # except:
    #     st.write("Channel Table already created")
    
    #Retrieving the channel information from mongoDB
    channel_list = []
    collection = client["YouTube_Data"]["YouTube_Channels"]
    for ch_data in collection.find({},{"_id":0,"Channel_info":1}):
            channel_list.append(ch_data['Channel_info'])
    ch_df = pd.DataFrame(channel_list)

    #Inserting data's into the created table
    for index,row in ch_df.iterrows():
            insert_query = '''INSERT into Channels(channel_Id,
                                                        channel_name,
                                                        channel_description,
                                                        subscriberCount,
                                                        totalviewcount,
                                                        videocount,
                                                        playlist_id)
                                            VALUES(?,?,?,?,?,?,?)'''
                

            values =(
                    row['channel_Id'],
                    row['channel_name'],
                    row['channel_description'],
                    row['subscriberCount'],
                    row['totalviewcount'],
                    row['videocount'],
                    row['playlist_id'])
            #try:                     
            cursor.execute(insert_query,values)
            conn.commit()            
            # except:
            #     st.write("Channels values are already inserted")

#Creating Video Information table

def videos_table():
    conn = sqlite3.connect("youtubedata.db")
    cursor = conn.cursor()

    #try:
    Create_table1 = '''CREATE TABLE IF NOT EXISTS Videos(
                    Channel_Id TEXT,
                    Channel_Name TEXT,
                    Title TEXT,
                    VideoId TEXT PRIMARY KEY,
                    Duration TEXT,
                    View_count BIGINTEGER,
                    Like_count BIGINTEGER,
                    Comment_count INTEGER,
                    Published_at DATETIME,
                    Video_resolution TEXT)'''
    cursor.execute(Create_table1)
    conn.commit()
    #except:
        #st.write("Videos table already created")
    
    #Retrieving Video Information from mongoDB

    videos_list = []
    collection = client["YouTube_Data"]["YouTube_Channels"]
    for vi_data in collection.find({},{"_id":0,"Video_info":1}):
        for i in range(len(vi_data["Video_info"])):
                videos_list.append(vi_data["Video_info"][i])
        vi_df = pd.DataFrame(videos_list)
    
    #Inserting Video data's into the created table

    for index, row in vi_df.iterrows():
            insert_query = '''
                        INSERT INTO Videos(Channel_Id,
                            Channel_Name,
                            Title, 
                            VideoId,
                            Duration, 
                            View_count,
                            Like_count,
                            Comment_count, 
                            Published_at,
                            Video_resolution)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

                    '''
            values = (
                        row['Channel_Id'],
                        row['Channel_Name'],
                        row['Title'],
                        row['VideoId'],
                        row['Duration'],
                        row['View_count'],
                        row['Like_count'],
                        row['Comment_count'],
                        row['Published_at'],
                        row['Video_resolution'])
                                    
            #try:    
            cursor.execute(insert_query,values)
            conn.commit()
            #except:
                #st.write("videos values already inserted in the table")

#Creating Channel Information table

def comments_table():
    conn = sqlite3.connect("youtubedata.db")
    cursor = conn.cursor()
    #try:
    Create_table2 = '''CREATE TABLE if not exists Comments(
                                Video_Id TEXT,
                                Comment_Id TEXT PRIMARY KEY,
                                Author TEXT, 
                                Comment_text TEXT,
                                Comment_like_count INTEGER,
                                Comment_publisheddate DATETIME)'''
    cursor.execute(Create_table2)
    conn.commit()
    #except:
        #st.write("Comment table already created")

    #Retieving Comment information from mongoDB

    comment_list = []
    collection = client["YouTube_Data"]["YouTube_Channels"]
    for com_data in collection.find({},{"_id":0,"Comment_info":1}):
            for i in range(len(com_data["Comment_info"])):
                comment_list.append(com_data["Comment_info"][i])
            com_df = pd.DataFrame(comment_list)

    #Inserting the Comment data's into the created comment table

    for index,row in com_df.iterrows():
                insert_query = '''
                    INSERT INTO Comments (Video_Id,
                                        Comment_Id ,
                                        Author,
                                        Comment_text,
                                        Comment_like_count,
                                        Comment_publisheddate )
                    VALUES (?, ?, ?, ?, ?, ?)

                '''
                values = (
                    row['Video_Id'],
                    row['Comment_Id'],
                    row['Author'],
                    row['Comment_text'],
                    row['Comment_like_count'],
                    row['Comment_publisheddate']
                )
                #try:
                cursor.execute(insert_query,values)
                conn.commit()
                #except:
                    #st.write("This comments are already exist in comments table")

def tables():
    channels_table()
    videos_table()
    comments_table()
    # conn.commit()
    # conn.close()
    return "Tables Created successfully"

def show_channels_table():
    channel_list = []
    collection = client["YouTube_Data"]["YouTube_Channels"]
    for ch_data in collection.find({},{"_id":0,"Channel_info":1}):
            channel_list.append(ch_data['Channel_info'])
    channels_table = st.dataframe(channel_list)
    return channels_table

def show_videos_table():
    videos_list = []
    collection = client["YouTube_Data"]["YouTube_Channels"]
    for vi_data in collection.find({},{"_id":0,"Video_info":1}):
        for i in range(len(vi_data["Video_info"])):
                videos_list.append(vi_data["Video_info"][i])
    videos_table = st.dataframe(videos_list)
    return videos_table

def show_comments_table():
    comment_list = []
    collection = client["YouTube_Data"]["YouTube_Channels"]
    for com_data in collection.find({},{"_id":0,"Comment_info":1}):
        for i in range(len(com_data["Comment_info"])):
                comment_list.append(com_data["Comment_info"][i])
    comments_table = st.dataframe(comment_list)
    return comments_table

#Streamlit application building
if __name__=="__main__":
    st.title(":red[You]Tube")
    
    channel_id = st.text_input("Enter the Channel id")
    channels = channel_id.split(',')
    channels = [ch.strip() for ch in channels if ch]

    if st.button("Get Data"):
        for channel in channels:
            ch_ids = []
            collection = client["YouTube_Data"]["YouTube_Channels"]
            for ch_data in collection.find({},{"_id":0,"Channel_info":1}):
                ch_ids.append(ch_data['Channel_info']["channel_Id"])
            if channel in ch_ids:
                st.success("Channel details of the given channel id: " + channel + " already exists")
            else:
                output = all_data(channel)
                st.success(output)

    show_table = st.selectbox("Identify the table for the Preview",("Channels","Videos","Comments"))

    if show_table == "Channels":
        show_channels_table()
    elif show_table =="Videos":
        show_videos_table()
    elif show_table == "Comments":
        show_comments_table()


    if st.button("Port to SQL"):
        display = tables()
        st.success(display)
        

    question = st.selectbox(
        'Please Select Your Question',
        ('1.What are the names of all the videos and their corresponding channels?',
        '2.Which channels have the most number of videos, and how many videos do they have?',
        '3.What are the top 10 most viewed videos and their respective channels?',
        '4.How many comments were made on each video, and what are their corresponding video names?',
        '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7.What is the total number of views for each channel, and what are their corresponding channel names?',
        '8.What are the names of all the channels that have published videos in the year 2022?',
        '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10.Which videos have the highest number of comments, and what are their corresponding channel names?'))

    #SQL connection
    conn = sqlite3.connect("youtubedata.db")
    cursor = conn.cursor()

    #Data frames for all the questions

    if question == '1.What are the names of all the videos and their corresponding channels?':
        query1 = "select Title, channel_name from Videos;"
        cursor.execute(query1)
        conn.commit()
        t1=cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

    elif question == '2.Which channels have the most number of videos, and how many videos do they have?':
        query2 = "select channel_name,videocount from Channels order by videocount desc;"
        cursor.execute(query2)
        conn.commit()
        t2=cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name","Video Count"]))

    elif question == '3.What are the top 10 most viewed videos and their respective channels?':
        query3 = '''select view_count, channel_name, Title from Videos 
                            where view_count is not null order by view_count desc limit 10;'''
        cursor.execute(query3)
        conn.commit()
        t3 = cursor.fetchall()
        st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

    elif question == '4.How many comments were made on each video, and what are their corresponding video names?':
        query4 = "select comment_count,Title, channel_name from Videos where comment_count is not null;"
        cursor.execute(query4)
        conn.commit()
        t4=cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title","channel name"]))

    elif question == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        query5 = '''select Title, channel_name, like_count from Videos 
                        where like_count is not null order by like_count desc;'''
        cursor.execute(query5)
        conn.commit()
        t5 = cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

    elif question == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        query6 = '''select like_count,Title from Videos;'''
        cursor.execute(query6)
        conn.commit()
        t6 = cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["like count","video title"]))

    elif question == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
        query7 = "select channel_name, totalviewcount from Channels;"
        cursor.execute(query7)
        conn.commit()
        t7=cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

    elif question == '8.What are the names of all the channels that have published videos in the year 2022?':
        query8 = '''select Title, Published_at, channel_name from Videos 
                    where strftime('%Y' ,Published_at) = '2022';'''
        cursor.execute(query8)
        conn.commit()
        t8=cursor.fetchall()
        st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

    elif question == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        query9 = """SELECT channel_name,
                    AVG(
                    CAST(SUBSTR(duration, 3, INSTR(duration, 'M') - 3) AS INTEGER) * 60
                    + CAST(SUBSTR(duration, INSTR(duration, 'M') + 1, INSTR(duration, 'S') - INSTR(duration, 'M') - 1) AS INTEGER)
                    ) AS average_duration_seconds
                     FROM Videos
                    GROUP BY channel_name;"""
        cursor.execute(query9)
        results = cursor.fetchall()
        average_durations = []

        for channel_name, average_duration_seconds in results:
            average_minutes = average_duration_seconds // 60
            average_seconds = average_duration_seconds % 60
            average_duration = f'PT{average_minutes}M{average_seconds}S'
            average_durations.append({"Channel Name": channel_name, "Average Duration": average_duration})

        st.write(pd.DataFrame(average_durations))

    elif question == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        query10 = '''select Title, channel_name, comment_count from Videos 
                        where comment_count is not null order by comment_count desc;'''
        cursor.execute(query10)
        conn.commit()
        t10=cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))