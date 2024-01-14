from googleapiclient.discovery  import build
from pprint import pprint
import pymongo
import streamlit as st 
import mysql.connector
import pandas as pd 

def duration_to_time(Duration):
    numbers = []
    current_number = '' 

    for char in Duration:
     if char.isdigit():
         current_number += char
     else:
      if current_number:
          numbers.append(current_number)
          current_number = ''

    if 'H' not in Duration:
        numbers.insert(0,'00')
    if 'M' not in Duration:
        numbers.insert(1,'00')
    if 'S' not in Duration:
        numbers.insert(-1,'00')

    return ':'.join(numbers)
     
             
def api_connect():
    api_key = "AIzaSyBU-_YYGudPnDB8ifzhoAABAa0zvrKcpzU"
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=api_key)

    return youtube

youtube=api_connect()


def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response['items']:
        data=dict(channel_Name=i["snippet"]["title"],
                channel_id=i['id'],
                subscribers=i['statistics']['subscriberCount'],
                views=i['statistics']['viewCount'],
                Total_vidcount=i['statistics']['videoCount'],
                channel_description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data   



def get_videos_ids(channel_id):
    video_ids=[]



    response = youtube.channels().list(id=channel_id,   
                                    part='contentDetails').execute()
                                    
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None


    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=25,
                                            pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids   



def get_video_details(Video_ids):
    video_data=[]

    for video_id in Video_ids:
        request = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )

        response = request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item.get('tags'),
                    Description=item['snippet']['description'],
                    Published_date=item['snippet']['publishedAt'],
                    Duration= duration_to_time(item['contentDetails']['duration']),
                    Views=item['statistics']['viewCount'],
                    Likes=item['statistics']['likeCount'],
                    Comments=item.get('commentCount'),
                    Favorite_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data   


def get_comment_info(Vid_ID):
    Comment_data=[]
    try:
        for video_id in Vid_ID:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=25
            )

            response=request.execute()

            for item in response['items']:
                data=dict(Comment_id=item['snippet']['topLevelComment']['id'],
                        Video_ID=item['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_published_date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)


    except:
      pass                
              
    return Comment_data   

def get_playlist_details(channel_id):
        next_page_token=None
        Playlist_data=[]

        while True:
                request = youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=25,
                        pageToken=next_page_token)

                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_ID=item['id'],
                                Title=item['snippet'][ 'title'],
                                Channel_ID=item['snippet'][ 'channelId'],
                                Channel_Name=item['snippet'][ 'channelTitle'],
                                Publshed_Date=item['snippet'][ 'publishedAt'],
                                Video_Count=item['contentDetails'][ 'itemCount'])

                        Playlist_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
                
        return  Playlist_data   


Client=pymongo.MongoClient("mongodb+srv://lokeshhunt46:learner@cluster0.s4b7yel.mongodb.net/?retryWrites=true&w=majority")
db=Client["Youtube-data"]

def channel_details(channel_id):                     #importing data to  Mongodb
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vid_ids=get_videos_ids(channel_id)
    vid_details=get_video_details(vid_ids)
    com_details=get_comment_info(vid_ids)

    coll1=db["Channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vid_details,"comment_information":com_details})
    

    return "uploaded successfully"


def data_from_mongodb(channel_id):
    Client=pymongo.MongoClient("mongodb+srv://lokeshhunt46:learner@cluster0.s4b7yel.mongodb.net/?retryWrites=true&w=majority")
    db=Client["Youtube-data"]
    coll1=db["Channel_details"]
    d = coll1.find_one({'channel_information.channel_id':channel_id})
    return d

st.title("Youtube_Data_Harvesting 📺")
ch_id=st.text_input("Enter channel id")
if ch_id and st.button("Fetch Details"):
     data=channel_details(ch_id)
     st.write(data)
     st.success("data Fetched successfully")



st.write("MongoDB URI:",pymongo.MongoClient ("mongodb+srv://lokeshhunt46:learner@cluster0.s4b7yel.mongodb.net/?retryWrites=true&w=majority"))
st.write("MongoDB Database:", "Youtube-data")
st.write("MongoDB Collection:", "Channel_details")

mongo_client = pymongo.MongoClient ("mongodb+srv://lokeshhunt46:learner@cluster0.s4b7yel.mongodb.net/?retryWrites=true&w=majority")
db = mongo_client["Youtube-data"]
collection = db["Channel_details"]



try:
  mongo_data = list(collection.find({})) 

except Exception as e:
    st.error(f"Error retrieving data from MongoDB: {e}") 

#st.write("MongoDB Data:", mongo_data)

   #delete from table  


mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    database="youtube_data",
    password=""

 

)

print(mydb)
mycursor = mydb.cursor(buffered=True)





#if st.button("migrate to sql"):
  # mongo_data = list(collection.find({}))




    # to Display MongoDB data
for document in mongo_data:
    channel_Name = document.get('channel_information', {}).get('channel_Name', 'N/A')
    channel_id = document.get('channel_information', {}).get('channel_id', 'N/A')
    subscribers = document.get('channel_information', {}).get('subscribers', 'N/A')
    views = document.get('channel_information', {}).get('views', 'N/A')
    Total_vidcount = document.get('channel_information', {}).get('Total_vidcount', 'N/A')
    channel_description = document.get('channel_information', {}).get('channel_description', 'N/A')
    playlist_id = document.get('channel_information', {}).get('playlist_id', 'N/A')
    #st.write("MongoDB Document - channel_Name:",channel_Name)
    #st.write("MongoDB Document - Total_vidcount:",Total_vidcount)


insert_query = "INSERT INTO channel (channel_name,channel_id,subscribers, views,Total_vidcount,channel_description,playlist_id) VALUES (%s, %s, %s,%s,%s,%s,%s)"
values = (channel_Name,channel_id, subscribers, views,Total_vidcount,channel_description,playlist_id)

mycursor.execute(insert_query,values)
mydb.commit()

comments = document.get('comment_information', [])  # Assuming 'comment_information' is the key for comment details

    # Iterate through comments
for comment in comments:
    comment_id = comment.get('Comment_id', 'N/A')
    video_id = comment.get('Video_ID', 'N/A')
    comment_text = comment.get('Comment_Text', 'N/A')
    comment_author = comment.get('Comment_author', 'N/A')
    comment_published_date = comment.get('Comment_published_date', 'N/A')

    check_duplicate_query = "SELECT comment_id FROM comment WHERE comment_id = %s"
    mycursor.execute(check_duplicate_query, (comment_id,))
    existing_record = mycursor.fetchone()

    if existing_record:
        # Handle the duplicate entry (e.g., update or skip)
        pass

    else:      # Insert comment details into MySQL
        insert_comment_query = "INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date) VALUES (%s, %s, %s, %s, %s)"
        comment_values = (comment_id, video_id, comment_text, comment_author, comment_published_date)

        mycursor.execute(insert_comment_query, comment_values)
        mydb.commit()

Video = document.get('video_information', [])

for video in Video:
    Video_Id = video.get('Video_Id', 'N/A')
    Channel_Name = video.get('Channel_Name', 'N/A')
    Channel_Id = video.get('Channel_Id', 'N/A')
    Title = video.get('Title', 'N/A')
    Tags = video.get('Tags', 'N/A')
    Description = video.get('Description', 'N/A')
    Published_date = video.get('Published_date', 'N/A')
    Duration = duration_to_time( video.get('Duration', 'N/A'))
    Views = video.get('Views', 'N/A')
    Likes = video.get('Likes', 'N/A')
    Comments = video.get('Comments', 'N/A')
    Favorite_count = video.get('Favorite_count', 'N/A')
    Definition = video.get('Definition', 'N/A')
    Caption_Status = video.get('Caption_Status', 'N/A')

    # Check if the Video_Id already exists
    check_duplicate_query = "SELECT Video_Id FROM Video WHERE Video_Id = %s"
    mycursor.execute(check_duplicate_query, (Video_Id,))
    existing_record = mycursor.fetchone()

    if existing_record:
        # Handle the duplicate entry (e.g., update or skip)
        pass
    else:
        insert_query = "INSERT INTO Video (Channel_Name, Channel_Id, Video_Id, Title, Tags, Description, Published_date, Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_Status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (Channel_Name, Channel_Id, Video_Id, Title, Tags, Description, Published_date, Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_Status)

        mycursor.execute(insert_query, values)
        mydb.commit()

   
st.subheader("Questions of SQL Query Output need to displayed as table!",divider= 'red')
st.text('1) What are the names of all the videos and their corresponding channels?')
st.text('2) Which channels have the most number of videos, and how many videos do they have')
st.text('3) What are the top 10 most viewed videos and their respective channels')
st.text('4) How many comments were made on each video, and what are their corresponding video names?')
st.text('5) Which videos have the highest number of likes, and what are their corresponding channel names?')
st.text('6) What is the total number of likes and dislikes for each video, and what are their corresponding video names')
st.text('7) What is the total number of views for each channel, and what are their corresponding channel names?')
st.text('8) What are the names of all the channels that have published videos in the year 2022?')
st.text('9) What is the average duration of all videos in each channel, and what are their corresponding channel names?')
st.text('10)  Which videos have the highest number of comments, and what are their corresponding channel names?')

query1 = "SELECT Title, channel_name FROM video"
mycursor.execute(query1)

st.title(" 🔴 Video Analytics Dashboard ")
st.subheader('1. Names of all videos and their corresponding channels',divider= 'rainbow')
df1 = pd.DataFrame(mycursor.fetchall(), columns=["Video Name", "Channel Name"])
st.dataframe(df1)


query2 = "SELECT channel_name, COUNT(Title) AS video_count FROM video GROUP BY channel_name \
           ORDER BY video_count DESC LIMIT 1"
mycursor.execute(query2)

st.subheader('2. Channels with the most number of videos and their count',divider= 'rainbow')
df2 = pd.DataFrame(mycursor.fetchall(), columns=["Channel Name", "Video Count"])
st.dataframe(df2)

query3="SELECT Title, channel_name, views FROM video ORDER BY views DESC LIMIT 10"
mycursor.execute(query3)

st.subheader('3. Top 10 most viewed videos and their respective channels',divider= 'rainbow')
df3=pd.DataFrame(mycursor.fetchall(), columns=["Video Name", "Channel Name", "Views"])
st.dataframe(df3)

query4="SELECT v.Title, COUNT(comment_id) AS comment_count FROM video v \
        LEFT JOIN comment c ON v.video_id = c.video_id GROUP BY v.Title"
mycursor.execute(query4)

st.subheader('4. Number of comments on each video and their corresponding video names',divider= 'rainbow')
df4 = pd.DataFrame(mycursor.fetchall(), columns=["Video Name", "Comment Count"])
st.dataframe(df4)

query5="SELECT Title, channel_name, MAX(likes) AS max_likes FROM video\
          GROUP BY Title ORDER BY max_likes DESC LIMIT 1"
mycursor.execute(query5)

st.subheader('5. Videos with the highest number of likes and their corresponding channel names',divider= 'rainbow')
df5 = pd.DataFrame(mycursor.fetchall(), columns=["Video Name", "Channel Name", "Max Likes"])
st.dataframe(df5)

query6 = "SELECT Title, SUM(likes) AS total_likes FROM video\
           GROUP BY Title"
mycursor.execute(query6)

st.subheader('6. Total number of likes and dislikes for each video and their corresponding video names',divider= 'rainbow')
df6 = pd.DataFrame(mycursor.fetchall(), columns=["Video Name", "Total Likes"])
st.dataframe(df6)

query7="SELECT channel_name, SUM(views) AS total_views FROM video GROUP BY channel_name"

mycursor.execute(query7)

st.subheader('7. Total number of views for each channel and their corresponding channel names',divider= 'rainbow')
df7=pd.DataFrame(mycursor.fetchall(), columns=["Channel Name", "Total Views"])
st.dataframe(df7)


query8="SELECT DISTINCT channel_name FROM video WHERE YEAR(published_date) = 2022"
mycursor.execute(query8)


st.subheader('8. Names of all channels that have published videos in the year 2022',divider= 'rainbow')
df8 = pd.DataFrame(mycursor.fetchall(), columns=["Channel Name"])
st.dataframe(df8)

query9="SELECT channel_name, AVG(duration) AS avg_duration FROM video GROUP BY channel_name"
mycursor.execute(query9)

st.subheader(' 9. Average duration of all videos in each channel and their corresponding channel names',divider= 'rainbow')
df9 = pd.DataFrame(mycursor.fetchall(), columns=["Channel Name", "Avg Duration"])
st.dataframe(df9)



query10 =  """
                SELECT v.Title, v.channel_name, MAX(comment_count) AS max_comments
                FROM video v
                LEFT JOIN (
                    SELECT video_id, COUNT(comment_id) AS comment_count
                    FROM comment
                    GROUP BY video_id
                ) AS temp ON v.video_id = temp.video_id
                GROUP BY v.Title
                ORDER BY max_comments DESC
                LIMIT 1 """
            
mycursor.execute(query10)

st.subheader('10. Videos with the highest number of comments and their corresponding channel names',divider= 'rainbow')
df10 = pd.DataFrame(mycursor.fetchall(), columns=["Video Name", "Channel Name", "Max Comments"])
st.dataframe(df10)   



















    












 







    





    

                                      


     
