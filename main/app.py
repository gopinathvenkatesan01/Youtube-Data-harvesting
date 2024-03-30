import streamlit as st
import googleapiclient.discovery
from streamlit_option_menu import option_menu
import pymongo as pymongo
import pandas as pd
import psycopg2
from data_dashboard import yt_dash_board
from data_metrics import channel_metrics,comment_data



vid_data =None
class Mongo:
    @staticmethod
    def get_mongo_client():
        """Get MongoDB client instance."""
        return pymongo.MongoClient("mongodb+srv://gopinath:guvi13@cluster0.ikuevcw.mongodb.net/"
                                   "?retryWrites=true&w=majority&appName=Cluster0")

    @staticmethod
    def list_collection_names(collection):
        """List collection names in the database."""
        mongo_client = Mongo.get_mongo_client()
        collection = mongo_client[collection]
        record = collection.list_collection_names()
        return record

    @staticmethod
    def drop_temp_data():
        """Drop temporary data collections."""
        mongo_client = Mongo.get_mongo_client()
        collection = mongo_client['temp']
        col = collection.list_collection_names()
        if len(col) > 0:
            for i in col:
                collection.drop_collection(i)

    @staticmethod
    def push_data(channel_name, collection_name, data):
        """Push data to MongoDB."""
        mongo_client = Mongo.get_mongo_client()
        collection = mongo_client[collection_name]
        record = collection[channel_name]
        record.insert_one(data)

    @staticmethod
    def replace_data(collection_name, channel_name, data):
        """Replace data in MongoDB."""
        mongo_client = Mongo.get_mongo_client()
        db = mongo_client[collection_name]
        db[channel_name].drop()
        Mongo.push_data(channel_name=channel_name, collection_name=collection_name, data=data)
        st.toast("The data has been successfully replaced in MongoDB database")
        st.balloons()
        Mongo.drop_temp_data()
    
    def mongo_upload(collection_name):
     mongo_client = Mongo.get_mongo_client()
     collection = mongo_client['temp']
 
     # Get list of collection names
     record = Mongo.list_collection_names('temp')
 
     if len(record) == 0:
         st.info('No Data Available to Push')
         st.toast('No Data Available to Push')
     else:
        channel_name = record[0]
        print(":>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", channel_name)
        channel_data = {}
        channel_record = collection[channel_name]

        for i in channel_record.find():
            channel_data.update(i)

        collection_list = Mongo.list_collection_names(collection_name)

        if channel_name not in collection_list:
            Mongo.push_data(channel_name=channel_name, collection_name=collection_name, data=channel_data)
            st.toast("The Data has successfully pushed to the MongoDB")
            st.balloons()
            Mongo.drop_temp_data()
            
        else:
            Mongo.mongo_replace_data(channel_name=channel_name, collection_name=collection_name, data=channel_data)

    
    def mongo_replace_data(collection_name, channel_name, data):
        options = ['Select One below','Yes', 'No']
        
        st.warning("The data has already been stored in MongoDB database")
        # Create the radio button with a unique key
        option = st.radio('Choose an option:', options, key="option")
        
        if option == 'Yes':
            Mongo.replace_data(data=data, channel_name=channel_name, collection_name=collection_name)
            st.balloons()
            Mongo.drop_temp_data()
            st.info("Data successfully replaced in MongoDB.")
            option='Select One below'
        elif option == 'No':
            Mongo.drop_temp_data()
            st.toast("Data replacement skipped")    
            st.info("Data replacement skipped")
            option='Select One below'

    def order_collection_names(collection):

        mdb = Mongo.list_collection_names(collection)

        if mdb == []:
            st.info("The Mongodb database is currently empty")

        else:
            st.subheader('List of collections in MongoDB database')
            m = mdb
            c = 1
            for i in m:
                st.write(str(c) + ' - ' + i)
                c += 1    
     
    def channel(database, channel_name):

        mongo_client =Mongo.get_mongo_client()
        collection = mongo_client[database]
        record = collection[channel_name]

        data = []
        for i in record.find({}, {'_id': 0, 'Channel_Name': 1}):
            data.append(i['Channel_Name'])
        
        df = pd.DataFrame(data)
        new_columns = {
            'Channel_Id': 'channel_id',
            'Channel_Name': 'channel_name',
            'Subscription_Count': 'subscription_count',
            'Channel_Views': 'channel_views',
            'Channel_Description': 'channel_description',
            'Playlist_Id': 'playlist_id',
            'Country': 'country'
                    }
        
        df = df.reindex(columns=['Channel_Id', 'Channel_Name', 'Subscription_Count', 'Channel_Views',
                                 'Channel_Description', 'Playlist_Id', 'Country'])
        
        df.rename(columns=new_columns, inplace=True)
        df['subscription_count'] = pd.to_numeric(df['subscription_count'])
        df['channel_views'] = pd.to_numeric(df['channel_views'])
        df = df[['channel_id', 'channel_name', 'subscription_count', 'channel_views', 'channel_description', 'playlist_id', 'country']]
        return df  
    
    def playlist(database,channelname):
        mongo_client =Mongo.get_mongo_client()
        collection = mongo_client[database]
        record =collection[channelname]
        data=[]
        new_columns={
            'Playlist_Id':'playlist_id',
            'Playlist_Name':'playlist_name',
            'Channel_Id':'channel_id',
            'Upload_Id':'upload_id'
        }
        for i in record.find({}, {'_id': 0, 'PlayList': 1}):
            data.extend(i['PlayList'])
        df = pd.DataFrame(data)  
        df.rename(columns=new_columns,inplace=True)
        df = df[['playlist_id', 'playlist_name', 'channel_id', 'upload_id']]
        return df
    
    def find_keys_by_keyword(data, keyword):
        matching_keys = []
        for doc in data.find({}, {'_id': 0}):
            for key in doc:
                # print(key)
                if keyword in key:
                    matching_keys.append(key)
        return matching_keys    
    
    def find_comment_keys_by_keyword(data,keyword):
        matching_keys = []
        for comment in data.keys():
            matching_keys.append(comment)                
        # print('Keys>>>>>>>>>>',matching_keys[0])
        return matching_keys 

    def get_video_and_comments(databse, channel_name):
        mongo_client = Mongo.get_mongo_client()
        collection = mongo_client[databse]
        record = collection[channel_name]
        matching_keys = Mongo.find_keys_by_keyword(data=record, keyword='Video_Id_')

        data = []
        comments=[]

        projection = {'_id': 0}  # Exclude the _id field from the result
        for key in matching_keys:
            projection[key] = 1  # Include each matching key in the projection



        for doc in record.find({}, projection):
            for mt in matching_keys:
                comment_key=[]
                comment_data=[]
                data.append(doc[mt])
                if 'Comments' in doc[mt]: 
                    comment_key = Mongo.find_comment_keys_by_keyword(data=doc[mt]['Comments'], keyword='Comment_Id_')
                    comment_data.append(doc[mt]['Comments'])
                    for comment in comment_data:
                        if len(comment_key) > 0:
                            for i in comment_key:
                                comments.append(comment[i])


        # Convert list of dictionaries to DataFrame


        df = pd.DataFrame(data)
        # Rename columns using the new_columns dictionary
        video_new_columns = {
            'Video_Id': 'video_id',
            'playlist_id': 'playlist_id',
            'Video_Name': 'video_name',
            'Video_Description': 'video_description',
            'Tags': 'tags',
            'PublishedAt': 'published_date',
            'Published_Time': 'published_time',
            'Like_Count': 'like_count',
            'View_Count': 'view_count',
            'Favorite_Count': 'favourite_count',
            'Comment_Count': 'comment_count',
            'Duration': 'duration',
            'Caption_Status': 'caption_status',
            'Thumbnail': 'thumbnail'
        }

        comment_new_columns={
            'Comment_Id':'comment_id',
            'Comment_Text':'comment_text',
            'Comment_Author':'comment_author',
            'Comment_PublishedAt':'comment_published_date',
            'video_id':'video_id',
            'playlist_id': 'playlist_id'
        }
        df.rename(columns=video_new_columns, inplace=True)
        # print(df)
        print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
        cdf = pd.DataFrame(comments)
        cdf.rename(columns=comment_new_columns,inplace=True)
        df = df[['video_id', 'video_name', 'video_description', 
                                                'playlist_id', 'tags', 'published_date', 'published_time', 'view_count', 
                                                'like_count', 'favourite_count', 'comment_count', 'duration', 'thumbnail', 
                                                'caption_status']]
        cdf =cdf[['comment_id', 'comment_text', 'comment_author', 
                                                'comment_published_date','playlist_id','video_id']]
        # print(cdf)
        return df,cdf
      
        
class Sql:
    def psql_client():
        try:
            connection = psycopg2.connect(host='localhost',
                    user='postgres',
                    password='admin',
                    database='youtube')
            return connection
        except Exception as e:
            st.warning(e)
                     
    def create_tables():
        connection = Sql.psql_client()
        cursor =connection.cursor()
        # schema
        cursor.execute(f"""create table if not exists channel(
                                    channel_id 			varchar(255) primary key,
                                    channel_name		varchar(255),
                                    subscription_count	int,
                                    channel_views		int,
                                    channel_description	text,
                                    playlist_id			varchar(255),
                                    country				varchar(255));""")

        cursor.execute(f"""create table if not exists playlist(
                                    playlist_id		varchar(255) primary key,
                                    playlist_name	varchar(255),
                                    channel_id		varchar(255),
                                    upload_id		varchar(255));""")

        cursor.execute(f"""create table if not exists video(
                                    video_id			varchar(255) primary key,
                                    video_name			varchar(255),
                                    video_description	text,
                                    playlist_id			varchar(255),
                                    tags				text,
                                    published_date		date,
                                    published_time		time,
                                    view_count			int,
                                    like_count			int,
                                    favourite_count		int,
                                    comment_count		int,
                                    duration			time,
                                    thumbnail			varchar(255),
                                    caption_status		varchar(255));""")

        cursor.execute(f"""create table if not exists comment(
                                    comment_id				varchar(255) primary key,
                                    comment_text			text,
                                    comment_author			varchar(255),
                                    comment_published_date	date,
                                    playlist_id             varchar(255),
                                    video_id				varchar(255));""")

        connection.commit()
     
    def list_channel_names():
        connection = Sql.psql_client()
        cursor = connection.cursor()
        cursor.execute("select channel_name from channel")
        channel_names = cursor.fetchall()
        channel_names = [i[0] for i in channel_names]
        channel_names.sort(reverse=False)
        return channel_names
    def order_channel_names():
        sql_db = Sql.list_channel_names()
        if sql_db == []:
            st.info("The SQL database is currently empty")
        else:
            st.subheader("List of channels in SQL database")
            c = 1
        for i in sql_db:
            st.write(str(c) + ' - ' + i)
            c += 1        
    
    def delete_channel_data(channel_name):
        print(channel_name)
        connection = Sql.psql_client()
        cursor =connection.cursor()
        cursor.execute('select playlist_id from channel where channel_name = %s',(channel_name,))
        playlist_id = cursor.fetchall()
        playlist_id = playlist_id[0][0]
        cursor.execute('DELETE FROM playlist WHERE upload_id = %s',(playlist_id,))
        cursor.execute('DELETE FROM video WHERE playlist_id = %s',(playlist_id,))
        cursor.execute('DELETE FROM comment WHERE playlist_id = %s',(playlist_id,))
        cursor.execute('DELETE FROM channel WHERE playlist_id = %s',(playlist_id,))
        connection.commit()
        connection.close()
    
    def store_sql_data(option,mdb_collection_name):
        channel = Mongo.channel(mdb_collection_name, option)
        playlist = Mongo.playlist(mdb_collection_name, option)
        video,comment = Mongo.get_video_and_comments(mdb_collection_name, option)            
        connection = Sql.psql_client()
        cursor = connection.cursor()
        cursor.executemany(f"""insert into channel(channel_id, channel_name, subscription_count,
                                channel_views, channel_description, playlist_id, country) 
                                values(%s,%s,%s,%s,%s,%s,%s)""", channel.values.tolist())
        cursor.executemany(f"""insert into playlist(playlist_id, playlist_name, channel_id, 
                                upload_id) 
                                values(%s,%s,%s,%s)""", playlist.values.tolist())
        cursor.executemany(f"""insert into video(video_id, video_name, video_description, 
                                playlist_id, tags, published_date, published_time, view_count, 
                                like_count, favourite_count, comment_count, duration, thumbnail, 
                                caption_status) 
                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",video.values.tolist())
        cursor.executemany(f"""insert into comment(comment_id, comment_text, comment_author, 
                                comment_published_date,playlist_id,video_id) 
                                values(%s,%s,%s,%s,%s,%s)""", comment.values.tolist())
        connection.commit()
        st.toast("Data Migration Successfull")
        st.info("Data migrated successfully")
        st.balloons()
        connection.close()
        
                    
    def main(mdb_collection_name):
        # try:             
            #create table
            Sql.create_tables()

            mdb_channel_names = Mongo.list_collection_names(collection=mdb_collection_name)
            sql_channel_names = Sql.list_channel_names()

            if sql_channel_names == mdb_channel_names == []:
                st.info("Both Mongodb and SQL databases are currently empty")  

            else:
                Mongo.order_collection_names(collection=mdb_collection_name)      
                # Sql.order_channel_names()

                list_mongodb_notin_sql = ['Select one']
                m = Mongo.list_collection_names(mdb_collection_name)
                s = Sql.list_channel_names()

                # verify channel name not in sql
                for i in m:
                    list_mongodb_notin_sql.append(i)

                # channel name for user selection
                option = st.selectbox('', list_mongodb_notin_sql)

                if option == 'Select one':
                    st.warning('Please select the channel') 
                else:
                    if option in s:
                        options = ['Select One below','Yes', 'No']
        
                        st.warning("The data has already been stored  database")
                        # Create the radio button with a unique key
                        data_option = st.radio('Replace Data:', options, key="option")
        
                        if data_option == 'Yes':
                            Sql.delete_channel_data(option)
                            Sql.store_sql_data(mdb_collection_name=mdb_collection_name,option=option)
                            data_option='Select One below'
   
                        elif data_option == 'No':
                            st.info("Data replacement skipped")
                            option='Select One below'      
                    else:
                        Sql.store_sql_data(mdb_collection_name=mdb_collection_name,option=option)
                        
                        
        # except Exception as e:
        #     print('Data Migration Failed',e)  
        #     st.warning('Data Migration Failed')
        #     st.warning(e)          
                
class sql_queries:
    def main():
        st.subheader('Select the Query below')
        q1 = 'Q1-What are the names of all the videos and their corresponding channels?'
        q2 = 'Q2-Which channels have the most number of videos, and how many videos do they have?'
        q3 = 'Q3-What are the top 10 most viewed videos and their respective channels?'
        q4 = 'Q4-How many comments were made on each video with their corresponding video names?'
        q5 = 'Q5-Which videos have the highest number of likes with their corresponding channel names?'
        q6 = 'Q6-What is the total number of likes for each video with their corresponding video names?'
        q7 = 'Q7-What is the total number of views for each channel with their corresponding channel names?'
        q8 = 'Q8-What are the names of all the channels that have published videos in the particular year?'
        q9 = 'Q9-What is the average duration of all videos in each channel with corresponding channel names?'
        q10 = 'Q10-Which videos have the highest number of comments with their corresponding channel names?'
        
        #allvideoname_channelname
        query_1 =f'''select video.video_name, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.playlist_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_id
                            order by channel.channel_name ASC'''
        
        #channelname_totalvideos
        query_2=f'''select distinct channel.channel_name, count(distinct video.video_id) as total
                        from video
                        inner join playlist on playlist.upload_id = video.playlist_id
                        inner join channel on channel.channel_id = playlist.channel_id
                        group by channel.channel_id
                        order by total DESC'''
       
        #mostviewvideos_channelname                
        query_3 =f'''select distinct video.video_name, video.view_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.playlist_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            order by video.view_count DESC
                            limit 10'''       
        
        # videonames_totalcomments
        query_4 =f'''select video.video_name, video.comment_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.playlist_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_name
                            order by video.comment_count DESC'''
                            
        #videonames_highestlikes_channelname
        query_5 =f'''select distinct video.video_name, channel.channel_name, video.like_count
                            from video
                            inner join playlist on playlist.upload_id = video.playlist_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            where video.like_count = (select max(like_count) from video)'''
         
        # videonames_totallikes_channelname
        query_6 =f'''select distinct video.video_name, video.like_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.playlist_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_id
                            order by video.like_count DESC'''
        
        #channelnames_totalviews 
        query_7=f'''select channel_name, channel_views from channel
                            order by channel_views DESC'''
        
        # channelnames_releasevideos
        query_8=f"""select distinct channel.channel_name, count(distinct video.video_id) as total
                            from video
                            inner join playlist on playlist.upload_id = video.playlist_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            where extract(year from video.published_date) = %s
                            group by channel.channel_id
                            order by total DESC"""                                                                                                                                

        # channelnames_avgvideoduration
        query_9=f'''select channel.channel_name, substring(cast(avg(video.duration) as varchar), 1, 8) as average
                            from video
                            inner join playlist on playlist.upload_id = video.playlist_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by channel.channel_id
                            order by average DESC'''
        
        #videonames_channelnames_mostcomments 
        query_10=f'''select video.video_name, video.comment_count, channel.channel_name
                            from video
                            inner join playlist on playlist.upload_id = video.playlist_id
                            inner join channel on channel.channel_id = playlist.channel_id
                            group by video.video_id, channel.channel_name
                            order by video.comment_count DESC
                            limit 1'''                                        
        
        option = st.selectbox('', ['Select One', q1, q2, q3, q4, q5, q6, q7, q8, q9, q10])
        
        if option == q1:
            query_data = sql_queries.query_tool(query_string=query_1)
            columns=["VideoName","ChannelName"]
            sql_queries.display_dataframe(query_data=query_data,columns=columns)

        if option == q2:
            query_data = sql_queries.query_tool(query_string=query_2)
            columns=['Channel Name', 'Total Video']
            sql_queries.display_dataframe(query_data=query_data,columns=columns)
        
        if option == q3:
            query_data = sql_queries.query_tool(query_string=query_3)
            columns=['Video Name', 'Total View', 'Channel Name']
            sql_queries.display_dataframe(query_data=query_data,columns=columns)   
            
        if option ==q4:
            query_data = sql_queries.query_tool(query_string=query_4)
            columns=['Video Name', 'Total Comment', 'Channel Name']
            sql_queries.display_dataframe(query_data=query_data,columns=columns)   
            
        if option ==q5:
            query_data = sql_queries.query_tool(query_string=query_5)
            columns=['Video Name', 'Channel Name', 'Most Like']
            sql_queries.display_dataframe(query_data=query_data,columns=columns)   
            
        if option ==q6:
            query_data = sql_queries.query_tool(query_string=query_6)
            columns=['Video Name', 'Total Like', 'Channel Name']
            sql_queries.display_dataframe(query_data=query_data,columns=columns) 
                                      
        if option ==q7:
            query_data = sql_queries.query_tool(query_string=query_7)
            columns=['Channel Name', 'Total View']
            sql_queries.display_dataframe(query_data=query_data,columns=columns)   
                        
        if option ==q8:
            year = st.text_input('Enter the year')
            submit = st.button('Submit')
            if submit:
                st.session_state.year =year
                query_data = sql_queries.query_tool(query_string=query_8,params=(year,))
                columns=['Channel Name', 'Published Video']
                sql_queries.display_dataframe(query_data=query_data,columns=columns)    
                               
        if option ==q9:
            query_data = sql_queries.query_tool(query_string=query_9)
            columns=['Channel Name', 'Average Video Duration']
            sql_queries.display_dataframe(query_data=query_data,columns=columns) 
              
        if option ==q10:
            query_data = sql_queries.query_tool(query_string=query_10)
            columns=['Video Name', 'Total Comment', 'Channel Name']
            sql_queries.display_dataframe(query_data=query_data,columns=columns) 
                              
    def query_tool(query_string,params=None):
        connection = Sql.psql_client()
        cursor = connection.cursor()
        if params:
            cursor.execute(query_string,params)
        else:    
            cursor.execute(query_string)
        query_data=cursor.fetchall()
        return query_data
    
    def display_dataframe(query_data,columns):
        
        # Boolean to resize the dataframe, stored as a session state variable
        st.checkbox(label="Use container width", value=False, key="use_container_width")
        i = [i for i in range(1, len(query_data) + 1)]
        data =pd.DataFrame(query_data,columns=columns, index=i)
        data =data.rename_axis('S.No')
        data.index = data.index.map(lambda x: '{:^{}}'.format(x, 10))
        st.dataframe(data=data,use_container_width=st.session_state.use_container_width)
            
        
def fetch_youtube_channel_data(_myYoutube, channel_id):
    global vid_data
    response = _myYoutube.channels().list(
        part='contentDetails, snippet, statistics, status',
        id=channel_id
    ).execute()

    if response['items'] is not None:
        play_list_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        data = {}
        data["Channel_Name"] = {
            'Channel_Name': response['items'][0]['snippet']['title'],
            'Channel_Id': response['items'][0]['id'],
            'Subscription_Count': response['items'][0]['statistics']['subscriberCount'],
            'Channel_Views': response['items'][0]['statistics']['viewCount'],
            'Channel_Description': response['items'][0]['snippet']['description'],
            'Playlist_Id': play_list_id,
            'Country': response['items'][0]['snippet'].get('country', 'Not Available')
        }

        play_list = fetch_playlist_data(_myYoutube, channel_id, play_list_id)
        data['PlayList'] = play_list
        videos = fetch_videos_data(_myYoutube, play_list_id)
        vid_data=[{k: v for k, v in d.items() if k != 'Comments'} for d in videos]
        for i, video in enumerate(videos, 1):
            data["Video_Id_" + str(i)] = video
        return data


def fetch_playlist_data(myYoutube, channel_id, play_list_id):
    next_page_token = None
    while True:
        response = myYoutube.playlists().list(
            part='contentDetails, snippet',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        next_page_token = response.get('nextPageToken')
        playlist = []

        for i in range(0, len(response['items'])):
            data = {'Playlist_Id': response['items'][i]['id'],
                    'Playlist_Name': response['items'][i]['snippet']['title'],
                    'Channel_Id': channel_id,
                    'Upload_Id': play_list_id}

            playlist.append(data)
        if next_page_token is not None:
            continue
        else:
            break
    return playlist


def fetch_videos_data(myYoutube, play_list_id):
    videos_list = []
    next_page_token = None

    while True:
        response = myYoutube.playlistItems().list(
            part='contentDetails, snippet',
            playlistId=play_list_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        if response['items'] is not None:
            next_page_token = response.get('nextPageToken')
            for i in range(0, len(response['items'])):
                data = {
                    'channel_name': response['items'][i]['snippet']['title'],
                    'video_id': response['items'][i]['contentDetails']['videoId']
                }

                videos_list.append(data)
        if next_page_token is not None:
            continue
        else:
            break

    if videos_list is not None:
        videos_info = fetch_videos_by_id(myYoutube, videos_list, play_list_id)
    return videos_info


def fetch_videos_by_id(myYoutube, videos_list, play_list_id):
    videos = []
    for i in range(0, len(videos_list)):
        data = video(myYoutube, videos_list[i]['video_id'], play_list_id)
        videos.append(data)
    return videos


def video(myYoutube, video_id, play_list_id):
    response = myYoutube.videos().list(
        part='contentDetails, snippet, statistics',
        id=video_id
    ).execute()

    def time_duration(duration: str):
        duration[2:].replace('M', 'min').replace('S', 's')  # Remove the 'PT' prefix & Replace 'M' with 'T' and remove 'S'
        duration = pd.Timedelta(duration)
        return str(duration).split()[-1]

    caption = {'true': 'Available', 'false': 'Not Available'}
    if response['items'] is not None:
        data = {
            'Video_Id': response['items'][0]['id'],
            'playlist_id': play_list_id,
            'Video_Name': response['items'][0]['snippet']['title'],
            'Video_Description': response['items'][0]['snippet']['description'],
            'Tags': response['items'][0]['snippet'].get('tags', []),
            'PublishedAt': response['items'][0]['snippet']['publishedAt'],
            'Published_Time': response['items'][0]['snippet']['publishedAt'][11:19],
            'View_Count': response['items'][0]['statistics']['viewCount'],
            'Like_Count': response['items'][0]['statistics'].get('likeCount', 0),
            'Favorite_Count': response['items'][0]['statistics']['favoriteCount'],
            'Comment_Count': response['items'][0]['statistics'].get('commentCount', 0),
            'Duration': time_duration(response['items'][0]['contentDetails']['duration']),
            'Thumbnail': response['items'][0]['snippet']['thumbnails']['default']['url'],
            'Caption_Status': caption[response['items'][0]['contentDetails']['caption']]
        }
        comments = comment_by_video_id(myYoutube, video_id,play_list_id)
        comments_data = {}

        if comments is not None:
            for i, comment in enumerate(comments, 1):
                comments_data["Comment_Id_" + str(i)] = comment

            data['Comments'] = comments_data

    if data['Tags'] == []:
        del data['Tags']

    return data


def comment_by_video_id(myYoutube, video_id,play_list_id):
    try:
        response = myYoutube.commentThreads().list(
            part='id, snippet',
            videoId=video_id,
            maxResults=100).execute()

        comment = []
        if 'error' in response:
            error_message = response['error']['message']
            print(f"Error occurred: {error_message}")

        else:
            for i in range(0, len(response['items'])):
                data = {'Comment_Id': response['items'][i]['id'],
                        'Comment_Text': response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'Comment_Author': response['items'][i]['snippet']['topLevelComment']['snippet'][
                            'authorDisplayName'],
                        'Comment_PublishedAt': response['items'][i]['snippet']['topLevelComment']['snippet'][
                            'publishedAt'],
                        'video_id': video_id,
                        'playlist_id':play_list_id}

                comment.append(data)

        return comment

    except Exception as e:
        print(f"An error occurred: {str(e)}")


def main():
    st.set_page_config(page_title="YouTube Channel Data Harvesting And Warehousing", layout='wide',
                       page_icon=':globe_with_meridians:')
    st.title("YouTube Analytics")
    st.write('')
    st.write('')
    with st.sidebar:
        select_box = option_menu(
            menu_title="YouTube Analytics",
            options=["YouTube Data Retrival", "Push Data to MongoDB", "Migration Of Data","SQL Queries"],
            icons=["youtube","cloud-upload","database-fill-check","clipboard-data"],
            menu_icon="bar-chart-fill",
            default_index=0,
            )
    # select_box = st.sidebar.selectbox(
    # "Choose Function",
    # ("YouTube Data Retrival", "Push Data to MongoDB", "Migration Of Data","SQL Queries"))
     
    if select_box =='YouTube Data Retrival':
       col1, col2 = st.columns([4,8])
       with col1:
           channel_id = st.text_input("Enter Channel ID:")
       with col2:
           api_key = st.text_input("Enter your YouTube Data API key:", type="password")
       st.write("<br>",unsafe_allow_html=True)
       st.write("<br>",unsafe_allow_html=True)
       load = st.button("Fetch Channel Data",key="fetch_channel_data")
       
       if "load_state" not in st.session_state:
           st.session_state.load_state =False
   
   #Channel Info
       if load or st.session_state.load_state:
            st.session_state.load_state =True
            if api_key:
                with st.spinner('Loading Data...'):
                    st.toast('Retrieving Data from YouTube')
                    myYoutube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
                    channel_data = fetch_youtube_channel_data(myYoutube, channel_id)
                    if channel_data:
                        channel_name = channel_data['Channel_Name']['Channel_Name']
                        Mongo.drop_temp_data()
                        Mongo.push_data(channel_name=channel_name, collection_name='temp', data=channel_data)
                         # Metrics
                        channel_info = channel_metrics(myYoutube,channel_id)
                        video_df = pd.DataFrame(vid_data)
                        comment_dt = comment_data(myYoutube,video_df['Video_Id'].head(50).to_list())
                        yt_dash_board(channel_info=channel_info,channel_name=channel_name,comment_dt=comment_dt,video_df=video_df)
                        # st.write("Channel Data:")
                        # pprint(channel_data['Channel_Name'])
                        # st.json(channel_data['Channel_Name'])
                        # st.json(channel_data['PlayList'])
                        st.toast('Retrieved data from YouTube successfully')
                        # st.balloons()
                    else:
                        st.info("No data Found....")    
                
            else:
                st.error("Please enter your YouTube Data API key.")

    elif select_box == 'Push Data to MongoDB':
        with st.spinner('Publishing data to MongoDB...'):
            st.toast('Publishing Data in MongoDB')
            Mongo.mongo_upload("youtubeChannelData")
    
    elif select_box =="Migration Of Data":
        with st.spinner('Migrating Data......'):
            st.toast('Migrating Data')
            Sql.main(mdb_collection_name='youtubeChannelData')
            
    elif select_box =="SQL Queries":
        sql_data = Sql.list_channel_names()
        if sql_data == []:
            st.info('The Database is currently Empty')
            st.toast('The Database is currently Empty')
        else:
            sql_queries.main()
                
            
                
            


if __name__ == '__main__':
    main()
