from requests import HTTPError
import streamlit as st
import googleapiclient.discovery
import pandas as pd

@st.cache_data
def channel_metrics(_myYoutube,channel_id):
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
            'Video_Count': response['items'][0]['statistics']['videoCount'],
            'Channel_Description': response['items'][0]['snippet']['description'],
            'Playlist_Id': play_list_id,
            'Country': response['items'][0]['snippet'].get('country', 'Not Available'),
            'Thumb_Nail': response['items'][0]['snippet']['thumbnails']['default']['url'],
            'Start_Date': response['items'][0]['snippet']['publishedAt'][0:10],
            'Custom_Name':response['items'][0]['snippet']['customUrl']
        }
        
    return data   
@st.cache_data
def comment_data(_myYoutube,vid_lis):
    youtube = _myYoutube
        
    comments = []
    for vids in vid_lis:
        try:
            ch_response = youtube.videos().list(
                part='snippet',
                id=vids).execute()

            for video in ch_response['items']:
                ch_id = video['snippet']['channelId']
                vid_title = video['snippet']["title"]
                Channel_title = video['snippet']["channelTitle"]

            response = youtube.commentThreads().list(
                part='snippet,replies',
                videoId=vids,
                maxResults=30,
            ).execute()
            
            video_comments = []
            for item in response['items']:
                
                comment = item['snippet']['topLevelComment']['snippet']['textOriginal']
                #reply_count = item['snippet']["totalReplyCount"]
                
                
                repl = []
                if 'replies' in item:
                    replies = item['replies']['comments']
                    for reply in replies:
                        reply_text = reply['snippet']['textOriginal']
                        repl.append(reply_text)

                else:
                    repl = ["No reply"]
                
                video_comments.append({"Comments":comment,"Replies": repl})
            comments.append({"Channel_id":ch_id,"Video_id": vids,"Video_title":vid_title,"Comments":video_comments})

        except HTTPError as e:
            if e.resp.status == 403:
                pass

    return comments 
@st.cache_data
def process_comment(comment_df):
    cdf_df = comment_df['Comments'].to_frame()
    cdf_df1 = cdf_df.applymap(lambda x: [] if x == [] else x)  # convert empty list into []
    rldf = pd.DataFrame({})

    for i, row in cdf_df1.iterrows():
        comments = row['Comments']
        if comments:
            for comment_dict in comments:
                temp_df = comment_df.loc[[i]].copy()
                if 'Replies' in comment_dict and comment_dict['Replies']:
                    temp_df.at[i, 'Replies'] = ', '.join(comment_dict['Replies'])
                else:
                    temp_df.at[i, 'Replies'] = "No Replies"
                temp_df.at[i, 'Comments'] = comment_dict['Comments']
                rldf = pd.concat([rldf, temp_df], ignore_index=True)
        else:
            temp_df = comment_df.loc[[i]].copy()
            temp_df.at[i, 'Replies'] = "No Replies"
            temp_df.at[i, 'Comments'] = "No Comments"
            rldf = pd.concat([rldf, temp_df], ignore_index=True)


    
    return rldf
    