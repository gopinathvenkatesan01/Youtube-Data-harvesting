import streamlit  as st
import googleapiclient.discovery
import streamlit as st
from pprint import pprint
import pandas as pd

# fetchYouTubeChannelData
def fetchYoutubeChannelData(myYoutube, channelId):
    # try:
        response = myYoutube.channels().list(
            part='contentDetails, snippet, statistics, status',
            id=channelId
        ).execute()
          
        if response['items'] is not None:
          playListId = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
          
          data ={
              'channel_name':response['items'][0]['snippet']['title'],
              'channel_id':response['items'][0]['id'],
              'subscription_count':response['items'][0]['statistics']['subscriberCount'],
              'channel_views':response['items'][0]['statistics']['videoCount'],
              'channel_description':response['items'][0]['snippet']['description'],
              'play_list_id':playListId,
              'country': response['items'][0]['snippet'].get('country', 'Not Available')
          }
          
          playList = fetchPlayListData(myYoutube,channelId)
          videos = fetchVideosData(myYoutube,playListId)
          data['playList'] =playList
          data['videos'] =videos
        return data
    
    # except Exception as e:
    #     st.error("Something went wrong in fetch channelData: {}".format(e))
    #     return None
 
 #fetch playList 
def fetchPlayListData(myYouTube,channelId):
    data =[]
    nextPageToken =None
    # try:
    while(True):
        response = myYouTube.playlists().list(
            part='contentDetails, snippet',
            channelId=channelId,
            maxResults=50,
            pageToken=nextPageToken 
            ).execute()  
        nextPageToken = response.get('nextPageToken')
        data.append(response['items'][0] if 'items' in response else None)
        if(nextPageToken!=None):
          continue
        else:
          break
    return data
    # except Exception as e:
    #     st.error("Something went wrong in fetchPlayList: {}".format(e))
    #     return None

    
# fetch videos data
def fetchVideosData(myYouTube,playListId):
    videosList = []
    nextPageToken=None
    
    while(True):  
      response = myYouTube.playlistItems().list(
        part='contentDetails, snippet',
        playlistId=playListId,
        maxResults=50,
        pageToken=nextPageToken 
    ).execute()
      if response['items'] is not None:
        nextPageToken = response.get('nextPageToken')
        for i in range(0, len(response['items'])):
            data = {
                    'channel_name': response['items'][i]['snippet']['title'],
                    'video_id': response['items'][i]['contentDetails']['videoId']
                   }

            videosList.append(data)      
      if(nextPageToken!=None):
          continue
      else:
          break
      
    if videosList is not None:
        videosInfo=fetchVideosById(myYouTube,videosList,playListId)
    return videosInfo

#fetch video by ID
def fetchVideosById(myYouTube,videosList,playListId):
    videos=[]
    for i in range(0,len(videosList)):
        data=video(myYouTube,videosList[i]['video_id'],playListId)
        videos.append(data)
    return videos  
  
def video(myYouTube,videoId,playListId):
    
    response = myYouTube.videos().list(
        part='contentDetails, snippet, statistics',
        id=videoId
    ).execute()
    
    def timeDuration(duration:str):
        duration[2:].replace('M', 'min').replace('S', 's') # Remove the 'PT' prefix & Replace 'M' with 'T' and remove 'S'
        duration=pd.Timedelta(duration)
        return str(duration).split()[-1]


    caption = {'true': 'Available', 'false': 'Not Available'}
    if response['items'] is not None:
        data = {
                'video_id': response['items'][0]['id'],
                'playlist_id': playListId,
                'video_name': response['items'][0]['snippet']['title'],
                'video_description': response['items'][0]['snippet']['description'],
                'tags': response['items'][0]['snippet'].get('tags', []),
                'published_date': response['items'][0]['snippet']['publishedAt'][0:10],
                'published_time': response['items'][0]['snippet']['publishedAt'][11:19],
                'view_count': response['items'][0]['statistics']['viewCount'],
                'like_count': response['items'][0]['statistics'].get('likeCount', 0),
                'favourite_count': response['items'][0]['statistics']['favoriteCount'],
                'comment_count': response['items'][0]['statistics'].get('commentCount', 0),
                'duration': timeDuration(response['items'][0]['contentDetails']['duration']),
                'thumbnail': response['items'][0]['snippet']['thumbnails']['default']['url'],
                'caption_status': caption[response['items'][0]['contentDetails']['caption']]
            }
        data['comments'] =commentByVideoId(myYouTube,videoId)
    if data['tags'] == []:
        del data['tags']

    return data
    
def commentByVideoId(myYouTube,videoId):
  try:  
     response = myYouTube.commentThreads().list(
            part='id, snippet',
            videoId=videoId,
            maxResults=100).execute()

     comment = []
     if 'error' in response:
        error_message = response['error']['message']
        print(f"Error occurred: {error_message}")
        
     else:
        for i in range(0, len(response['items'])):
            data = {'comment_id': response['items'][i]['id'],
                    'comment_text': response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'comment_author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'comment_published_date': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][0:10],
                    'comment_published_time': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][11:19],
                    'video_id': videoId}
            
            comment.append(data)
            
     return comment
 
  except Exception as e:
    print(f"An error occurred: {str(e)}")  
     
def main():
    st.title("YouTube Channel Data Viewer")
    col1, col2 = st.columns(2)
    with col1:
        channelId = st.text_input("Enter Channel ID:")
    with col2:
        apiKey = st.text_input("Enter your YouTube Data API key:", type="password")
          
    if st.button("Fetch Channel Data"):
        if apiKey:
            myYoutube = googleapiclient.discovery.build("youtube", "v3", developerKey=apiKey)
            channel_data = fetchYoutubeChannelData(myYoutube, channelId)
            if channel_data:
                st.write("Channel Data:")
                st.json(channel_data)
        else:
            st.error("Please enter your YouTube Data API key.")

if __name__ == '__main__':
    main()
