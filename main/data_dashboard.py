import pandas  as pd
import streamlit as st
from millify import millify

from data_metrics import process_comment


def yt_dash_board(channel_name,channel_info,video_df,comment_dt):
    comment_df= pd.DataFrame(comment_dt)
    comment_df1 = process_comment(comment_df)
    video_count=channel_info['Channel_Name']['Video_Count']
    total_views=channel_info['Channel_Name']['Channel_Views']
    subscribers=channel_info['Channel_Name']['Subscription_Count']
    thumbnail=channel_info['Channel_Name']['Thumb_Nail']
    published_at=channel_info['Channel_Name']['Start_Date']
    channel_link="https://www.youtube.com/"+channel_info['Channel_Name']['Custom_Name']
    
    scol1,scol2,scol3 = st.columns([4.2,0.1,7])
    with scol1 :
    #          
        st.title(':orange',channel_name)
        tcol1, tcol2, tcol3 = st.columns([4, 0.1, 8])
        with tcol1:
            st.image(thumbnail, width=150)
        with tcol3:
            #st.markdown('<br>', unsafe_allow_html=True)
            st.write("<h4 style='text-align: center; color: orange;'>Channel Created Date</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                                <h4>{published_at}</h4>
                            </div>""", unsafe_allow_html=True)
            link_html = f"<div style='text-align: center;'><a href={channel_link} target='_blank'>Channel Link</a></div>"
            st.markdown(link_html, unsafe_allow_html=True)
        st.markdown('<br>', unsafe_allow_html=True)    
        titl1, titl2,titl3 = st.columns([2.5, 3,3])
        with titl1:
            st.markdown("<h4 style='text-align: center; color: orange;'>Total Views</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                                <h5>{millify(total_views,precision=2)}</h5>
                            </div>""", unsafe_allow_html=True)
        with titl2:
            st.markdown("<h4 style='text-align: center; color: orange;'>Total Videos</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                                <h5>{millify(video_count,precision=2)}</h5>
                            </div>""", unsafe_allow_html=True)
        with titl3:
            st.markdown("<h4 style='text-align: center; color: orange;'>Subcribers</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                                <h5>{millify(subscribers,precision=2)}</h5>
                            </div>""", unsafe_allow_html=True)
    #------------------------------------Video data ---------------------------------------------
    video_df = video_df.head(50)
    all_vid_list = video_df['Video_Name'].to_list()
    with scol3:
        vid_names = all_vid_list
        vid_name = st.selectbox(" :orange[Choose Your Video Name]",vid_names)
        selected_vid = video_df.loc[video_df['Video_Name'] == vid_name, 'Video_Id'].to_list()[0]
        selected_vid_data = video_df.loc[video_df['Video_Id'] == selected_vid ,['View_Count','Like_Count','Comment_Count']]
        vid_views = millify(selected_vid_data['View_Count'].to_list()[0],precision=2)
        vid_likes = millify(selected_vid_data['Like_Count'].to_list()[0],precision=2)
        vid_com = millify(selected_vid_data['Comment_Count'].to_list()[0],precision=2)
        stitl1, stitl2,stitl3 = st.columns([3, 3,3])
        with stitl1:
            st.markdown("<h4 style='text-align: center; color: orange;'>No Of Comments</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                                <h5>{vid_com}</h5>
                            </div>""", unsafe_allow_html=True)
        with stitl2:
            st.markdown("<h4 style='text-align: center; color: orange;'>No Of Views</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                                <h5>{vid_views}</h5>
                            </div>""", unsafe_allow_html=True)
        with stitl3:
            st.markdown("<h4 style='text-align: center; color: orange;'>Likes</h4>", unsafe_allow_html=True)
            st.markdown(f"""<div style='text-align: center;'>
                                <h5>{vid_likes}</h5>
                            </div>""", unsafe_allow_html=True)  
        
        selected_com = comment_df1.loc[comment_df1['Video_id'] == selected_vid, ['Comments','Replies']].reset_index(drop=True) ## df
        selected_com = selected_com.set_index('Comments')
        st.dataframe(selected_com.style.set_properties(**{'max-height': '500px', 'overflow-y': 'scroll'}), width=1200, height=198)
        #st.info("Mongodb Db name, collection name displayed here") 
    st.markdown('<hr>', unsafe_allow_html=True)    
    
    return channel_name
    
    