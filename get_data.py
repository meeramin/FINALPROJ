import bs4
import requests
import sqlite3
import re
import json

# Youtube API
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser

# VARIABLES
DEVELOPER_KEY = "AIzaSyAWnj0Gt50K1_RLb2mOPlE1oa1M5y5CCCU"
MOVIES_2019 = "PLh2QSchbA3pmMNcgXK9hQTw7gYoaSnKz0"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
DB_FILENAME = "data.sqlite3"


# DATABASE FUNCTIONS
def dict_factory(cursor, row):
    """Convert database row objects to a dictionary keyed on column name.
    This is useful for building dictionaries which are then used to render a
    template.  Note that this would be inefficient for large queries.
    """
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def get_connection():
    """Open a new database connection."""
   
    sqlite_db = sqlite3.connect(str(DB_FILENAME))
    sqlite_db.row_factory = dict_factory
    sqlite_db.execute("PRAGMA foreign_keys = ON")

    return sqlite_db


def youtube_search_for_playlist(options={}):
    options['q'] = "2019 Movie Trailers"
    options['max_results'] = 20
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
    developerKey=DEVELOPER_KEY)

  # Call the search.list method to retrieve results matching the specified
  # query term.
    print("\tStarting Search for 2019 Movie Trailers")
    search_response = youtube.search().list(
        q=options['q'],
        part='id,snippet',
        maxResults=options['max_results']
    ).execute()

    videos = []
    channels = []
    playlists = []

    # Add each result to the appropriate list, and then display the lists of
    # matching videos, channels, and playlists.
    for search_result in search_response.get('items', []):
        if search_result['id']['kind'] == 'youtube#video':
            videos.append('%s (%s)' % (search_result['snippet']['title'],
                                    search_result['id']['videoId']))
        elif search_result['id']['kind'] == 'youtube#channel':
            channels.append('%s (%s)' % (search_result['snippet']['title'],
                                    search_result['id']['channelId']))
        elif search_result['id']['kind'] == 'youtube#playlist':
            playlists.append('%s (%s)' % (search_result['snippet']['title'],
                                        search_result['id']['playlistId']))

    print ('\tFinished Search: Found #Playlists: ', len(playlists))
    return playlists

def fetch_all_youtube_videos(playlistId):
    """
    Fetches a playlist of videos from youtube
    We splice the results together in no particular order
    """
    print("\tCollecting Youtube Videos in 2019 Movies Playlist:" + playlistId)
    youtube = build(YOUTUBE_API_SERVICE_NAME,
                    YOUTUBE_API_VERSION,
                    developerKey=DEVELOPER_KEY)
    res = youtube.playlistItems().list(
    part="snippet",
    playlistId=playlistId,
    maxResults="50"
    ).execute()

    nextPageToken = res.get('nextPageToken')
    while ('nextPageToken' in res):
        nextPage = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlistId,
        maxResults="50",
        pageToken=nextPageToken
        ).execute()
        res['items'] = res['items'] + nextPage['items']

        if 'nextPageToken' not in nextPage:
            res.pop('nextPageToken', None)
        else:
            nextPageToken = nextPage['nextPageToken']

    print("\tCompleted Video Collection. Found " + str(len(res['items'])))
    return res

# CLEANUP THE YOUTUBE VID TITLES
def cleanup_title(title_str):
    ans = ""
    v1 = title_str.split('-')
    v2 = title_str.split('|')
    if len(v1) > 1:
        ans = v1[0]
    elif len(v2) > 1:
        ans = v2[0]
    else :
        return "ERROR"
    
    ans = ans.replace('-','')
    ans = ans.replace('|','')
    ans = ans.replace('::','')
    if 'trailer' in ans.lower():
        idx = ans.lower().find('trailer')
        ans = ans[:idx]
    if 'official' in ans.lower():
        idx = ans.lower().find('official')
        ans = ans[:idx]
    
    ans = ans.strip()
    if len(ans) == 0:
        return "ERROR"
    return ans

def process_and_save_youtube_info(playlist_contents):

    video_info = {}
    youtube = build(YOUTUBE_API_SERVICE_NAME,
                    YOUTUBE_API_VERSION,
                    developerKey=DEVELOPER_KEY)

    for video in playlist_contents['items']:

        title = cleanup_title(video['snippet']['title'])
        
        if title != "ERROR":
            video_info[title] = {
                "publishedAt" : video['snippet']['publishedAt'],
                'id': video['snippet']['resourceId']['videoId']
            }

            query_str = "https://www.googleapis.com/youtube/v3/videos?part=statistics&id={}&key={}".\
                        format(video_info[title]['id'], DEVELOPER_KEY)
            
            res = requests.get(query_str)
            text = json.loads(res.text)
            
            video_info[title]['viewCount'] = text['items'][0]['statistics'].get('viewCount',0)
            video_info[title]['likeCount'] = text['items'][0]['statistics'].get('likeCount',0)
            video_info[title]['dislikeCount'] = text['items'][0]['statistics'].get('dislikeCount',0)
            video_info[title]['commentCount'] = text['items'][0]['statistics'].get('commentCount',0)

    print('Successfully Processed #' + str(len(video_info)) + " videos")
    print('Adding Items to SQL Database')

    dbx = get_connection()

           
def setupdb():
    dbx = get_connection()
    sql_create_table = """ CREATE TABLE IF NOT EXISTS trailer (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        publishDate text,
                                        viewCount integer
                                        likeCount integer
                                        dislikeCount integer
                                        commentCount integer
                                    ); """
    dbx.execute(sql_create_table)     
        




def main():
    print('SetUp DataBase')
    setupdb()
    print("Beginning Data Collection from Youtube")
    
    playlists = youtube_search_for_playlist()
    playlist_info = fetch_all_youtube_videos(MOVIES_2019)

    print("Finished Data Collection from Youtube")

    process_and_save_youtube_info(playlist_info)

if __name__ == "__main__":
    main()


