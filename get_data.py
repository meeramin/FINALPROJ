import bs4
import requests
import sqlite3
import re
import json
from datetime import datetime
import sys


# Youtube API
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
from bs4 import BeautifulSoup

# VARIABLES
DEVELOPER_KEY = "AIzaSyAWnj0Gt50K1_RLb2mOPlE1oa1M5y5CCCU"
MOVIES_2019 = "PLh2QSchbA3pmMNcgXK9hQTw7gYoaSnKz0"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
DB_FILENAME = "data.sqlite3"


"""

QUERIES YOUTUBE API FOR STATISTICS on ~ 140 Movies
Adds info To Database


"""

# DATABASE FUNCTIONS
def dict_factory(cursor, row):
    #Function for making database rows into nice dictionaries.
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def get_connection():
    """Open a database connection"""
   
    # Connects to DB and returns database connection
    print("connecting to " + DB_FILENAME)
    sqlite_db = sqlite3.connect(str(DB_FILENAME))
    sqlite_db.row_factory = dict_factory
    sqlite_db.execute("PRAGMA foreign_keys = ON")

    return sqlite_db


# def youtube_search_for_playlist(options={}):
#     options['q'] = "2019 Movie Trailers"
#     options['max_results'] = 20
#     youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
#     developerKey=DEVELOPER_KEY)

#   # Call the search.list method to retrieve results matching the specified
#   # query term.
#     print("\tStarting Search for 2019 Movie Trailers")
#     search_response = youtube.search().list(
#         q=options['q'],
#         part='id,snippet',
#         maxResults=options['max_results']
#     ).execute()

#     videos = []
#     channels = []
#     playlists = []

#     # Add each result to the appropriate list, and then display the lists of
#     # matching videos, channels, and playlists.
#     for search_result in search_response.get('items', []):
#         if search_result['id']['kind'] == 'youtube#video':
#             videos.append('%s (%s)' % (search_result['snippet']['title'],
#                                     search_result['id']['videoId']))
#         elif search_result['id']['kind'] == 'youtube#channel':
#             channels.append('%s (%s)' % (search_result['snippet']['title'],
#                                     search_result['id']['channelId']))
#         elif search_result['id']['kind'] == 'youtube#playlist':
#             playlists.append('%s (%s)' % (search_result['snippet']['title'],
#                                         search_result['id']['playlistId']))

#     print ('\tFinished Search: Found #Playlists: ', len(playlists))
#     return playlists


# 1st API Call YOUTUBE - YOUTUBE 1
# QUERY YOUTUBE API FOR FETCHIING TITLES FROM PLAYLIST
def fetch_all_youtube_videos(playlistId):
    """
    Fetches a specific playlist of videos from youtube
    """
    print("\tCollecting Youtube Videos in 2019 Movies Playlist:" + playlistId)
    
    # Youtube Library API Call -- for Items in PLaylist
    # Build a request to youtube
    youtube = build(YOUTUBE_API_SERVICE_NAME,
                    YOUTUBE_API_VERSION,
                    developerKey=DEVELOPER_KEY)
    res = youtube.playlistItems().list(
    part="snippet",
    playlistId=playlistId,
    maxResults="50"
    ).execute() # Execute Request

    # Retrieve Youtube Videos
    nextPageToken = res.get('nextPageToken')
    # Loop Until theres No more videos left to get
    while ('nextPageToken' in res):
        # keep requesting more videos until ready
        # similar query as abov
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

    # Returns a dict/ list of all the videos
    # With ID, Title, ReleaseDate
    return res

def query_data_and_save_to_db(playlist_contents):

    video_info = {}
    # youtube = build(YOUTUBE_API_SERVICE_NAME,
    #                 YOUTUBE_API_VERSION,
    #                 developerKey=DEVELOPER_KEY)
    
    dbx = get_connection()
    
    print("Add up to 25 Item Infos to Database")
    
    # GET STARTING NUM OF STUFF DATABASE
    start_num = dbx.execute("SELECT COUNT(*) FROM trailer_info").fetchone()['COUNT(*)']
    print("Processing: [", end="")
    
    last_num = start_num
    for ix, video in enumerate(playlist_contents['items']):
        
        # Cleanup Title ("Official Trailer - Inception -  ") --> Inception
        title = cleanup_title(video['snippet']['title'])
        
        # Make Sure Cleanup Didn't ERROR 
        if title != "ERROR":
            
            video_info[title] = {
                "publishedAt" : video['snippet']['publishedAt'],
                'id': video['snippet']['resourceId']['videoId']
            }

            # 25 - INSERT OR IGNORE
            dbx.execute("INSERT OR IGNORE INTO trailer_info(id,name,publishDate) VALUES (?,?,?)",\
                (video_info[title]['id'], title, video_info[title]['publishedAt']))

            curr_num = dbx.execute("SELECT COUNT(*) FROM trailer_info").fetchone()['COUNT(*)']
            # print(curr_num, last_num)
            
            # Check if Successful addition occurred
            if curr_num > last_num:
                
                # Performs a nother query to the "STATISTICS" API 
                # Adds 1 Row to Statistics table for this Video ID
                get_save_stats_info(video_info[title]['id'], dbx)

                # Adds 1 Row to the IMDB Movie Table for corresponding trailer/movie
                get_save_imdb_info(video_info[title]['id'], title, dbx)
                
                # if retval != "Success":
                #     print('Error:' + retval)
                print(curr_num, end=' ', flush=True)
                
            last_num = curr_num
            
            # LOGIC FOR MAX 25 OPERATIONS PER ITERATION
            # CHECKS if original count is == 25 less than the current num
            # Stops Loop
            if curr_num - start_num >= 25:
                print("RETURN", end='', flush=True)
                break

    print('] Completed')
# CLEANUP THE YOUTUBE VID TITLES
def cleanup_title(title_str):
    ans = ""
    # Most videos have either - or | seperating the title
    v1 = title_str.split('-')
    v2 = title_str.split('|')
    
    # Choose which symbol to process on
    if len(v1) > 1:
        ans = v1[0]
    elif len(v2) > 1:
        ans = v2[0]
    else :
        return "ERROR"
    # Filter out unwanted symbols
    ans = ans.replace('-','')
    ans = ans.replace('|','')
    ans = ans.replace('::','')

    # Filter Out Trailer / Offical language
    if 'trailer' in ans.lower():
        idx = ans.lower().find('trailer')
        ans = ans[:idx]
    if 'official' in ans.lower():
        idx = ans.lower().find('official')
        ans = ans[:idx]
    
    # Filter Whitespace
    ans = ans.strip()
    
    # error check
    if len(ans) == 0:
        return "ERROR"
    return ans


# YOUTUBE API - FOR STATISTICS ON VIDEOS
def get_save_stats_info(v_id, dbx):
    """ GETS STATISTICS INFO FROM YOUTUBE STATS API """
    """ ADDS TO DATABASE (NON_DUPLICATES) """

    # Create API Query String
    # queries API on this URL -- SPECIFIED VIDEO ID
    # Key = DEVELOPER KEY
    query_str = "https://www.googleapis.com/youtube/v3/videos?part=statistics&id={}&key={}".\
                format(v_id, DEVELOPER_KEY)
    #print(query_str)

    # Make Request to API url from above. Load from JSON --> Dict    
    res = requests.get(query_str)
    # load json into PYTHON DICTIONARY
    text = json.loads(res.text)
    
    # Get Data from API JSON response
    # get the data, if its not found default to 0
    viewCount = int(text['items'][0]['statistics'].get('viewCount',0))
    likeCount = int(text['items'][0]['statistics'].get('likeCount',0))
    dislikeCount = int(text['items'][0]['statistics'].get('dislikeCount',0))
    commentCount = int(text['items'][0]['statistics'].get('commentCount',0))


    # dbx = get_connection()

    # INSERT / IGNORE DUPLICATES
    # INSERT INTO STATISTICS DATABASE
    dbx.execute("INSERT OR IGNORE INTO trailer_stats(id,viewCount,likeCount,dislikeCount,commentCount) VALUES (?,?,?,?,?)",(v_id, viewCount,\
        likeCount, dislikeCount, commentCount
    ))
    
    dbx.commit()

# WEB CRAWLING IMDB
def get_save_imdb_info(v_id, name, dbx):
    core = "https://imdb.com"
    querystr = "https://imdb.com/find?q={}&ref_=nv_sr_sm"

    # dbx = get_connection()
        
    # Create URL for IMDB SEARCH + Search Using Requests Library
    url = querystr.format(name.replace(" ", "+"))
    req = requests.get(url) # SEARCH FOR "MOVIE NAME"
    
    # Process HTML with BS4
    soup = BeautifulSoup(req.text, features="lxml")
    # get list of search results
    # all search results are of tag = td, and class=result_text
    sites = soup.find_all('td', {"class" : "result_text"})
    
    # If No Results for Our Movie Search Query
    if len(sites) == 0:
        return "No Search Results"
    
    # Create Link to Movie on IMDB SITE from search list
    # Take the first result in the list. And get the A tag and the LINK
    link = core + sites[0].find("a")['href'] # /titles/...
    # Request the MOVIE PAGE
    req = requests.get(link) # request from IMDB
   
    # Process HTML with BS4
    soup = BeautifulSoup (req.text, features="lxml")

    # Get IMDB Rating from HTML
    # rating is found in span tags with itemprop=ratingval
    rating = soup.find("span", {"itemprop" : "ratingValue"})
    if rating is None:
        return "No Rating Found"
    
    rating = float(rating.text) # convert to decimal
    
    # Get Box Office Global # from HTML
    box_office = soup.find("h4", string="Cumulative Worldwide Gross:")
    if box_office is None:
        return "No Box Office Found"
    
    # convert from $ 500,000 --> 500000
    box_office_amt =  box_office.parent.text[box_office.parent.text.find("$")+1:]
    box_office_amt = int(box_office_amt.replace(",", ''))
    
    # Get Release Dates
    releaseDate = soup.find("a", {"title" : "See more release dates"})
    if releaseDate is None:
        return "No Release Date Found Found"
    
    date = releaseDate.text.split('(')[0]
    # datetime = date.strptime(date, '%d %b %Y')
    
    
    # Execute Database INSERT or IGNORE DUPLICATES
    dbx.execute("INSERT OR IGNORE INTO movie(id,name,BoxOffice,IMDBScore,releaseDate) VALUES (?,?,?,?,?)",(v_id, name, box_office_amt, rating, date))

    dbx.commit()
    return "Success"

# Create Database Tables in DataBase File      
def setupdb():
    dbx = get_connection()
    # Primary Key ENSURES NO DUPLICATION if id is equal id. 
    
    # trailer databases are for youtube
    #YOutube database 1
    trailer_info = """ CREATE TABLE IF NOT EXISTS trailer_info (
                                        id text PRIMARY KEY,
                                        name text NOT NULL,
                                        publishDate text
                                    ); """
    # Youtube Database 2
    trailer_stats = """ CREATE TABLE IF NOT EXISTS trailer_stats (
                                        id text PRIMARY KEY,
                                        viewCount integer,
                                        likeCount integer,
                                        dislikeCount integer,
                                        commentCount integer,
                                        FOREIGN KEY(id)
                                        REFERENCES trailer_info(id)
                                    ); """
    
    # IMDB Database
    imdb_table = """ CREATE TABLE IF NOT EXISTS movie (
                                        id text PRIMARY KEY,
                                        name text NOT NULL,
                                        BoxOffice int,
                                        IMDBScore float,
                                        releaseDate text,
                                        FOREIGN KEY(id)
                                        REFERENCES trailer_info(id)
                                        ON UPDATE CASCADE
                                        ON DELETE CASCADE
                                    ); """

    # Execute our Create Table Operations
    dbx.execute(trailer_info)     
    dbx.execute(trailer_stats)     
    dbx.execute(imdb_table)     
    dbx.commit()
        




def main():
    # Running the Program
    # python3 get_data.py
    # default database data.sqlite3

    # python3 get_data.py new_database.sqlite3 

    # Args Handling
    if len(sys.argv) > 1:
        global DB_FILENAME 
        DB_FILENAME = sys.argv[1]
    
    print("Using DB : " + DB_FILENAME)
   
    print('Setup Database: If Not There')
    setupdb()
    
    print("Beginning Data Collection from Youtube")
    
    # playlists = youtube_search_for_playlist() 
    # Fetched Data on 145 Trailers from Youtube Playlist
    playlist_info = fetch_all_youtube_videos(MOVIES_2019)

    print("Finished Data Collection from Youtube")

    # ADD UP TO 25 Videos To Database
    # PRIMARY KEY OF DB WILL STOP DUPLICATION
    query_data_and_save_to_db(playlist_info)

    print("Completed Adding Up To 25 Youtube Trailer Data to Databases")
    print("added to Movie, trailer_statistics, trailer_info data tables")

if __name__ == "__main__":
    main()






'''
Use the grading rubric to figure out what to present. We want to know what API/websites you used, how you limited to storing 25 items at a time in the database, what tables you created for each API/website, show us the Select statement with a JOIN, how you made sure to not have duplicate string data in your database, what you calculated from the data in the database, and what visualization you created.

'''
