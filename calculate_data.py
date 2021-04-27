import bs4
import requests
import sqlite3
import re
import json
from datetime import datetime
import csv
import sys


DB_FILENAME = "data.sqlite3"


"""

Calculates Data Outputs Calcs to File

1. Likes to Unlikes Ratio

2. JOIN IS IN HERE - Get the Time between trailer release date and movie release date

3. Get Comments / Day & Views / Day. Estimate Relative


"""

# DATABASE FUNCTIONS
def dict_factory(cursor, row):
    # online. helps give nicer output format when you execute SQL Statments
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def get_connection():
    # GET DB CONNECTION
    # connect to DB and return to connection
    sqlite_db = sqlite3.connect(str(DB_FILENAME))
    sqlite_db.row_factory = dict_factory
    sqlite_db.execute("PRAGMA foreign_keys = ON")
    return sqlite_db

# Create Database Tables in DataBase File      
def calculate():
   
    # Calculate like - dislike ratio
    # Calculate time from trailer - release
    # Calculate AVG - Views & Comments per Day (Estimated)
    data = {}
    # KEY - VIDEO_ID
    # VALUE - multiple stuff
    dbx = get_connection()

    # 1. Calculate Like Dislike Ratio
    # SELECT specific STATS from stats table
    # JOINS ON ID==ID
    like_stats = dbx.execute("""SELECT trailer_stats.id, trailer_stats.likeCount, trailer_stats.dislikeCount, movie.IMDBScore, movie.BoxOffice from trailer_stats
                                JOIN movie ON movie.id = trailer_stats.id""").fetchall()
    
    for el in like_stats:
        data[el['id']] = {}
        data[el['id']]['like_ratio'] = el['likeCount'] / el['dislikeCount'] # GET THE RATIO
        data[el['id']]['rating'] = el['IMDBScore']
        data[el['id']]['BoxOffice'] = el['BoxOffice']
    
    #1.5 Gather views & comments- for DaysBetwenTrailer&MovieRElease Ratio
    comment_stats = dbx.execute("SELECT id, commentCount, viewCount from trailer_stats").fetchall()
    cm2 = {}
    for el in comment_stats:
        cm2[el['id']] = (el['viewCount'], el['commentCount'])
    
    # 2. Calculate Days between Trailer Release & Movie Release
    # behold a JOIN
    # VIDEO ID -- MOVIE ID - JOIN MATCH ON THEM. Join Trailer Info & Join 
    time_info = dbx.execute("""SELECT trailer_info.id, trailer_info.publishDate, movie.releaseDate from trailer_info 
                                JOIN movie ON trailer_info.id = movie.id""").fetchall()
    # loop through the results
    for el in time_info:
        
        start_time = el['publishDate'].split("T")[0] # trailer release
        # create datetime object with stuffs
        start_time = datetime.strptime(start_time, "%Y-%m-%d")
        # print(el['publishDate'], "--", start_time)
        # print(el['id'])
        
        # create datetime object with stuffs
        end_time = datetime.strptime(el['releaseDate'], "%d %B %Y ") # movie release
        # print(el['releaseDate'], "--", end_time)
        
        diff = end_time - start_time
        # print(diff)
        data[el['id']]['time_diff'] = max(diff.days, 30) # USE MAX TO STOP DIV by 0 ERROR
        # 30 is reasonable estimate. Randomly some have 0 due to errors

        data[el['id']]['releaseDate'] = end_time.strftime("%d-%m-%Y")
        
        # comments per day, and views per day. Estimated Ratio of comments -- time_diff
        data[el['id']]['comment/day'] = cm2[el['id']][1] / max(diff.days, 1)
        data[el['id']]['view/day'] = cm2[el['id']][0] / max(diff.days, 1)
       
        data[el['id']]['views'] = cm2[el['id']][0] 
        data[el['id']]['comments'] = cm2[el['id']][1]

    
    with open("calculated_data.txt", 'w') as ofile:
        ofile.write(json.dumps(data, indent=4))





        



def main():
    print('Begin Calculations')
    
    if len(sys.argv) > 1:
        global DB_FILENAME 
        DB_FILENAME = sys.argv[1]
        print("using DB: " + DB_FILENAME)
    
    calculate()
    print('End Calculations --> calculated_data.txt')
    
    

if __name__ == "__main__":
    main()


'''
Use the grading rubric to figure out what to present. We want to know what API/websites you used, how you limited to storing 25 items at a time in the database, what tables you created for each API/website, show us the Select statement with a JOIN, how you made sure to not have duplicate string data in your database, what you calculated from the data in the database, and what visualization you created.

'''
