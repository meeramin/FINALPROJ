import bs4
import requests
import sqlite3
import re
import json
from datetime import datetime
import csv
import sys
import numpy as np


import matplotlib.pyplot as plt

DB_FILENAME = "data.sqlite3"


"""

USES CALCULATED DATA + DATA FROM SQL TO CREATE VARIOUS VISUALIZATIONS

"""

# DATABASE FUNCTIONS
def dict_factory(cursor, row):
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

def get_connection():
    """Open a new database connection."""
   
    sqlite_db = sqlite3.connect(str(DB_FILENAME))
    sqlite_db.row_factory = dict_factory
    sqlite_db.execute("PRAGMA foreign_keys = ON")

    return sqlite_db

# Create Database Tables in DataBase File      
def create_likes_rating_viz():
  
    print('Create Likes ratio -- rating viz')

    # Get Data
    calc_data = {}
    with open ("calculated_data.txt") as file:
        file = file.read()
        calc_data = json.loads(file)

    # gather x and y data points
    x = [v['like_ratio'] for k,v in calc_data.items()]
    y = [v['rating'] for k, v in calc_data.items()]

    # line of best fit
    m, b = np.polyfit(x,y, 1)

    # Create Scatter Plot
    plt.figure()
    plt.scatter(x, y, alpha=0.5)
    plt.plot(x, m*np.array(x) + b) #plot bestfit line
    
    plt.title('YouTube Like/Unlike Ratio and IMDB Score')
    plt.xlabel('Like / Unlike Ratio')
    plt.ylabel('IMDB Score')

# Creates a graph of youtube vies --> box office IMDB
def create_views_box_viz():
  
    print('Creating Views to Box Office Viz')

    # Gather data
    calc_data = {}
    with open ("calculated_data.txt") as file:
        file = file.read()
        calc_data = json.loads(file)

    # Set XY Data
    x = [v['views'] for k,v in calc_data.items()]

    # Get Comments for 3rd Dimensionality
    size = [v['comments'] / 250 for k,v in calc_data.items()]

    # legend helper
    size_legend = []
    y = [v['BoxOffice'] for k, v in calc_data.items()]

    # Create Scatter Plot
    plt.figure()
    sc = plt.scatter(x, y, alpha=0.5, c= "red",s=size)
    
    plt.title('Youtube Trailer Views / Comments vs Box Office')
    plt.xlabel('Youtube Views')
    plt.ylabel('Box Office Amount')
    plt.legend(loc="lower right", *sc.legend_elements("sizes", num=6), title="# Comments (1/250 Scale)")

def create_release_box_viz():
    
    print('creating Release Date -- Box Office Viz')

    calc_data = {}
    # Reads Data In
    with open ("calculated_data.txt") as file:
        file = file.read()
        calc_data = json.loads(file)

    # Get all movie rel dates, Get all Box Office, Get All Ratings
    x = [datetime.strptime(v['releaseDate'], "%d-%m-%Y") for k,v in calc_data.items()]
    y1 = [v['BoxOffice'] for k, v in calc_data.items()]
    y2 = [v['rating'] for k, v in calc_data.items()]

    for i, el in enumerate(x):
        if el.year < 2018:
            del x[i]
            del y1[i]
            del y2[i]

    # Create Time Series Plot
    fig,ax = plt.subplots() # subplot for 2 graphs on same plane w 2 axis
    
    ax.grid= True
    ax2=ax.twinx()
    ax2.set_ylabel("IMDB rating", color="blue")

    ax.plot(x, y1, alpha=0.5, color="orange", label="BoxOffice")
    ax2.plot(x, y2, alpha=0.1, color="blue", label="Rating")

    ax.set_title('Movie Release Data vs Box Office')
    ax.set_xlabel('Time of Year')
    ax.set_ylabel('Box Office', color="orange")

# def create_popular_movies_viz(): # nvm
#     print("creating popular movies viz")


def main():
    if len(sys.argv) > 1:
        global DB_FILENAME 
        DB_FILENAME = sys.argv[1]
    print("Using DB : " + DB_FILENAME)

    print("Creating Visualizations")
    create_likes_rating_viz()
    create_views_box_viz()
    create_release_box_viz() # time release vs box office
    
    print("A final total of 3 Graphs Should Open")
    print("Plots Will Open All At End")


    plt.show()
    print("Completed")
    
    

if __name__ == "__main__":
    main()



'''
done :)
'''
