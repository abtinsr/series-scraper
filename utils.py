import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib
from matplotlib import pyplot as plt
from bs4 import BeautifulSoup as bs
import requests
import lxml
import html5lib
import time
import random
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Create a top list of TV series based on highest-average IMDB rating.
def get_top_list():
    """Returns a dataframe of TV series based on highest-average IMDB rating."""
    
    # Create soup based on IMDB page with highest-average rated 250 TV series.
    URL = "https://www.imdb.com/chart/toptv/?ref_=nv_tvv_250"
    page = requests.get(URL)
    soup = bs(page.content, "lxml")
    
    # Create an empty dataframe.
    top_list = pd.DataFrame(columns=["series_name", "series_rank", "series_origin_year", "series_link"])
    
    # Get all the series' information.
    all_series = soup.find_all('td', class_='titleColumn')
    
    # Run through each series and pick out rank, title, origin year and page link.
    for ser in all_series:

        series_link = ser.find("a")

        series_rank = (
            ser
            .text
            .split(".")[0]
            .strip()
        )

        series_title = (
            ser
            .text
            .split(".")[1]
            .split("(")[0]
            .strip()
        )
        series_year = (
            ser
            .text
            .split("(")[1]
            .replace(")", "")
            .strip()
        )

        row = {
            "series_name": series_title, 
            "series_rank": series_rank,
            "series_origin_year": series_year,
            "series_link": series_link["href"]
          }
        
        # Append series to the top list.
        top_list = top_list.append(row, ignore_index=True)
        
    # This is a way of dealing with series with similar names (e.g. House of Cards)
    top_list['series_id'] = top_list['series_name'] + " (" + top_list['series_origin_year'] + ")"
        
    return(top_list)


# Get the number of seasons for a certain series.
def get_last_season(series_name, top_list, session):
    """Returns the number of seasons for a certain series."""
    
    print("########################################################################################")
    print(f"SCRAPING: {series_name.upper()}")
    
    # Add some random sleep time between each request toward IMDB.
    time.sleep(random.randint(5,15))
    
    # Get the URL and id based on mapping between series title and series link.
    try: 
        series_link = top_list[top_list['series_name']==f"{series_name}"]['series_link'].item()
    except ValueError:
        print(f"{series_name.upper()} references more than one series. Exclude it from the dataset.")
    
    # Send request and get soup.
    URL = f"https://www.imdb.com{series_link}"
    page = session.get(URL) # Write "session" instead of "requests" to deal with connection errors.
    soup = bs(page.content, "lxml")

    max_season = soup.find("select", id="browse-episodes-season")

    # The "browse episodes" button only exists if there are multiple seasons. 
    if max_season is None:
        max_season = 1
    else:
        max_season = int(max_season['aria-label'].split(' ')[0])
        
    print(f"##### TOTAL SEASONS: {max_season}")
            
    return(max_season)

# Get episode data for each season and series in a selected list.
def get_ratings(series_name, top_list, session):
    """Returns episodes' average rating, total votes and description for each season and series in a selected list."""
    
    # Get the URL based on mapping between series title and series link.
    try: 
        series_link = top_list[top_list['series_name']==f"{series_name}"]['series_link'].item()
    except ValueError:
        print(f"{series_name.upper()} references more than one series. Exclude it from the dataset.")
    
    # Create empty dataframe.
    df = pd.DataFrame(columns=["series_name", 
                               "series_rank", 
                               "series_origin_year", 
                               "series_link",
                               "series_n_seasons",
                               "season",
                               "episode",
                               "episode_rating",
                               "episode_total_votes",
                               "episode_description"
                              ])
    
    # Get the max number of seasons.
    seasons = get_last_season(series_name=series_name, top_list=top_list, session=session)
    
    print(f"##### GETTING EPISODES...")
    
    # Loop through each season for a series.
    for season in np.arange(1, seasons+1):
        
        # Add some random sleep time between each request toward IMDB.
        time.sleep(random.randint(5,15))
        
        # Get the soup for each season's IMDB URL page.
        URL = f"https://www.imdb.com{series_link}episodes?season={season}"
        page = session.get(URL) # Write "session" instead of "requests" to deal with connection errors.
        soup = bs(page.content, "lxml")
        
        # Get all info for all episodes in selected season.
        all_episode_info = soup.find_all(class_="info")

        for episode in all_episode_info:
            
            # Get episode description
            try: 
                description = (
                    episode
                    .find("div", class_="item_description")
                    .text
                    .strip()
                )
            except AttributeError:
                description = "N/A"
            # Get episode rating
            try:
                rating = (
                    episode
                    .find("span", class_="ipl-rating-star__rating")
                    .text
                ) # There are actually several of this object. Luckily, the first one is the one we're looking for.
            except AttributeError:
                rating = -1
            # Get episode total votes
            try:
                total_votes = (
                    episode
                    .find("span", class_="ipl-rating-star__total-votes")
                    .text
                    .replace("(", "")
                    .replace(")", "")
                    .replace(',', '')
                )
            except AttributeError:
                total_votes = 0
            # Get episode number
            try:
                number = episode.find("meta", itemprop="episodeNumber")
            except AttributeError:
                number = -1
            
            row = {
                "series_name": series_name, 
                "series_rank": top_list[top_list['series_name']==series_name]['series_rank'].item(),
                "series_origin_year": top_list[top_list['series_name']==series_name]['series_origin_year'].item(),
                "series_link": series_link,
                "series_n_seasons": seasons,
                "season": season,
                "episode": number["content"],
                "episode_rating": rating,
                "episode_total_votes": total_votes,
                "episode_description": description
            }
            
            df = df.append(row, ignore_index=True)
            
        print(f"######## SEASON {season} SCRAPED")
            
    print(f"{series_name.upper()} - TOTAL EPISODES: {len(df)}")
    print("########################################################################################")
    
    df = df.astype(
        {
            'series_name': 'str', 
            'series_rank': 'int64',
            'series_origin_year': 'int64',
            'series_link': 'str', 
            'series_n_seasons': 'int64',
            'season': 'int64',
            'episode': 'int64',
            'episode_rating': 'float64',
            'episode_total_votes': 'int64',
            'episode_description': 'str'
        }
    )
            
    return(df)
        