import json
import random
import sqlite3
import time
import os
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn
import logging
from dotenv import load_dotenv
from itertools import combinations

# Load env file
load_dotenv()

# Rich log
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)
FORMAT = "%(message)s"
logging.basicConfig(
    level=log_level_str, 
    format=FORMAT, 
    datefmt="[%X]", 
    handlers=[RichHandler()]
)
log = logging.getLogger("rich")

# Variables
schedule_list = []
solo_db = os.getenv("DB_LOCATION")
channel_file = os.getenv("CHANNEL_FILE")

# Functions
def initialize_schedule_db():
    """
    Initializes the Schedule table in the database

    Args:
        None

    Returns:
        None
    """

    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()

    log.debug("Initializing Schedule database")
    table = """ CREATE TABLE IF NOT EXISTS SCHEDULE(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Channel INTEGER,
        Showtime TEXT,
        End TEXT,
        Filepath TEXT,
        Chapter INTEGER,
        Runtime TEXT
    );"""

    cursor.execute(table)
    conn.close()

def clear_schedule_table():
    """
    Completely clears the schedule table

    Args:
        None

    Returns:
        None
    """

    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()
    cursor.execute("DELETE FROM SCHEDULE")
    conn.commit()
    conn.close()

def clear_old_schedule_items():
    '''
    Removes all old scheduled items from the SCHEDULE table
    where End time has been passed by current time

    Args:
    Returns:  
        None
        
    Raises:
    '''

    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()

    current_time = (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
    query = f"""SELECT * FROM SCHEDULE WHERE End < '{current_time}'"""
    cursor.execute(query)
    results = cursor.fetchall()
    log.info(f"Found {len(results)} old items in Schedule")
    
    query = f"""DELETE FROM SCHEDULE WHERE End < '{current_time}'"""
    cursor.execute(query)
    conn.commit()
    conn.close()

def check_schedule_for_rebuild():
    """
    Checks for items scheduled for today in the Schedule table

    Args:
        None

    Returns:
        (bool) - True if there are no future dates in the schedule

    Raises:

    Example:
        check_schedule_for_rebuild()
    """
    global channel_file

    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()
    rebuild_needed = False

    # Extract all channel numbers from channels file
    now = datetime.now().strftime("%Y-%m-%d")
    with open(channel_file, "r") as channel_file_input:
        channel_data = json.load(channel_file_input)

    for channel in channel_data:
        channel_number = channel_data[channel]["channel_number"]
        log.info(f"Checking for channel {channel_number} for {now}")
        query = f""" SELECT Showtime, End, Filepath FROM SCHEDULE WHERE Channel = {channel_number} AND DATE(Showtime) = {now} ORDER BY Showtime ASC"""
        cursor.execute(query)
        items = [{
            "showtime": datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"),
            "end": datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S"),
            "filepath": row[2]
        } for row in cursor.fetchall()]

        log.info(f"Found {len(items)} items in schedule for channel {channel_number}")

        if not items:
            log.warning(f"No items found for channel {channel_number}")
            rebuild_needed = True
            break

    conn.close()

    return rebuild_needed

def insert_into_schedule(channel_number, showtime, end, filepath, chapter, runtime):
    """
    Inserts a single media item into the schedule table

    Args:
        channel_number (integer): Channel number
        showtime (string): Time of which this media item plays
        end (string): Time of which this media item stops
        filepath (string): Video file
        chapter (integer): If episode, which chapter number
        length (string): Length of media item

    Returns:
        None

    Raises:

    Example:
        insert_into_schedule(2, "05:00:00", "05:01:02", /folder/media.mp4, 2, "02:45:00")
    """

    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO SCHEDULE (Channel, Showtime, End, Filepath, Chapter, Runtime) VALUES (?, ?, ?, ?, ?, ?)",
        (channel_number, showtime, end, filepath, chapter, runtime),
    )

    # Commit changes to database
    conn.commit()
    conn.close()

def search_database(search_term):
    """
    Searches the Solostation Database based on tags

    Args:
        search_term (list): List of strings, search keywords/terms

    Returns:
        results (list): List of dictionaries in tuple format

    Raises:
    Example:
        search_database(['action', 'movie'])
    """

    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()

    # log.debug(f"Searching for {search_term}")

    query = f"""
        SELECT 'TV' AS source_table, json_object('ID', ID, 'Name', Name, 'ShowName', ShowName, 'Season', Season, 'Episode', Episode, 'Overview', Overview, 'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath, 'LastPlayed', LastPlayed) AS data
        FROM TV WHERE Tags LIKE '%{search_term}%' OR ShowName LIKE '%{search_term}%' OR Name LIKE '%{search_term}%'

        UNION ALL

        SELECT 'MOVIE' AS source_table, json_object('ID', ID, 'Name', Name, 'Year', Year, 'Overview', Overview, 'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath, 'LastPlayed', LastPlayed) AS data
        FROM MOVIE WHERE Tags LIKE '%{search_term}%' OR Name LIKE '%{search_term}%' OR Year LIKE '%{search_term}%'

        UNION ALL

        SELECT 'MUSIC' AS source_table, json_object('ID', ID, 'Artist', Artist, 'Title', Title, 'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath) AS data
        FROM MUSIC WHERE Tags LIKE '%{search_term}%'

        UNION ALL

        SELECT 'WEB' AS source_table, json_object('ID', ID,  'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath) AS data
        FROM WEB WHERE Tags LIKE '%{search_term}%'

        UNION ALL

        SELECT 'COMMERCIALS' AS source_table, json_object('ID', ID,  'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath, 'LastPlayed', LastPlayed) AS data
        FROM COMMERCIALS WHERE Tags LIKE '%{search_term}%';
    """

    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()

    return results

def get_chapters(filepath):
    '''
    Finds the filepath in the TV table and checks to see if episode chapters are
    in the CHAPTERS table.

    Args:
        filepath (string): Video file

    Returns:
        chapters (list): All chapters from the CHAPTERS table for filepath
        OR
        None

    Raises:
    Example:
    '''

    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()

    # Get episode from TV table using filepath
    query = f"SELECT ID FROM TV WHERE Filepath = '{filepath}'"
    cursor.execute(query)
    episode_ID = cursor.fetchall()
    if episode_ID:
        # Pull EpisodeID as a string from SQLite Tuple
        episode_ID, = episode_ID[0]

        # Search and retrieve all chapters for this EpisodeID
        query = f"SELECT Title, Start, End FROM CHAPTERS WHERE EpisodeID = '{episode_ID}'"
        cursor.execute(query)
        chapters = cursor.fetchall()

        conn.close()
        
        return chapters
    else:
        return None

def get_next_tv_playtime(marker, episode_TD):
    """
    Determine the next available play time and size of episode block

    Args:
        marker (datetime): Current place in the schedule timeline
        episode_TD (timedelta): Timedelta of episode runtime

    Returns:
        episode_block (timedelta)
        next_play_time (datetime)

    Raises:
    Example:
    """

    # Find next play time depending on episode runtime
    if episode_TD < timedelta(minutes=30):
        episode_block = timedelta(minutes=30)
        if marker.minute < 30:
            next_play_time = marker.replace(minute=30, second=0, microsecond=0)
        else:
            if marker.hour == 23:
                next_play_time = marker.replace(day=(marker.day + 1), hour=0, minute=0, second=0, microsecond=0)
            else:
                next_play_time = marker.replace(hour=(marker.hour + 1), minute=0, second=0, microsecond=0)
    else:
        episode_block = timedelta(hours=1)
        if marker.minute < 30:
            if marker.hour == 23:
                next_play_time = marker.replace(day=(marker.day + 1), hour=0, minute=0, second=0, microsecond=0)
            else:
                next_play_time = marker.replace(hour=(marker.hour + 1), minute=0, second=0, microsecond=0)
            episode_block = timedelta(minutes=30)
        else:
            episode_block = timedelta(hours=1)
            if marker.hour == 23:
                next_play_time = marker.replace(day=(marker.day + 1), hour=0, minute=30, second=0, microsecond=0)
            else:
                next_play_time = marker.replace(hour=(marker.hour + 1), minute=30, second=0, microsecond=0)

    return episode_block, next_play_time

def get_next_movie_playtime_c2(marker):
    '''
    Find the next available time to play the next movie, ensuring that the next play time
    starts on or half the hour.

    Args:
        marker (datetime): Location of marker

    Returns:
        next_play_time (datetime): Next available play time for next movie

    Raises:
    Example:
    '''

    if marker.minute < 30:
        next_play_time = marker.replace(minute=30, second=0, microsecond=0)
    else:
        if marker.hour == 23:
            next_play_time = marker.replace(day=(marker.day + 1), hour=0, minute=0, second=0, microsecond=0)
        else:
            next_play_time = marker.replace(hour=(marker.hour + 1), minute=0, second=0, microsecond=0)

    return next_play_time


def get_next_movie_playtime(marker):
    '''
    Find the next available time to play the next movie, giving 15-30 between show times
    to play web content, movie trailers, etc

    Args:
        marker (datetime): Location of marker

    Returns:
        next_play_time (datetime): Next available play time for next movie

    Raises:
    Example:
    '''

    marker = marker.replace(second=0, microsecond=0)
    if marker.minute >= 0 and marker.minute <=15:
        # log.debug("30")
        next_play_time = marker.replace(minute=30, second=0)
    if marker.minute > 15 and marker.minute <=30:
        # log.debug("45")
        next_play_time = marker.replace(minute=45, second=0)
    if marker.minute > 30 and marker.minute <=45:
        # log.debug("00")
        if marker.hour == 23:
            next_play_time = (marker.replace(hour=0, minute=0, second=0)) + timedelta(days=1)
        else:
            next_play_time = marker.replace(hour=(marker.hour + 1), minute=0, second=0)
    if marker.minute > 45 and marker.minute <=59:
        # log.debug("15")
        if marker.hour == 23:
            next_play_time = (marker.replace(hour=0, minute=15, second=0)) + timedelta(days=1)
        else:
            next_play_time = marker.replace(hour=(marker.hour + 1), minute=15, second=0)

    return next_play_time

def add_post_movie(channel_number, marker, next_play_time):
    """
    Schedules movie trailers and web content after a movie has been played.

    Args:
        channel_number (integer): Number of current channel
        marker (datetime): Location of marker
        next_play_time (datetime): Next available play time for next movie

    Returns:
        marker (datetime): Location of marker

    Raises:
    Example:
    """

    all_trailers = []
    filled = False

    while not filled:
        # Add all trailers if all_trailers is empty
        if len(all_trailers) == 0:    
            table, data = zip(*search_database("trailers"))
            data = [json.loads(d) for d in data]
            for d in data:
                all_trailers.append(d)
        random.shuffle(all_trailers)
        
        for trailer in all_trailers:
            # log.debug(f"{trailer}")
            hours, minutes, seconds = map(int, trailer["Runtime"].split(":"))
            trailer_runtime_TD = timedelta(hours=hours,minutes=minutes,seconds=seconds)
            post_marker = marker + trailer_runtime_TD

            if post_marker > next_play_time:
                filled = True
                break
            else:
                # Insert into schedule and move the marker
                insert_into_schedule(channel_number, marker, post_marker, trailer["Filepath"], None, trailer["Runtime"])
                marker = post_marker
                trailer_index = all_trailers.index(trailer)
                all_trailers.pop(trailer_index)
        
    return marker

def schedule_loud(channel_number, marker, channel_end_datetime):
    """ Schedules for the Loud Channel """

    # Get total time of channel runtime in seconds for Progress
    total_seconds = (channel_end_datetime - marker).total_seconds()
    log.debug(f"{total_seconds=}")   
    all_music = []
    all_idents = []

    # Monitor progress of scheduling for channel
    with Progress() as progress:
        task = progress.add_task("[green]Scheduling loud ...", total=total_seconds)
        completed_time = 0

        while not progress.finished:
        # while marker < channel_end_datetime:
            if len(all_music) == 0:
                # Search database for 'music' tag and create lists from results
                all_music.extend([json.loads(data) for table, data in search_database("music") if "music" in json.loads(data)["Filepath"]])
                random.shuffle(all_music)
    
            # 2 music videos, 1 ident
            for music_index, music_video in enumerate(all_music):
                # Insert video into schedule
                # log.debug(f"{music_index} - {all_music[music_index]['Filepath']}")
                hours, minutes, seconds = map(int, music_video["Runtime"].split(":"))
                mv_runtime_TD = timedelta(hours=hours,minutes=minutes,seconds=seconds)
                post_marker = marker + mv_runtime_TD
                insert_into_schedule(channel_number, marker, post_marker, music_video["Filepath"], None, music_video["Runtime"])
                marker = post_marker
                all_music.pop(all_music.index(music_video))
                completed_time += mv_runtime_TD.total_seconds()

                if music_index % 2 == 0:
                    if len(all_idents) < 2:
                        # Search database for 'ident' tag and create lists from results
                        all_idents.extend([json.loads(data) for table, data in search_database("ident") if "idents" in json.loads(data)["Filepath"]])
                        random.shuffle(all_idents)

                    # Schedule ident
                    hours, minutes, seconds = map(int, all_idents[0]["Runtime"].split(":"))
                    mv_runtime_TD = timedelta(hours=hours,minutes=minutes,seconds=seconds)
                    post_marker = marker + mv_runtime_TD
                    insert_into_schedule(channel_number, marker, post_marker, all_idents[0]["Filepath"], None, all_idents[0]["Runtime"])
                    marker = post_marker
                    all_idents.pop(all_idents.index(all_idents[0]))

                    # Update Progress
                    completed_time += mv_runtime_TD.total_seconds()
                    progress.update(task, completed=completed_time)


def schedule_ppv(channel_number, marker, channel_end_datetime):
    """ Schedules for the PPV Channels """

    # Get total time of channel runtime in seconds for Progress
    total_seconds = (channel_end_datetime - marker).total_seconds()

    with Progress() as progress:
        task = progress.add_task("[green]Scheduling PPV ...", total=total_seconds)
        completed_time = 0

        while not progress.finished:
            # Search database for 'movie' tag and create lists from results
            selected_movies = select_weighted_movie(["movie"])

            # Select random movie
            ppv_movie = selected_movies[0]
            ppv_movie_filepath = ppv_movie[0]
            ppv_movie_runtime = ppv_movie[3]

            # Calculate movie runtime
            hours, minutes, seconds = map(int, ppv_movie_runtime.split(":"))
            movie_TD = timedelta(hours=hours,minutes=minutes,seconds=seconds)

            # Update Progress
            completed_time += movie_TD.total_seconds()
            # log.debug(f"{completed_time=}")
            progress.update(task, completed=completed_time)

            # Fill with the movie until the channel end time
            while marker < channel_end_datetime:
                post_marker = marker + movie_TD
                insert_into_schedule(channel_number, marker, post_marker, ppv_movie_filepath, None, ppv_movie_runtime)
                marker = post_marker #+ timedelta(seconds=1)

def schedule_bang(channel_number, marker, channel_end_datetime):
    """ Schedules for the Bang Channel """

    # Get total time of channel runtime in seconds for Progress
    total_seconds = (channel_end_datetime - marker).total_seconds()

    with Progress() as progress:
        task = progress.add_task("[green]Scheduling bang! ...", total=total_seconds)
        completed_time = 0

        while not progress.finished:
            # Select 20 movies weighted on LastPlayed
            selected_movies = select_weighted_movie(["action", "movie"])
            
            # Insert selected movies into the schedule
            for movie in selected_movies:
                movie_filepath = movie[0]
                movie_runtime = movie[3]
                movie_h, movie_m, movie_s = map(int, movie_runtime.split(":"))
                movie_TD = timedelta(hours=movie_h, minutes=movie_m, seconds=movie_s)

                # Update Progress
                completed_time += movie_TD.total_seconds()
                # log.debug(f"{completed_time=}")
                progress.update(task, completed=completed_time)

                post_marker = marker + movie_TD
                insert_into_schedule(channel_number, marker, post_marker, movie_filepath, None, movie_runtime)

                # Update LastPlayed
                conn = sqlite3.connect(os.getenv("DB_LOCATION"))
                cursor = conn.cursor()
                now_str = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
                cursor.execute(f'Update MOVIE SET LastPlayed = "{now_str}" WHERE Filepath = "{movie_filepath}"')
                conn.commit()
                conn.close()
                
                # Update marker
                marker = post_marker #+ timedelta(seconds=1)

                # Stop filling with movies if marker is past the channel end date
                if marker >= channel_end_datetime:
                    break

                # Add movie trailers between show times
                next_showtime = get_next_movie_playtime(marker)
                marker = add_post_movie(channel_number, marker, next_showtime)
                marker = next_showtime

def schedule_motion(channel_number, marker, channel_end_datetime):
    """ Schedules for the Motion Channel """

    # Get total time of channel runtime in seconds for Progress
    total_seconds = (channel_end_datetime - marker).total_seconds()

    # Monitor progress of scheduling for channel
    with Progress() as progress:
        task = progress.add_task("[green]Scheduling motion ...", total=total_seconds)
        completed_time = 0

        while not progress.finished:
            # Select 20 movies weighted on LastPlayed
            selected_movies = select_weighted_movie(["movie"])
            
            # Insert selected movies into the schedule
            for movie in selected_movies:
                movie_filepath = movie[0]
                movie_runtime = movie[3]
                movie_h, movie_m, movie_s = map(int, movie_runtime.split(":"))
                movie_TD = timedelta(hours=movie_h, minutes=movie_m, seconds=movie_s)

                # Update Progress
                completed_time += movie_TD.total_seconds()
                # log.debug(f"{completed_time=}")
                progress.update(task, completed=completed_time)

                post_marker = marker + movie_TD
                insert_into_schedule(channel_number, marker, post_marker, movie_filepath, None, movie_runtime)

                # Update LastPlayed
                conn = sqlite3.connect(os.getenv("DB_LOCATION"))
                cursor = conn.cursor()
                now_str = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
                cursor.execute(f'Update MOVIE SET LastPlayed = "{now_str}" WHERE Filepath = "{movie_filepath}"')
                conn.commit()
                conn.close()
                
                # Update marker
                marker = post_marker #+ timedelta(seconds=1)

                # Stop filling with movies if marker is past the channel end date
                if marker >= channel_end_datetime:
                    break

                # Add movie trailers between show times
                next_showtime = get_next_movie_playtime(marker)
                # log.debug("Adding post movie")
                marker = add_post_movie(channel_number, marker, next_showtime)
                marker = next_showtime

def schedule_channel2(channel_number, marker, channel_end_datetime, tags):
    """ Schedules for the channel2 """

    # Get total time of channel runtime in seconds for Progress
    total_seconds = (channel_end_datetime - marker).total_seconds()

    # List creation
    channel_media = []

    # Gather media by tag
    for tag in tags:
        channel_media.extend([json.loads(data) for table, data in search_database(tag)])

    # Sample 75 items from tag search
    random_media_list = random.sample(channel_media, min(75, len(channel_media)))

    # Build schedule
    while marker < channel_end_datetime:
        # Select random media object
        media = random.choice(random_media_list)
        log.debug(f"{media['Filepath']} - {marker.hour:02}:{marker.minute:02}:{marker.second:02}")

        # Process TV episode
        if "tv" in media["Filepath"]:
            # Calculate episode timedelta
            episode_TD = runtime_to_timedelta(media["Runtime"])

            # Get episode block size and next play time
            episode_block, next_play_time = get_next_tv_playtime(marker, episode_TD)

            # Get chapter metadata
            chapters = get_chapters(media['Filepath'])

            # If Chapters
            if chapters:
                # log.debug(f"Chapters: {len(chapters)}")

                # Calculate max commercial time
                max_commercial_time = get_max_break_time(episode_TD, chapters, episode_block)

                for chapter in chapters:
                    # Get chapter duration
                    chapter_number, chapter_start, chapter_end = chapter
                    chapter_start_TD  = runtime_to_timedelta(chapter_start)
                    chapter_end_TD  = runtime_to_timedelta(chapter_end)
                    chapter_duration = chapter_end_TD - chapter_start_TD

                    # Insert into schedule
                    log.debug(f"Inserting {media['Filepath']} - Chapter {chapter_number}")
                    post_marker = marker + chapter_duration
                    insert_into_schedule(channel_number, marker, post_marker, media["Filepath"], chapter_number, seconds_to_hms(chapter_duration.total_seconds()))
                    marker = post_marker #+ timedelta(seconds=1)

                    # Commercials between chapters
                    if int(chapter_number) < len(chapters):
                        marker = standard_commercial_break(marker, max_commercial_time, channel_number)
                    else:
                        # Commercials post episode
                        log.debug("Final chapter has been played")

                        log.debug(f"Filling commercials from {marker} to {next_play_time}")
                        marker = post_episode(marker, next_play_time, channel_number)
            else:
                # If no chapters are in episode, add episode and fill the rest of the block with commercials
                post_marker = marker + episode_TD
                insert_into_schedule(channel_number, marker, post_marker, media["Filepath"], None, media["Runtime"])
                marker = post_marker #+ timedelta(seconds=1)

                # Pop 'media' from the list
                media_index = random_media_list.index(media)
                random_media_list.pop(media_index)

                # Commercials post episode
                log.debug(f"Filling commercials from {marker} to {next_play_time}")
                marker = post_episode(marker, next_play_time, channel_number)
        else:
            # Process Movie
            movie_TD = runtime_to_timedelta(media["Runtime"])

            # Insert into schedule
            post_marker = marker + movie_TD
            insert_into_schedule(channel_number, marker, post_marker, media["Filepath"], None, media["Runtime"])
            marker = post_marker #+ timedelta(seconds=1)

            # Pop 'media' from the list
            movie_index = random_media_list.index(media)
            random_media_list.pop(movie_index)

            # Get next movie playtime
            next_play_time = get_next_movie_playtime_c2(marker)

            # Fill time with commercials
            log.debug(f"Filling commercials from {marker} to {next_play_time}")
            marker = post_movie(marker, next_play_time, channel_number)

def time_str_to_seconds(time_str):
    """ Converts time formatted string to number of seconds  """
    h, m, s = map(int, time_str.split(":"))
    return h * 3600 + m * 60 + s

def seconds_to_hms(seconds):
    """ Converts number of seconds to time formatted string """
    s = seconds % 60
    m = (seconds//60) % 60
    h = seconds//3600
    return ( "%02d:%02d:%02d" % (h,m,s) )

def format_timedelta(seconds):
    """ Converts seconds to hh:mm:ss format. """
    hours, remainder = divmod(seconds.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    return '{:02}:{:02}:{:02}'.format(int(hours), int(minutes), int(seconds))

def runtime_to_timedelta(runtime):
    """ Convert runtime (datetime) to timedelta """
    h, m, s = map(int, runtime.split(":"))
    runtime_TD = timedelta(hours=h, minutes=m, seconds=s)
    return runtime_TD

def get_max_break_time(episode_TD, chapters, episode_block):
    """
    Calculate max time for commercial breaks

    Args:
        episode_TD (timedelta):  Seconds of episode runtime
        chapters (integer): Total number of chapters in episode
        episode_block (integer): Total number of minutes for episode 

    Returns:
        max_break_time (integer): Rounded number of seconds for max break time

    Raises:
        None

    Example:
        get_max_break_time(episode_TD, chapters, episode_block)
    """
    # Get total amount of time for commercials
    # i.e. 30 minute block - 21 minute episode = 9 minutes for commercials
    total_commercial_time = episode_block - episode_TD
    max_break_time = round((total_commercial_time // len(chapters)).total_seconds(), 0)
    return max_break_time

def standard_commercial_break(marker, max_break_time, channel_number):
    """
    Creates a commercial break between episode chapters

    Args:
        marker (datetime): Where we are in the current schedule timeline
        max_break_time (timedelta):  Seconds of largest possible commercial break time
        channel_number (integer): Channel number

    Returns:
        None

    Raises:
        None

    Example:
        post_episode(next_play_time, channel_number)
    """

    log.debug(f"Standard commercial break - {max_break_time}")

    # Fill with commercials until max_break_time is depleted
    max_break_time = timedelta(seconds=max_break_time)
    while max_break_time > timedelta(seconds=14):
        # Select fitted, random commercial
        commercial = select_commercial(max_break_time)

        # Parse commercial runtime
        comm_h, comm_m, comm_s = map(int, commercial["Runtime"].split(":"))
        comm_TD = timedelta(hours=comm_h, minutes=comm_m, seconds=comm_s)

        # Insert into schedule
        max_break_time -= comm_TD #(comm_TD + timedelta(seconds=1))
        post_marker = marker + comm_TD
        insert_into_schedule(channel_number, marker, post_marker, commercial["Filepath"], None, commercial["Runtime"])
        marker = post_marker  #+ timedelta(seconds=1)
    
    return marker

def post_episode(marker, next_play_time, channel_number):
    """
    Fills the remaining timeblock with commercials post episode

    Args:
        marker (datetime): Where we are in the current schedule timeline
        next_play_time (datetime):  When next item is to play
        channel_number (integer): Channel number

    Returns:
        None

    Raises:
        None

    Example:
        post_episode(next_play_time, channel_number)
    """

    log.debug(f"Post Episode - {marker}")

    # Fill with commercials until filler needs to play - Less than 1 minute until next_play_time
    time_remaining = next_play_time - marker
    if time_remaining > timedelta(minutes=1):
        while time_remaining > timedelta(minutes=1):
            # Select fitted, random commercial
            commercial = select_commercial(time_remaining)
            # log.debug(f"{time_remaining=} - {commercial['Runtime']}")

            # Parse commercial runtime
            comm_h, comm_m, comm_s = map(int, commercial["Runtime"].split(":"))
            comm_TD = timedelta(hours=comm_h, minutes=comm_m, seconds=comm_s)

            # Insert into schedule
            time_remaining -= comm_TD #(comm_TD + timedelta(seconds=1))
            post_marker = marker + comm_TD
            insert_into_schedule(channel_number, marker, post_marker, commercial["Filepath"], None, commercial["Runtime"])
            marker = post_marker  #+ timedelta(seconds=1)
    
    # Add final filler
    marker = add_final_filler(marker, next_play_time, time_remaining, channel_number)

    return marker

def post_movie(marker, next_play_time, channel_number):
    """
    Fills the remaining timeblock with web content post movie

    Args:
        marker (datetime): Where we are in the current schedule timeline
        next_play_time (datetime):  When next item is to play
        channel_number (integer): Channel number

    Returns:
        None

    Raises:
        None

    Example:
        post_movie(next_play_time, channel_number)
    """

    log.debug(f"Post Movie - {marker}")

    # Get time remaining
    time_remaining = next_play_time - marker
    time_remaining_hms = seconds_to_hms(time_remaining.total_seconds())
    log.debug(type(time_remaining_hms))

    # Get all web content
    all_web_media = [json.loads(data) for table, data in search_database("web")]
    random.shuffle(all_web_media)

    for web_media in all_web_media:
        web_h, web_m, web_s = map(int, web_media["Runtime"].split(":"))
        web_TD = timedelta(hours=web_h, minutes=web_m, seconds=web_s)
        if web_TD <= time_remaining:
            time_remaining -= web_TD
            post_marker = marker + web_TD
            insert_into_schedule(channel_number, marker, post_marker, web_media["Filepath"], None, web_media["Runtime"])
            marker = post_marker

    # Add final filler
    marker = add_final_filler(marker, next_play_time, time_remaining, channel_number)
    return marker

def select_weighted_movie(tags):
    """
    Selects a movie, filtered by tags, based on the LastPlayed datetime

    Args:
        tags (list):  Strings of tags in which to search the movie database for

    Returns:
        selected_movies (list): Sample of 20 movies

    Raises:
        None

    Example:
        select_weighted_movie(["movie", "action"])
    """

    # Search database for tags
    all_movies = []
    for tag in tags:
        table, data = zip(*search_database(tag))
        data = [json.loads(d) for d in data]
        for item in data:
            all_movies.append(item)

    # Created weighted list based on LastPlayed datetime
    weighted_list = []
    for movie in all_movies:
        if movie["LastPlayed"]:
            last_played = datetime.strptime(movie["LastPlayed"], "%Y-%m-%d %H:%M:%S")
            time_difference = (datetime.now() - last_played).total_seconds()
            weight = max(time_difference, 1)
        else:
            weight = 10000
        weighted_list.append((movie["Filepath"], weight, movie["LastPlayed"], movie["Runtime"]))

    sorted_list = sorted(weighted_list, key=lambda tup: tup[1], reverse=True)

    # Create sample of 20 movies
    selected_movies = random.sample(sorted_list, 20)

    return selected_movies

def select_commercial(max_break):
    """
    Selects a commercial based on the LastPlayed datetime

    Args:
        max_break (timedelta):  Max time for commercial break

    Returns:
        selected_commercial (list): A single commercial's metadata from the database

    Raises:
        None

    Example:
        select_commercial(max_break)
    """
    # Open the database
    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()

    # Get all commercials
    all_comms = [json.loads(data) for table, data in search_database("commercial") if "filler" not in json.loads(data)["Tags"]]
    # all_comms = []
    # for r in search_database("commercial"):
    #     all_comms
    #     table, data = r
    #     all_comms.append(json.loads(data))

    # Try and find commercials that match max_break
    # log.debug(f"Looking for commercials that match {seconds_to_hms(max_break.total_seconds())}")
    all_comms_fit = [c for c in all_comms if c["Runtime"] == seconds_to_hms(max_break.total_seconds())]
    # log.debug(f"All Comms Fit: {len(all_comms_fit)}")

    # If no exact matches, get all commercials that are less than max_break
    if len(all_comms_fit) == 0:
        # log.debug(f"Could not find a commercial for {seconds_to_hms(max_break.total_seconds())}")
        all_comms = [c for c in all_comms if c["Runtime"] < seconds_to_hms(max_break.total_seconds())]
    # log.debug(f"All Comms: {len(all_comms)}")

    # Weigh all commercials based on LastPlayed
    weighted_list = []
    # {'ID': 1, 'Tags': 'commercial,games', 'Runtime': '00:00:29', 'Filepath': '/media/ascott/USB/bumpers/Games/Gaming-19.mp4', 'LastPlayed': '2025-02-06 15:18:11'}
    for comm in all_comms:
        if comm["LastPlayed"]:
            last_played = datetime.strptime(comm["LastPlayed"], "%Y-%m-%d %H:%M:%S")
            time_difference = (datetime.now() - last_played).total_seconds()
            weight = max(time_difference, 1)
        else:
            weight = 10000

        weighted_list.append((comm["Filepath"], weight, comm["Runtime"]))

    filepaths, weights, runtimes = zip(*weighted_list)
    selected = random.choices(filepaths, weights=weights, k=1)[0]

    # Update LastPlayed with Datetime.Now timestamp
    now_str = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    cursor.execute(f"Update COMMERCIALS SET LastPlayed = '{now_str}' WHERE Filepath = '{selected}'")
    conn.commit()
    conn.close()

    # Return the selected commercial
    selected_commercial = [c for c in all_comms if c["Filepath"] == selected][0]
    return selected_commercial

def add_final_filler(marker, next_play_time, time_remaining, channel_number):
    """
    Adds last filler to a block

    Args:
        next_play_time (string): Next datetime to play media
        time_remaining (timedelta): Seconds of time remaining until next_play_time
        channel_number (integer): Channel number

    Returns:
        None

    Raises:
        None

    Example:
        add_final_filler(marker, next_play_time, time_remaining, channel_number)
    """

    # Add final filler
    # log.debug(f"Final filler with {time_remaining} remaining")
    insert_into_schedule(channel_number, marker, datetime.strftime(next_play_time, "%Y-%m-%d %H:%M:%S"), os.getenv("FILLER_VIDEO"), None, seconds_to_hms(time_remaining.total_seconds()))
    return next_play_time
                

def create_schedule():
    """
    Creates a schedule for all channels

    Args:

    Returns:
        None

    Raises:
        None

    Example:
        create_schedule()
    """
    global marker

    # Clear old items in the schedule
    if check_schedule_for_rebuild():
        # Read in channel json file
        # log.debug("Opening the channel file")
        with open(channel_file, "r") as channel_file_input:
            channel_data = json.load(channel_file_input)

        # Parse channel information
        for channel_metadata in channel_data:
            channel_name = channel_metadata
            channel_number = channel_data[channel_name]["channel_number"]
            channel_commercials = channel_data[channel_name]["commercials"]
            channel_tags = map(str, channel_data[channel_name]["tags"].split(", "))

            log.info(f"Building schedule for {channel_name} - {channel_number}")

            # Set marker and channel end datetime
            marker = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)
            channel_end_datetime = marker + timedelta(days = 1, seconds=1)

            match channel_number:
                case 2:
                    schedule_channel2(channel_number, marker, channel_end_datetime, channel_tags)
                    continue
                case 3:
                    schedule_loud(channel_number, marker, channel_end_datetime)
                    continue
                case 4:
                    schedule_motion(channel_number, marker, channel_end_datetime)
                    continue
                case 5:
                    schedule_bang(channel_number, marker, channel_end_datetime)
                    continue
                case 6:
                    schedule_ppv(channel_number, marker, channel_end_datetime)
                    continue
                case 7:
                    schedule_ppv(channel_number, marker, channel_end_datetime)
                    continue
                case 8:
                    schedule_ppv(channel_number, marker, channel_end_datetime)
                    continue



