import MediaManager
import Schedule
import logging
from datetime import datetime, timedelta
import time
import sqlite3
import os
import mpv
import re
from PIL import Image, ImageDraw, ImageFont
import threading

from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler
from rich.text import Text

# Rich log
FORMAT = "%(message)s"
logging.basicConfig(
    level="DEBUG", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("rich")

# MPV Player Creation and Properties
player = mpv.MPV(
    ytdl=True, 
    input_default_bindings=True, 
    input_vo_keyboard=True,
    keep_open=True,
    sub="no"
)

player["vo"] = "gpu"
player["hwdec"] = "drm-copy"
player.fullscreen = True

# Open Database
conn = sqlite3.connect("/media/ascott/USB/database/solodb.db")
cursor = conn.cursor()

# Functions
def import_schedule(channel_number):
    '''
    Queries the schedule in the database for all items with given channel number.
    All results are converted to a dictionary and returned in a list.

    Args:
        channel_number (int) - Channel number

    Returns:  
        all_scheduled_items (list of dictionaries) - Each scheduled item
        
    Raises:
    Example:
    '''

    # Query schedule in database for given channel number 
    now = datetime.strftime(datetime.now(), "%Y-%m-%d %HH:%MM:%SS")
    cursor.execute(f"SELECT * FROM SCHEDULE WHERE Channel = '{channel_number}' ORDER BY Showtime ASC")

    # Convert query results to a list of dictionaries
    all_scheduled_items = [{
            "channel": row[1],
            "showtime": datetime.strptime(str(row[2]), "%Y-%m-%d %H:%M:%S"),
            "end": datetime.strptime(str(row[3]), "%Y-%m-%d %H:%M:%S"),
            "filepath": row[4],
            "chapter": row[5],
            "runtime": row[6]
    } for row in cursor.fetchall()]

    return all_scheduled_items

def get_playing_now(channel_number):
    '''
    Queries the schedule in the database for what is currently playing

    Args:
        channel_number (int) - Channel number

    Returns:  
        s_object (dict) - Single scheduled item
        
    Raises:
    Example:
    '''

    # Get what's playing now
    query = """
        SELECT * FROM SCHEDULE
        WHERE Showtime <= ? AND End > ?
        ORDER BY Showtime DESC
        LIMIT 1;
    """
    cursor.execute(query, (datetime.now(), datetime.now()))
    results = cursor.fetchone()

    # Convert to a custom object and return it
    ID, channel, showtime, end, filepath, chapter, runtime = results
    s_object = {
        "channel": channel,
        "showtime": showtime,
        "end": end,
        "filepath": filepath,
        "chapter": chapter,
        "runtime": runtime
    }
    return s_object

def get_playing_next(channel_number):
    '''
    Queries the schedule in the database for what is playing next

    Args:
        channel_number (int) - Channel number

    Returns:  
        s_object (dict) - Single scheduled item
        
    Raises:
    Example:
    '''
    # Get what's playing next
    query = """
        SELECT * FROM SCHEDULE
        WHERE Showtime > ?
        ORDER BY Showtime ASC
        LIMIT 1;
    """
    cursor.execute(query, (datetime.now(),))
    results = cursor.fetchone()

    # Convert to a custom object and return it
    ID, channel, showtime, end, filepath, chapter, runtime = results
    s_object = {
        "channel": channel,
        "showtime": showtime,
        "end": end,
        "filepath": filepath,
        "chapter": chapter,
        "runtime": runtime
    }
    return s_object

def get_chapter_start(filepath, chapter_number):
    '''
    Queries the schedule in the database for the start time of what is currently
    playing

    Args:
        filepath (str) - Filepath of episode
        channel_number (int) - Channel number

    Returns:  
        chapter_start (str) - Start time of chapter
        
    Raises:
    Example:
    '''

    # Get episode ID
    query = f"SELECT ID FROM TV WHERE Filepath = '{filepath}'"
    cursor.execute(query)
    result = cursor.fetchone()
    episode_id = result[0]
    log.info(f"{episode_id=}")

    # Get chapter from the database
    query = f"SELECT Start FROM CHAPTERS WHERE ID = {episode_id}"
    cursor.execute(query)
    result = cursor.fetchone()
    chapter_start = result[0]

    return chapter_start

def get_chapter_start_time(filepath, chapter_number):
    '''
    Queries the schedule in the database for the start time of what is currently
    playing

    Args:
        filepath (str) - Filepath of episode
        channel_number (int) - Channel number

    Returns:  
        chapter_start (str) - Start time of chapter
        
    Raises:
    Example:
    '''

    # Get episode ID
    query = f"SELECT ID FROM TV WHERE Filepath = '{filepath}'"
    cursor.execute(query)
    result = cursor.fetchone()
    episode_id = result[0]
    log.info(f"{episode_id=}")

    # Get time after start of current chapter from the database
    query = f"SELECT Start FROM CHAPTERS WHERE EpisodeID = {episode_id} AND Title = {chapter_number}"
    cursor.execute(query)
    chapter_start = cursor.fetchone()[0]
    # chapter_start = cursor.fetchall()
    log.debug(f"{chapter_start=}")

    return chapter_start

def get_chapter_end_time(filepath, chapter_number):
    '''
    Queries the schedule in the database for the end time of what is currently
    playing

    Args:
        filepath (str) - Filepath of episode
        chapter_number (int) - Channel number

    Returns:  
        chapter_end (str) - End time of chapter
        
    Raises:
    Example:
    '''

    # Get episode ID
    query = f"SELECT ID FROM TV WHERE Filepath = '{filepath}'"
    cursor.execute(query)
    result = cursor.fetchone()
    episode_id = result[0]
    log.info(f"{episode_id=}")

    # Get time after start of current chapter from the database
    query = f"SELECT End FROM CHAPTERS WHERE EpisodeID = {episode_id} AND Title = {chapter_number}"
    cursor.execute(query)
    chapter_end = cursor.fetchone()[0]

    return chapter_end

def monitor_playback(schedule):
    '''
    Threaded function that monitors what is playing.
    If it's time to move forward, then send a command to go to 
    next item in the playlist.

    Args:
        schedule (list) - Current schedule

    Returns:  
        None
        
    Raises:
    Example:
    '''

    log.debug("Playback monitor running")

    while True:
        playable = False

        try:
            now = datetime.now()

            # Get what should be playing now
            playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]][0]
            if not playing_now:
                time.sleep(0.1)
                continue

            # Get what is playing next, if possible
            try:
                playing_now_index = schedule.index(playing_now)
                playing_next = schedule[playing_now_index + 1]
            except IndexError:
                playing_next = None

            # Wait until playing_now end time has come
            while datetime.now() < playing_now["end"]:
                time.sleep(0.1)

            # Go to next item in playlist
            if playing_next:
                log.debug("Playback Monitor: Time for next playlist item")

                if playing_next["chapter"] is not None:
                    # Chapters all around should start at 1, not 0.
                    log.debug(f"Should be playing chapter {playing_next['chapter']} of {playing_next['filepath']} at {playing_next['showtime']} until {playing_next['end']}")
                    chapter_index = int(playing_next['chapter'])
                    log.debug(f"{chapter_index=}")
                    
                    player.playlist_next()

                    while not playable:
                        try:
                            duration = player.duration
                            if duration is not None:
                                playable = True
                        except Exception as e:
                            log.debug(f"Playback Monitor: Duration Check: {e}")
                            time.sleep(0.5)

                    # Go to proper chapter
                    player.start = f"#{chapter_index}"
                else:
                    # Forces episode to stop playing and go to the next item
                    player.playlist_next()

        except Exception as e:
            log.debug(f"Playback Monitor: {e}")
            time.sleep(1)


def load_channel(channel_number):
    '''
    Loads the given channel (by number) media contents into a playlist for MPV to play

    Args:
        channel_number (int) - Channel number

    Returns:  
        None
        
    Raises:
    Example:
    '''
    # os.system("clear")
    now = datetime.now()
    playable = False
    chapter_ready = False

    # Clear playlist
    player.playlist_clear()

    # Get a list of all items playing on given channel number
    schedule_list = import_schedule(channel_number)

    # Get what's playing now
    playing_now = [s for s in schedule_list if now >= s["showtime"] and now < s["end"]][0]
    playing_now_index = schedule_list.index(playing_now)
    playing_next = schedule_list[playing_now_index + 1]
    remaining_schedule = schedule_list[playing_now_index:]

    # Add remaining items in schedule to MPV playlist
    for item in remaining_schedule:
        # If currently playing item
        if now >= item["showtime"]:
            player.playlist_append(item["filepath"])

            # If this is the first item in the playlist, start playing
            if player.playlist_count == 1:
                player.play(item["filepath"])

                # Start playing at proper time
                if item["chapter"] is None:
                    # No chapters
                    # log.debug(f"Should be playing {item['filepath']} at {item['showtime']} until {item['end']}")

                    # Wait for duration property to be available to avoid MPV playback errors
                    while not playable:
                        try:
                            duration = player.duration
                            if duration is not None:
                                playable = True
                        except Exception as e:
                            log.debug(f"Error waiting on chapter playback: {e}")
                            time.sleep(0.5)

                    # Get elapsed time
                    elapsed_time = (now - item["showtime"]).total_seconds()

                    # Seek to the proper time once 'seekable' property is available
                    if elapsed_time > 0:
                        # log.debug("Waiting for seekable property to be available")
                        # log.debug(f"Seeking to {elapsed_time}")
                        player.wait_for_property("seekable")
                        player.seek(elapsed_time, reference="absolute")
                else:
                    # With chapters
                    # log.debug(f"Should be playing chapter {item['chapter']} of {item['filepath']} at {item['showtime']} until {item['end']}")
                    chapter_index = int(item['chapter'])
                    log.debug(f"{chapter_index=}")

                    while not playable:
                        try:
                            duration = player.duration
                            if duration is not None:
                                playable = True
                        except Exception as e:
                            log.debug(f"Error waiting on duration check: {e}")
                            time.sleep(0.5)

                    # Seek to proper playback position in episode
                    # Logic:  chapter_start + (now - item["showtime"])
                    chapter_start = get_chapter_start_time(item["filepath"], item["chapter"])
                    time_gap = datetime.now() - item["showtime"]
                    log.debug(f"{time_gap=}")
                    chapter_hour, chapter_minute, chapter_second = map(int, chapter_start.split(":"))
                    elapsed_time = time_gap + timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second)
                    
                    # log.debug("Waiting for seekable property to be available")
                    # log.debug(f"Seeking to {elapsed_time}")
                    player.seek(elapsed_time, reference="absolute")
               
        else:
            # If not the currently playing item, append to the playlist
            player.playlist_append(item["filepath"])

    log.debug("Channel loaded")

    # Start playback monitoring function thread
    monitor_thread = threading.Thread(target=monitor_playback, args=(remaining_schedule,))
    monitor_thread.daemon = True
    monitor_thread.start()

def on_path_change(_name, value):
    global schedule_list, current_channel

    try:
        now = datetime.now()
        playing_now = [s for s in schedule_list if now >= s["showtime"] and now < s["end"]][0]
        log.debug(f"MPV File Observer: Changed file")

        if value != playing_now["filepath"]:
            log.debug("MPV Mismatch!")
            log.debug(f"MPV: {value}")
            log.debug(f'Schedule: {playing_now["filepath"]}')
            log.debug("Calling to reload the channel")
            load_channel(current_channel)
        else:
            log.debug(f"MPV File Observer: {playing_now['filepath']} is on schedule")

        # Handle episodes with chapters
        if playing_now["chapter"] is not None:
            log.debug(f"MPV File Observer: Playing chapter {playing_now['chapter']} of {playing_now['filepath']} at {playing_now['showtime']} until {playing_now['end']}")

    except Exception as e:
        log.debug(f"MPV File Observer Error: {e}")


def on_chapter_change():
    global first_chapter_run

    if not first_chapter_run:
        log.debug("A chapter changed in MPV")
        player.playlist_next()
    else:
        log.debug("First chapter run")

def pbtime(name, value):
    now = datetime.now()
    playing_now = [s for s in schedule_list if now >= s["showtime"] and now < s["end"]][0]

    # Format playback-time to HH:MM:SS
    hours, remainder = divmod(value, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_time = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
    
    # Show playback-time on the screen
    player.command("show-text", f"{formatted_time}")

# MediaManager.initialize_all_tables()
# MediaManager.process_tv()

if Schedule.check_schedule_for_rebuild(2):
    Schedule.clear_old_schedule_items()
    Schedule.create_schedule()
else:
    Schedule.clear_old_schedule_items()

# MPV Property Monitoring
first_chapter_run = True
current_channel = 2
schedule_list = import_schedule(current_channel)

# Load Channel
load_channel(current_channel)

player.observe_property("path", on_path_change)
# player.observe_property("chapter", on_chapter_change())
# player.observe_property("playback-time", pbtime)

# Main Playback Loop
while True:
    time.sleep(1)