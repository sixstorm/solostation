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
# Ensure the socket path does not already exist
socket_path = "/tmp/mpv_socket"
if os.path.exists(socket_path):
    os.remove(socket_path)
player = mpv.MPV(
    ytdl=True, 
    input_default_bindings=True, 
    input_vo_keyboard=True,
    keep_open=True,
    sub="no",
    input_ipc_server=socket_path
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

    conn2 = sqlite3.connect("/media/ascott/USB/database/solodb.db")
    cursor2 = conn2.cursor()

    # Get episode ID
    query = f"SELECT ID FROM TV WHERE Filepath = '{filepath}'"
    cursor2.execute(query)
    result = cursor2.fetchone()
    episode_id = result[0]
    log.info(f"{episode_id=}")

    # Get time after start of current chapter from the database
    query = f"SELECT Start FROM CHAPTERS WHERE EpisodeID = {episode_id} AND Title = {chapter_number}"
    cursor2.execute(query)
    chapter_start = cursor2.fetchone()[0]
    log.debug(f"{chapter_start=}")

    conn2.close()

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

def reset_channel(channel_number):
    now = datetime.now()
    playable = False

    # Clear playlist
    player.playlist_clear()

    # Get schedule for channel
    schedule = import_schedule(channel_number)

    # Get playing now
    playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]][0]
    playing_now_index = schedule.index(playing_now)

    # Get playing next if possible
    try:
        playing_next = schedule[playing_now_index + 1]
    except IndexError:
        playing_next = None

    # Get remaining schedule
    remaining_schedule = schedule[playing_now_index:]
    log.debug(f"There are {len(remaining_schedule)} items in the remaining schedule")
    time.sleep(2)

    for item in remaining_schedule:
        if now >= item["showtime"]:
            # Append to playlist
            player.playlist_append(item["filepath"])

            if player.playlist_count == 1:
                # Start playing 
                player.play(item["filepath"])

                # Wait until playable
                while not playable:
                    try:
                        duration = player.duration
                        if duration is not None:
                            playable = True
                    except Exception as e:
                        log.debug(f"Error waiting on duration check: {e}")
                        time.sleep(0.5)

                # If chapter is available
                if item["chapter"] is not None:
                    chapter_start = get_chapter_start_time(item["filepath"], item["chapter"])
                    time_gap = datetime.now() - item["showtime"]
                    log.debug(f"{time_gap=}")
                    chapter_hour, chapter_minute, chapter_second = map(int, chapter_start.split(":"))
                    elapsed_time = (time_gap + timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second)).total_seconds()
                else:
                    elapsed_time = (now - item["showtime"]).total_seconds()

                if elapsed_time > 0:
                    player.wait_for_property("seekable")
                    player.seek(elapsed_time, reference="absolute")
        else:
            player.playlist_append(item["filepath"])

def load_schedule(current_channel):
    now = datetime.now()
    playable = False

    # Clear playlist
    player.playlist_clear()

    # Get schedule for channel
    schedule = import_schedule(current_channel)

    # Get playing now
    playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]][0]
    playing_now_index = schedule.index(playing_now)

    # Get playing next if possible
    try:
        playing_next = schedule[playing_now_index + 1]
    except IndexError:
        playing_next = None

    # Get remaining schedule
    remaining_schedule = schedule[playing_now_index:]
    log.debug(f"There are {len(remaining_schedule)} items in the remaining schedule")

    for item in remaining_schedule:
        player.playlist_append(item["filepath"])

def on_path_change(_name, value):
    global schedule, current_channel, initial_path_update_ignored

    log.debug(f"MPV File Observer: Changed file: {value}")
    now = datetime.now().replace(microsecond=0)
    playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]][0]
    log.debug(f"{initial_path_update_ignored=}")

    # Don't do anything on the first run unless item playing has chapters
    if not initial_path_update_ignored:
        if playing_now["chapter"] is not None:
            initial_path_update_ignored = True
            while now < playing_now["end"]:
                now = datetime.now().replace(microsecond=0)
                time.sleep(0.1)
            log.debug("MPV File Observer: Going to next item in playlist post chapter")
            player.playlist_next()
        else:
            log.debug("MPV File Observer: Ignoring first run for path change")
            initial_path_update_ignored = True
    else:
        try:
            # If chapter, wait until end time and move forward
            if playing_now["chapter"] is not None:
                log.debug(f"MPV File Observer: Playing chapter {playing_now['chapter']} of {playing_now['filepath']} at {playing_now['showtime']} until {playing_now['end']}")
                chapter_start = get_chapter_start_time(playing_now["filepath"], playing_now["chapter"])
                chapter_hour, chapter_minute, chapter_second = map(int, chapter_start.split(":"))
                time_gap = datetime.now() - playing_now["showtime"]
                log.debug(f"MPV File Observer: Time Gap {time_gap}")
                elapsed_time = (time_gap + timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second)).total_seconds()
                player.wait_for_property("seekable")
                player.seek(elapsed_time, reference="absolute")

                log.debug(f"MPV File Observer: Playing until {playing_now['end']}")
                while now < playing_now["end"]:
                    now = datetime.now().replace(microsecond=0)
                    time.sleep(0.1)
                log.debug("End of chapter!")

                # Get what is playing next, if possible
                try:
                    playing_now_index = schedule.index(playing_now)
                    playing_next = schedule[playing_now_index + 1]
                    log.debug(f"Post Chapter Play Next: {playing_next}")
                    player.playlist_next()
                    # if playing_next["chapter"] is not None:
                    #     log.debug("2 chapters were detected together; doing nothing")
                    # else:
                    #     player.playlist_next()
                except IndexError:
                    playing_next = None

            # Filler killer
            elif "Filler" in value:
                log.debug("Filler detected")
                now = datetime.now().replace(microsecond=0)
                playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]][0]
                while now < playing_now["end"]:
                    now = datetime.now().replace(microsecond=0)
                    time.sleep(0.1)
                log.debug("Killing filler!")
                player.playlist_next()

            else:
                now = datetime.now().replace(microsecond=0)
                playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]][0]
                while now < playing_now["end"]:
                    now = datetime.now().replace(microsecond=0)
                    time.sleep(0.1)

        except Exception as e:
            log.debug(f"MPV File Observer Error: {e}")
    log.debug("End of path change function")

##############################################

# MediaManager.initialize_all_tables()
# MediaManager.process_tv()

initial_path_update_ignored = False

if Schedule.check_schedule_for_rebuild(2):
    Schedule.clear_old_schedule_items()
    Schedule.create_schedule()
else:
    Schedule.clear_old_schedule_items()

# MPV Property Monitoring
current_channel = 2
schedule = import_schedule(current_channel)

# Load Channel
# load_channel(current_channel)

player.observe_property("path", on_path_change)

# Main Playback Loop
reset_channel(current_channel)
while True:
    time.sleep(1)

# while True:
#     log.debug("Starting main loop")
#     playable = False

#     while not playable:
#         try:
#             duration = player.duration
#             if duration is not None:
#                 playable = True
#         except Exception as e:
#             time.sleep(0.5)

#     # Will continue in this loop unless reset
#     while player.time_pos is not None:
#         now = datetime.now().replace(microsecond=0)
#         playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]][0]

#         # If chapter is available
#         if playing_now["chapter"] is not None:
#             chapter_start = get_chapter_start_time(playing_now["filepath"], playing_now["chapter"])
#             time_gap = datetime.now() - playing_now["showtime"]
#             log.debug(f"{time_gap=}")
#             chapter_hour, chapter_minute, chapter_second = map(int, chapter_start.split(":"))
#             elapsed_time = (time_gap + timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second)).total_seconds()
#         else:
#             elapsed_time = (now - playing_now["showtime"]).total_seconds()

#         if elapsed_time > 0:
#             player.wait_for_property("seekable")
#             player.seek(elapsed_time, reference="absolute")

#         # If currently playing item has a chapter, set force_out to True
#         if playing_now["chapter"] is not None:
#             force_out = True
#             log.debug(f"{force_out=}")
#         else:
#             force_out = False

#         # Wait until end of playing_now media item
#         while now <= playing_now["end"]:
#             now = datetime.now().replace(microsecond=0)
#             time.sleep(0.1)
#         log.debug("End reached")

#         # Force next item in playlist if a chapter
#         # Otherwise, allow playlist to play
#         log.debug("Going to next item")
#         if force_out:
#             log.debug("FORCED OUT")
#             player.playlist_next()
#             player.playlist_remove(0)
#         log.debug("Resetting")