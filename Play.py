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
# logging.setLevel(logging.INFO)
log = logging.getLogger("rich")

# MPV
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
    now = datetime.strftime(datetime.now(), "%Y-%m-%d %HH:%MM:%SS")
    cursor.execute(f"SELECT * FROM SCHEDULE WHERE Channel = '{channel_number}' ORDER BY Showtime ASC")
    # cursor.execute(f"SELECT * FROM SCHEDULE WHERE Channel = '{channel_number}' AND Showtime >= '{now}' ORDER BY Showtime ASC")
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
    sObject = {
        "channel": channel,
        "showtime": showtime,
        "end": end,
        "filepath": filepath,
        "chapter": chapter,
        "runtime": runtime
    }
    return sObject

def get_playing_next(channel_number):
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
    sObject = {
        "channel": channel,
        "showtime": showtime,
        "end": end,
        "filepath": filepath,
        "chapter": chapter,
        "runtime": runtime
    }
    return sObject

def get_chapter_start(filepath, chapter_number):
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
    # Get episode ID
    query = f"SELECT ID FROM TV WHERE Filepath = '{filepath}'"
    cursor.execute(query)
    result = cursor.fetchone()
    episode_id = result[0]
    log.info(f"{episode_id=}")

    # Get time after start of current chapter from the database
    query = f"SELECT Start FROM CHAPTERS WHERE ID = {episode_id} ORDER BY Start"  # AND Chapter = {chapter_number}"
    cursor.execute(query)
    # result = cursor.fetchone()
    results = cursor.fetchall()
    log.debug(results)
    chapter_start = result[chapter_number - 1]

    return chapter_start

def monitor_playback(schedule):
    while True:
        try:
            now = datetime.now()

            # Get what should be playing now
            playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]][0]
            playing_now_index = schedule.index(playing_now)
            playing_next = schedule[playing_now_index + 1]
            # log.debug(f"Playback Monitor: {playing_now['filepath']} until {playing_now['end']}")
            # log.debug(f"Playback Monitor: {playing_next['filepath']} until {playing_next['end']}")

            if now >= playing_now["end"]:
                log.debug("I need to go to the next thing!")
                log.debug(f"Schedule says {playing_now['filepath']}")
                if item["chapter"] is not None:
                    log.debug(f"Chapter {item['chapter']} detected")
                    player.playlist_next()
                    chapter_index = (int(item['chapter']) - 1)
                    player.chapter = chapter_index
                    player.wait_for_property("chapter")
                else:
                    player.playlist_next()

        except Exception as e:
            log.debug(f"Playback Monitor: {e}")
        time.sleep(0.1)

def load_channel(channel_number):
    os.system("clear")
    now = datetime.now()

    # Get a list of all items playing on given channel number
    schedule_list = import_schedule(channel_number)

    # DEBUG
    # with open("./schedule_list.txt", "w") as file:
    #     for item in schedule_list:
    #         item_str = f"{item['filepath']} - {item['showtime']} - {item['end']}"
    #         file.write(item_str + "\n")

    # Get what's playing now
    playing_now = [s for s in schedule_list if now >= s["showtime"] and now < s["end"]][0]
    playing_now_index = schedule_list.index(playing_now)
    playing_next = schedule_list[playing_now_index + 1]
    remaining_schedule = schedule_list[playing_now_index:]

    # DEBUG
    # with open("./r_schedule_list.txt", "w") as file:
    #     for item in remaining_schedule:
    #         item_str = f"{item['filepath']} - {item['showtime']} - {item['end']}"
    #         file.write(item_str + "\n")

    # Add remaining items in schedule to MPV playlist
    for item in remaining_schedule:
        # If currently playing
        if now >= item["showtime"]:
            player.playlist_append(item["filepath"])

            # If this is the first item in the playlist, start playing
            if player.playlist_count == 1:
                # Start playing at proper time if episode has chapters
                log.debug(item["chapter"])
                if item["chapter"] is None:
                    log.debug(f"Should be playing {item['filepath']} at {item['showtime']} until {item['end']}")
                    elapsed_time = (now - item["showtime"]).total_seconds()
                    player.play(item["filepath"])
                    log.debug("Waiting for seekable property to be available")
                    player.wait_for_property('seekable')
                    log.debug(f"Seeking to {elapsed_time}")
                    player.seek(elapsed_time, reference="absolute")
                else:
                    log.debug(f"Chapter {item['chapter']} detected")
                    player.play(item["filepath"])
                    chapter_index = (int(item['chapter']) - 1)
                    player.chapter = chapter_index
                    player.wait_for_property("chapter")
                    elapsed_time = (now - item["showtime"]).total_seconds()
                    log.debug(f"Seeking to {elapsed_time}")
                    player.seek((now - item["showtime"]).total_seconds(), reference="absolute")

                    # # Get time into episode depending on chapter number
                    # # Subtract now - item["showtime"] - i.e. 2:05
                    # time_into_chapter = (now - item["showtime"]).total_seconds()
                    # log.debug(f"{time_into_chapter=}")
                    # # Chapter start time + results above - i.e. 7:30 + 2:05 = 9:35 into episode
                    # chapter_hour, chapter_minute, chapter_second = map(int, get_chapter_start_time(item["filepath"], item["chapter"]).split(":"))
                    # chapter_TD = timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second).total_seconds()
                    # elapsed_time = chapter_TD + time_into_chapter
                    # log.debug(f"{elapsed_time=}")
                    # time.sleep(5)

                
        else:
            player.playlist_append(item["filepath"])

    # DEBUG
    # with open("./pl_files.txt", "w") as file:
    #     for item in player.playlist_filenames:
    #         file.write(item + "\n")

    # Start playback monitoring function thread
    monitor_thread = threading.Thread(target=monitor_playback, args=(remaining_schedule,))
    monitor_thread.daemon = True
    monitor_thread.start()


# Schedule.clear_schedule_table()
if Schedule.check_schedule_for_rebuild(2):
    Schedule.clear_schedule_table()
    Schedule.create_schedule()
else:
    Schedule.clear_old_schedule_items()

# Load Channel
load_channel(2)

# Main Playback Loop
while player.time_pos is not None:
    time.sleep(1)

# def load_channel(channel_number):
#     os.system("clear")

#     # Prep for first run
#     playing_now = get_playing_now(channel_number)
#     playing_next = get_playing_next(channel_number)
#     log.info(f"Playing {playing_now['filepath']}")

#     player.play(playing_now["filepath"])

#     # Calculate seconds into playing_now
#     if not playing_now["chapter"] == None:
#         # Get chapter start from database
#         chapter_start = get_chapter_start(playing_now['filepath'], playing_now['chapter'])
#         log.info(f"Chapter Start: {chapter_start}")

#         chapter_hour, chapter_minute, chapter_second = map(int, chapter_start.split(":"))
#         chapter_start_TD = timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second)
#         elapsed_time = ((datetime.now() - datetime.strptime(playing_now["showtime"], "%Y-%m-%d %H:%M:%S")) + chapter_start_TD).total_seconds()
#         log.info(f"Seeking to {elapsed_time} seconds")
#     else:
#         elapsed_time = (datetime.now() - datetime.strptime(playing_now["showtime"], "%Y-%m-%d %H:%M:%S")).total_seconds()
#         log.info(f"Seeking to {elapsed_time} seconds")

#     # Use MPV Start command to force starting the video at a specific time    
#     player.start = elapsed_time

#     # Video Loop
#     while player.time_pos is None:
#         time.sleep(0.1)
#     while player.time_pos is not None:
#         os.system("clear")

#         if player.pause:
#             player.pause = False

#         # Check current time
#         now = datetime.now()

#         # Terminal stats
#         time_pos_hours, remainder = divmod(player.time_pos, 3600)
#         time_pos_minutes, time_pos_seconds = divmod(remainder, 60)
#         time_pos_str = f"{int(time_pos_hours):02}:{int(time_pos_minutes):02}:{int(time_pos_seconds):02}"
#         duration_hours, remainder = divmod(player.duration, 3600)
#         duration_minutes, duration_seconds = divmod(remainder, 60)
#         duration_str = f"{int(duration_hours):02}:{int(duration_minutes):02}:{int(duration_seconds):02}"

#         log.info(f"{time_pos_str}/{duration_str}")
#         log.info(f"Paused: {player.pause}")
#         log.info("")
#         log.info(f"Up Next: {playing_next['filepath']} starting at {playing_next['showtime']}")
#         log.info(playing_now)

#         # Break loop if end time has been passed
#         # if now >= datetime.strptime(playing_now["end"], "%Y-%m-%d %H:%M:%S"):
#         #     break
#         if time_pos_str == duration_str:
#             break
#         time.sleep(1)

# while True:
#     load_channel(2)