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
    sub="no",
    idle=False
)

player["vo"] = "gpu"
player["hwdec"] = "drm-copy"
player.fullscreen = True

# Open Database
conn = sqlite3.connect("/media/ascott/USB/database/solodb.db")
cursor = conn.cursor()

# Functions
def import_schedule():
    cursor.execute("SELECT * FROM SCHEDULE")
    allScheduledItems = cursor.fetchall()

    scheduleList = []
    for item in allScheduledItems:
        ID, ChannelNumber, Showtime, End, Filepath, Chapter, Runtime = item

        scheduleList.append({
            "channel": ChannelNumber,
            "showtime": datetime.strptime(Showtime, "%Y-%m-%d %H:%M:%S"),
            "end": datetime.strptime(End, "%Y-%m-%d %H:%M:%S"),
            "filepath": Filepath,
            "chapter": Chapter,
            "runtime": Runtime
        })

    return scheduleList

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

def load_channel(channel_number):
    os.system("clear")

    # Prep for first run
    playing_now = get_playing_now(channel_number)
    playing_next = get_playing_next(channel_number)
    log.info(f"Playing {playing_now['filepath']}")

    player.play(playing_now["filepath"])

    # Calculate seconds into playing_now
    if not playing_now["chapter"] == None:
        # Get chapter start from database
        chapter_start = get_chapter_start(playing_now['filepath'], playing_now['chapter'])
        log.info(f"Chapter Start: {chapter_start}")

        chapter_hour, chapter_minute, chapter_second = map(int, chapter_start.split(":"))
        chapter_start_TD = timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second)
        elapsed_time = ((datetime.now() - datetime.strptime(playing_now["showtime"], "%Y-%m-%d %H:%M:%S")) + chapter_start_TD).total_seconds()
        log.info(f"Seeking to {elapsed_time} seconds")
    else:
        elapsed_time = (datetime.now() - datetime.strptime(playing_now["showtime"], "%Y-%m-%d %H:%M:%S")).total_seconds()
        log.info(f"Seeking to {elapsed_time} seconds")

    # Use MPV Start command to force starting the video at a specific time    
    player.start = elapsed_time

    # Video Loop
    while player.time_pos is None:
        time.sleep(0.1)
    while player.time_pos is not None:
        os.system("clear")

        if player.pause:
            player.pause = False

        # Check current time
        now = datetime.now()

        # Terminal stats
        time_pos_hours, remainder = divmod(player.time_pos, 3600)
        time_pos_minutes, time_pos_seconds = divmod(remainder, 60)
        time_pos_str = f"{int(time_pos_hours):02}:{int(time_pos_minutes):02}:{int(time_pos_seconds):02}"
        duration_hours, remainder = divmod(player.duration, 3600)
        duration_minutes, duration_seconds = divmod(remainder, 60)
        duration_str = f"{int(duration_hours):02}:{int(duration_minutes):02}:{int(duration_seconds):02}"

        log.info(f"{time_pos_str}/{duration_str}")
        log.info(f"Paused: {player.pause}")
        log.info("")
        log.info(f"Up Next: {playing_next['filepath']} starting at {playing_next['showtime']}")
        log.info(playing_now)

        # Break loop if end time has been passed
        # if now >= datetime.strptime(playing_now["end"], "%Y-%m-%d %H:%M:%S"):
        #     break
        if time_pos_str == duration_str:
            break
        time.sleep(1)



while True:
    load_channel(2)