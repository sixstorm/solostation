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
log = logging.getLogger("rich")

# Ensure the socket path does not already exist
socket_path = "/tmp/mpv_socket"
if os.path.exists(socket_path):
    os.remove(socket_path)

# MPV Player Creation and Properties
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

# Vars
solo_db = "/media/ascott/USB/database/solodb.db"

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

    # Open Database
    conn = sqlite3.connect(solo_db)
    cursor = conn.cursor()

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

    conn.close()

    return all_scheduled_items

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

    conn2 = sqlite3.connect(solo_db)
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

# One loop
# Each loop equals 1 time slot

current_channel = 2

if Schedule.check_schedule_for_rebuild(current_channel):
    Schedule.clear_old_schedule_items()
    Schedule.create_schedule()
else:
    Schedule.clear_old_schedule_items()


while True:
    channel_changed = False
    playable = False

    # Import schedule
    schedule = import_schedule(current_channel)

    while not channel_changed:
        now = datetime.now().replace(microsecond=0)

        # Get playing now
        playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]][0]
        log.debug(f"Currently playing {playing_now['filepath']} until {playing_now['end']}")

        # Get what is playing next, if possible
        try:
            playing_now_index = schedule.index(playing_now)
            playing_next = schedule[playing_now_index + 1]
        except IndexError:
            playing_next = None

        # Start playback
        player.play(playing_now["filepath"])
        while not playable:
            try:
                duration = player.duration
                if duration is not None:
                    playable = True
            except Exception as e:
                time.sleep(0.5)

        # Chapter
        if playing_now["chapter"] is not None:
            log.debug(f"Chapter: {playing_now['chapter']}")
            chapter_start_time = get_chapter_start_time(playing_now["filepath"], playing_now["chapter"])
            time_gap = datetime.now() - playing_now["showtime"]
            chapter_hour, chapter_minute, chapter_second = map(int, chapter_start_time.split(":"))
            elapsed_time = time_gap + timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second)
        else:
            elapsed_time = (now - playing_now["showtime"]).total_seconds()

        if elapsed_time > 0:
            log.debug(f"{elapsed_time=}")
            player.wait_for_property("seekable")
            player.seek(elapsed_time, reference="absolute")

        while now < playing_now["end"]:
            now = datetime.now().replace(microsecond=0)
            time.sleep(0.1)

        # Playback has ended

