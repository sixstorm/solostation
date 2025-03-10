import mediamanager
import schedule
import logging
from datetime import datetime, timedelta
import time
import sqlite3
import os
import mpv
import re
import json
import threading
from PIL import Image, ImageDraw, ImageFont
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler
from rich.text import Text
from rich.progress import Progress

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
    input_ipc_server=socket_path,
    vo="gpu",
    hwdec="rpi"
)

player.fullscreen = True

# Vars
solo_db = os.getenv("DB_LOCATION")
channel_file = os.getenv("CHANNEL_FILE")

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
    cursor.execute(f"SELECT * FROM TESTSCHEDULE WHERE Channel = '{channel_number}' ORDER BY Showtime ASC")

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
    # log.info(f"{episode_id=}")

    # Get time after start of current chapter from the database
    query = f"SELECT Start FROM CHAPTERS WHERE EpisodeID = {episode_id} AND Title = {chapter_number}"
    cursor2.execute(query)
    chapter_start = cursor2.fetchone()[0]
    # log.debug(f"{chapter_start=}")

    conn2.close()

    return chapter_start

def update_schedule_check():
    while True:
        time.sleep(60)
        log.debug("Checking for schedule extension updates")

        # Get all channel numbers
        with open(channel_file, "r") as channel_file_input:
            channel_data = json.load(channel_file_input)
        channel_numbers = [channel_data[n]["channel_number"] for n in channel_data]

        # Check schedule to see if there is anything scheduled in the next 6 hours
        extension_needed = False
        future_date = datetime.now() + timedelta(hours=14)
        log.debug(f"{future_date=}")

        for num in channel_numbers:
            live_schedule = import_schedule(num)
            last_playing = sorted([s for s in live_schedule if s["channel"] == num], key=lambda x: x["end"], reverse=True)

            if future_date > last_playing[0]["end"]:
                log.debug(f"Channel {num} needs extended scheduling")
                extension_needed = True
            else:
                log.debug(f"Channel {num} does not need extended scheduling")

        if extension_needed:
            schedule.create_schedule(extension_needed)

   

@player.on_key_press("w")
def listen_for_channel_change():
    global current_channel, channel_changed

    current_channel += 1
    if current_channel > 8:
        current_channel = 2
    channel_changed = True

@player.on_key_press("s")
def listen_for_channel_change():
    global current_channel, channel_changed

    current_channel -= 1
    if current_channel < 2:
        current_channel = 8
    channel_changed = True

#############
# Clear out old scheduled items
# schedule.clear_old_schedule_items()
log.debug(schedule.check_schedule_for_rebuild())

# If schedule needs to be built, build it
if schedule.check_schedule_for_rebuild():
    extension_needed = False
    schedule.create_schedule(extension_needed)

log.debug("Starting update thread")
update_thread = threading.Thread(target=update_schedule_check)
# update_thread.start()

# Channel number to start on
current_channel = 2

# Main loop
while True:
    now = datetime.now().replace(microsecond=0)
    channel_changed = False
    playable = False

    # Import schedule
    live_schedule = import_schedule(current_channel)
    log.debug(f"{live_schedule[0]}")

    # Inner channel loop
    while not channel_changed:
        # Get playing now and playing next
        playing_now = [s for s in live_schedule if now >= s["showtime"] and now < s["end"]][0]
        # log.debug(f"{playing_now}")

        last_playing = sorted([s for s in live_schedule if s["channel"] == current_channel], key=lambda x: x["end"])
        # log.debug(f"{last_playing}")

        try:
            playing_now_index = live_schedule.index(playing_now)
            playing_next = live_schedule[playing_now_index + 1]
        except IndexError:
            playing_next = None

        # Start playback
        player.play(playing_now["filepath"])

        # Wait until duration and seekable properties are ready, signaling that the file is properly loaded
        while not playable:
            if player.duration is not None and player.seekable:
                playable = True
                time.sleep(0.1)

        if playing_now["chapter"] is not None:
            chapter_start_time = get_chapter_start_time(playing_now["filepath"], playing_now["chapter"])
            time_gap = datetime.now() - playing_now["showtime"]
            chapter_hour, chapter_minute, chapter_second = map(int, chapter_start_time.split(":"))
            elapsed_time = (time_gap + timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second)).total_seconds()
        else:
            elapsed_time = (now - playing_now["showtime"]).total_seconds()

        elapsed_time = max(0, elapsed_time)
        if elapsed_time > 0:
            try:
                player.seek(elapsed_time, reference="absolute")
            except Exception as e:
                log.debug(f"Seek Error: {e}")
                break
        else:
            time.sleep(0.1)

        # Unpause playback if it is paused
        if player.pause:
            player.pause = False

        # Play video until end time has come
        while now < playing_now["end"]:
            now = datetime.now().replace(microsecond=0)

            # Break the loop if channel_changed is set to True
            if channel_changed:
                break
            time.sleep(0.1)