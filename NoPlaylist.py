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
from dotenv import load_dotenv

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

# Load env file
load_dotenv()

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

def show_info_card():
    font = ImageFont.truetype(os.getenv("CUSTOM_FONT"), 40)
    overlay = player.create_image_overlay()
    artistimg = Image.new("RGBA", (1000, 1500), (255, 255, 255, 0))
    d = ImageDraw.Draw(artistimg)

    try:
        conn2 = sqlite3.connect(solo_db)
        cursor2 = conn2.cursor()
        log.debug(f"Finding metadata for {player.path}")
        query = f'SELECT * FROM MUSIC WHERE Filepath = "{player.path}"'
        cursor2.execute(query)
        music_metadata = cursor2.fetchone()
        conn2.close()

        artist = music_metadata[2]
        title = music_metadata[3]
        finalText = f"{artist}\n{title}"

        d.text(
            (50, 900),
            finalText,
            font=font,
            fill=(255, 255, 255, 128),
            stroke_width=3,
            stroke_fill="black",
        )
        overlay.update(artistimg)
        time.sleep(5)
        overlay.remove()
    except Exception as e:
        log.debug(f"show_info_card: {e}")

# Main loop
# Set initial channel
current_channel = 2
first_time = True

MediaManager.process_movies()

# Check to see if schedule needs to be rebuilt
Schedule.clear_schedule_table()
if Schedule.check_schedule_for_rebuild():
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
            elapsed_time = (time_gap + timedelta(hours=chapter_hour, minutes=chapter_minute, seconds=chapter_second)).total_seconds()
            log.debug(f"{elapsed_time=}")
            log.debug(f"{type(elapsed_time)}")
        
            if elapsed_time > 0:
                log.debug(f"{elapsed_time=}")
                player.wait_for_property("seekable")
                player.seek(elapsed_time, reference="absolute")

            if player.pause:
                log.debug("Unpausing playback")
                player.pause = False

            while now < playing_now["end"]:
                now = datetime.now().replace(microsecond=0)
                time.sleep(0.1)

        # Filler
        # Make sure that filler stops on time
        elif "Filler" in playing_now["filepath"]:
            log.debug("Filler time")
            
            if player.pause:
                log.debug("Unpausing playback")
                player.pause = False

            while now < playing_now["end"]:
                now = datetime.now().replace(microsecond=0)
                time.sleep(0.1)
        
        # No chapters, movies, etc
        # Playthough until the end of the item
        else:
            elapsed_time = (now - playing_now["showtime"]).total_seconds()

            if elapsed_time > 0:
                log.debug(f"{elapsed_time=}")
                player.wait_for_property("seekable")
                player.seek(elapsed_time, reference="absolute")

            if player.pause:
                log.debug("Unpausing playback")
                player.pause = False

            # Music video - Info cards
            if current_channel == 3:
                if "ident" not in player.path:
                    if first_time == False:
                        time.sleep(3)
                        show_info_card()
                    else:
                        show_info_card()

                    while True:
                        current_time = player.time_pos
                        duration = player.duration
                        remaining_time = duration - current_time if current_time is not None else None

                        if remaining_time is not None and remaining_time <= 10:
                            show_info_card()
                            break

            log.debug("Waiting for file to be completely played")
            player.wait_for_property("eof-reached")
            log.debug("End of file has been reached")


        # Playback has ended
                    # if (int(player.time_pos) == 3) or (int(player.time_pos) == (int(player.duration) - 10) or first_time == True):
                    #     show_info_card()
                    #     first_time = False

