import json
import random
import sqlite3
import time
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler
import logging

# Rich log
FORMAT = "%(message)s"
logging.basicConfig(
    level="DEBUG", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("rich")

# Variables
schedule_list = []
channel_file = "./channels.json"

# Open Database
conn = sqlite3.connect("/media/ascott/USB/database/solodb.db")
cursor = conn.cursor()

# Read in channel json file
log.debug("Opening the channel file")
with open(channel_file, "r") as channel_file_input:
    channel_data = json.load(channel_file_input)

# Functions
def initialize_schedule_db():
    """
    Initializes the Schedule table in the database

    Args:
        None

    Returns:
        None
    """

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

def clear_schedule_table():
    """
    Completely clears the schedule table

    Args:
        None

    Returns:
        None
    """

    cursor.execute("DELETE FROM SCHEDULE")
    # cursor.execute("VACUUM")
    conn.commit()

def clear_old_schedule_items():
    '''
    Removes all old scheduled items from the SCHEDULE table
    where End time has been passed by current time

    Args:
    Returns:  
        None
        
    Raises:
    '''

    current_time = (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
    query = f"""DELETE FROM SCHEDULE WHERE End < '{current_time}'"""
    cursor.execute(query)
    conn.commit()

def check_schedule_for_rebuild(channel_number):
    """
    Checks for items scheduled for today in the Schedule table
    Builds schedule if not available

    Args:
        None

    Returns:
        (bool) - True if there are no future dates in the schedule

    Raises:

    Example:
        check_schedule_for_rebuild()
    """

    now = datetime.now()
    cursor.execute("SELECT COUNT(*) FROM Schedule WHERE end > ?", (now,))
    results = cursor.fetchone()[0]
    return results == 0

    # cursor.execute(f"SELECT * FROM SCHEDULE WHERE Channel = '{channel_number}'")
    # results = cursor.fetchall()
    # if len(results) == 0:
    #     log.debug("Rebuild - YUP")
    #     return True
    # else:
    #     log.debug("Rebuild - NOPE")
    #     return False

    # for channel_number in [2, 3, 4, 5, 6]:
    #     # Query Database for all items scheduled for this channel
    #     today_date = datetime.strftime(datetime.now(), "%Y-%m-%d")
    #     cursor.execute(
    #         f"""SELECT * FROM SCHEDULE WHERE Channel = {channel_number} AND Showtime LIKE '%{today_date}%'"""
    #     )
    #     results = cursor.fetchall()
    #     if results:
    #         log.debug(f"Schedule already exists for {today_date}")
    #         return True
    #     else:
    #         log.debug(f"Schedule does not exists for {today_date}")
    #         return False

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

    cursor.execute(
        "INSERT INTO SCHEDULE (Channel, Showtime, End, Filepath, Chapter, Runtime) VALUES (?, ?, ?, ?, ?, ?)",
        (channel_number, showtime, end, filepath, chapter, runtime),
    )

    # Commit changes to database
    conn.commit()

def check_if_in_table(table, filepath):
    """
    Checks to see if a filepath exists in the database given the table

    Args:
        table (string): Table that needs to be searched
        filepath (string): Video file

    Returns:
        Bool - True or False

    Raises:

    Example:
        check_if_in_table("TV", "movie.mp4")
    """

    query = f'SELECT * FROM {table} WHERE Filepath="{filepath}"'
    cursor.execute(query)
    result = cursor.fetchone()
    if result:
        return True
    else:
        return False

def add_media_stats(channel_number, filepath):
    """
    Increments a file in the database if scheduled

    Args:
        filepath (string): Video file path

    Returns:
        None

    Raises:
    Example:  add_media_stats(4, "/media/ascott/USB/movies/The Batman (2020)/The Batman.mp4")
    """

    # Check to see if filepath is in the STATS table
    if not check_if_in_table("STATS", filepath):
        # Insert into table for the first time
        cursor.execute(
            "INSERT INTO STATS (Channel, TimesPlayed, Filepath) VALUES (?, ?, ?)", 
            (channel_number, 1, filepath),
            )
    else:
        # Increment stats counter by 1
        cursor.execute(
            """
            UPDATE STATS 
            SET TimesPlayed = TimesPlayed + 1 
            WHERE Channel = ? AND Filepath = ?
            """,
            (channel_number, filepath)
        )
        
    # Commit changes to database
    conn.commit()

def find_all_in_db_by_tag(tags):
    '''
    Search SQLite database based on tags column

    Args:
        tags (list): List of strings from channel2.json

    Returns:
        result_list (list): List of objects (dictionaries) for all query results

    Raises:
    Example:
    '''

    result_list = []

    if "tv" in tags:
        like_clauses = " AND ".join([f"Tags LIKE '%{tag}%'" for tag in tags])
        query = f"""SELECT * FROM TV WHERE {like_clauses}"""

        cursor.execute(query)
        results = cursor.fetchall()

        for result in results:
            (ID,episodeName,showName,season,episode,overview,tags,runtime,filepath,) = result
            result_list.append({
                "id": ID,
                "name": episodeName,
                "showName": showName,
                "season": season,
                "episode": episode,
                "overview": overview,
                "tags": tags,
                "runtime": runtime,
                "filepath": filepath,
            })

    if "movie" in tags:
        log.debug("Movie Tag detected")
        like_clauses = " AND ".join([f"Tags LIKE '%{tag}%'" for tag in tags])
        query = f"""SELECT * FROM MOVIE WHERE {like_clauses}"""

        cursor.execute(query)
        results = cursor.fetchall()

        for result in results:
            (ID,name,year,overview,tags,runtime,filepath,) = result
            result_list.append({
                "name": name,
                "year": year,
                "overview": overview,
                "tags": tags,
                "runtime": runtime,
                "filepath": filepath,
            })

    if "music" in tags:
        like_clauses = " AND ".join([f"Tags LIKE '%{tag}%'" for tag in tags])
        query = f"""SELECT * FROM MUSIC WHERE {like_clauses}"""

        cursor.execute(query)
        results = cursor.fetchall()

        for result in results:
            (id, tags, artist, title, runtime, filepath) = result
            name = f"{artist} - {title}"
            result_list.append({
                "name": name,
                "tags": tags,
                "artist": artist,
                "title": title,
                "runtime": runtime,
                "filepath": filepath,
            })
    
    if "ident" in tags:
        like_clauses = " AND ".join([f"Tags LIKE '%{tag}%'" for tag in tags])
        query = f"""SELECT * FROM MUSIC WHERE {like_clauses}"""

        cursor.execute(query)
        results = cursor.fetchall()

        for result in results:
            (id, tags, artist, title, runtime, filepath) = result
            result_list.append({
                "name": filepath,
                "tags": tags,
                "artist": artist,
                "title": title,
                "runtime": runtime,
                "filepath": filepath,
            })

    if "web" in tags:
        like_clauses = " AND ".join([f"Tags LIKE '%{tag}%'" for tag in tags])
        query = f"""SELECT * FROM WEB WHERE {like_clauses}"""

        cursor.execute(query)
        results = cursor.fetchall()

        for result in results:
            (id,tags,runtime,filepath,) = result
            result_list.append({
                "name": filepath.split("/")[5].split(".mp4")[0],
                "tags": tags,
                "runtime": runtime,
                "filepath": filepath,
            })

    return result_list

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
        log.debug("30")
        next_play_time = marker.replace(minute=30, second=0)
    if marker.minute > 15 and marker.minute <=30:
        log.debug("45")
        next_play_time = marker.replace(minute=45, second=0)
    if marker.minute > 30 and marker.minute <=45:
        log.debug("00")
        if marker.hour == 23:
            next_play_time = (marker.replace(hour=0, minute=0, second=0)) + timedelta(days=1)
        else:
            next_play_time = marker.replace(hour=(marker.hour + 1), minute=0, second=0)
    if marker.minute > 45 and marker.minute <=59:
        log.debug("15")
        if marker.hour == 23:
            next_play_time = (marker.replace(hour=0, minute=15, second=0)) + timedelta(days=1)
        else:
            next_play_time = marker.replace(hour=(marker.hour + 1), minute=15, second=0)

    return next_play_time

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
        
        return chapters
    else:
        return None

def get_chapter_length(chapter):
    '''
    Calculates the length of a given chapter

    Args:
        chapter (dictionary) - Chapter information:  Title, Start, End

    Returns:
        (string) - Total time in "HH:MM:SS" format

    Raises:
    Example:
    '''

    # Unpack chapter tuple
    title, start, end = chapter

    # Calculate length of chapter
    start_hours, start_minutes, start_seconds = map(int, start.split(":"))
    start_TD = timedelta(hours=start_hours,minutes=start_minutes,seconds=start_seconds)
    end_hours, end_minutes, end_seconds = map(int, end.split(":"))
    end_TD = timedelta(hours=end_hours,minutes=end_minutes,seconds=end_seconds)
    total_TD = end_TD - start_TD
    total_minutes = int(total_TD.total_seconds() // 60)
    total_seconds = int(total_TD.total_seconds() % 60)
    return f"00:{total_minutes:02}:{total_seconds:02}"

def calculate_max_break_time(runtime, chapters):
    '''
    Calculates the max amount of time each commercial break can have

    Args:
        runtime (string): Runtime of media item
        chapters (list of tuples): Video file that is "currently playing"

    Returns:
        max_break_time (timedelta): Total time per commercial

    Raises:
    Example:
    '''

    # Get total hours, minutes and seconds of random_media
    hours, minutes, seconds = map(int, runtime.split(":"))
    runtime_TD = timedelta(minutes=minutes, seconds=seconds)

    # Detect if episode if longer than 30 minutes
    if runtime_TD > timedelta(minutes=30):
        total_commercial_time = timedelta(hours=1) - runtime_TD
    else:
        total_commercial_time = timedelta(minutes=30) - runtime_TD

    # Final calculation - Time per commercial break
    max_break_time = total_commercial_time // len(chapters)
    return max_break_time

def add_commercial_break(channel_number, max_break):
    '''
    Adds a commercial break if commercials are allowed

    Args:
        channel_number (string): Number of channel
        filepath (string): Video file that is "currently playing"
        max_break (string): Minutes and seconds of the maximum time for a single commercial break

    Returns:
        None

    Raises:
    '''

    global marker

    chosen_commercials = []

    # Split up max break from timedelta to individual units
    mb_hours = int(max_break.total_seconds() // 3600)
    mb_minutes = int((max_break.total_seconds() % 3600) // 60)
    mb_seconds = int(max_break.total_seconds() % 60)
    mb_TD_str = f"{mb_hours:02}:{mb_minutes:02}:{mb_seconds:02}"
    log.debug(f"Initial Max Break: {mb_TD_str}")

    # Leave some padding - If max break gets less than 15 seconds, move on to next chapter
    while max_break > timedelta(seconds=15):
        # Re-calculate remaining time in commercial break
        mb_hours = int(max_break.total_seconds() // 3600)
        mb_minutes = int((max_break.total_seconds() % 3600) // 60)
        mb_seconds = int(max_break.total_seconds() % 60)
        mb_TD_str = f"{mb_hours:02}:{mb_minutes:02}:{mb_seconds:02}"

        # Get commercials that have a runtime less than max break
        query = f"""SELECT * FROM COMMERCIALS WHERE Runtime <= '{mb_TD_str}' ORDER BY RANDOM()"""
        cursor.execute(query)
        random_commercials = cursor.fetchall()

        # Stop commercial break if no commercials are available
        if len(random_commercials) == 0:
            log.debug("End of commercial break")
            break

        log.debug(f"Found {len(random_commercials)} commercials that fit")
        
        random_commercial = random.choice([r for r in random_commercials if r[3] not in chosen_commercials])
        if not random_commercial:
            log.debug("End of commercial break")
            break

        # Make sure that chosen commercial fits
        hours, minutes, seconds = map(int, random_commercial[2].split(":"))
        rc_str = f"{hours:02}:{minutes:02}:{seconds:02}"
        if rc_str <= mb_TD_str:
            post_marker = marker + timedelta(hours=hours,minutes=minutes,seconds=seconds)
            log.debug(f"Commercial chosen - {random_commercial[3]}")
            insert_into_schedule(channel_number, marker, post_marker, random_commercial[3], None, random_commercial[2])
            chosen_commercials.append(random_commercial[3])
            max_break -= timedelta(hours=hours,minutes=minutes,seconds=seconds)
            marker = post_marker

            # Break loop if max break gets under 15 seconds
            if max_break.total_seconds() >= 0 and max_break.total_seconds() < 15:
                break
        else:
            log.debug("Commercial does not fit, continuing")
    
    log.debug("End of commercial break")

def add_final_commercial_break(channel_number, next_showtime):
    '''
    Adds a final commercial break after an episode has played its last chapter
    Will play a filler track for the last ~30 seconds of break

    Args:
        channel_number (string): Number of channel
        next_showtime (datetime): Next showtime of next playing item

    Returns:
        None

    Raises:
    Example:
    '''
    global marker

    chosen_commercials = []

    # convert next_showtime to datetime
    nst_hour, nst_minute = map(int, next_showtime.split(":"))
    next_showtime = datetime.now().replace(hour=nst_hour, minute=nst_minute, second=0, microsecond=0)

    # Fill with commercials
    while marker < (next_showtime - timedelta(seconds=60)):
        # Determine time remaining in the final break
        time_remaining = (next_showtime - timedelta(seconds=60)) - marker
        tr_hours = int(time_remaining.total_seconds() // 3600)
        tr_minutes = int((time_remaining.total_seconds() % 3600) // 60)
        tr_seconds = int(time_remaining.total_seconds() % 60)
        tr_str = f"{tr_hours:02}:{tr_minutes:02}:{tr_seconds:02}"

        # Break loop if there is 15 seconds or less in time remaining so filler can play
        if time_remaining <= timedelta(seconds=15):
            break

        # Get commercials that have a runtime less than time remaining
        query = f"""SELECT * FROM COMMERCIALS WHERE Runtime <= '{tr_str}' ORDER BY RANDOM()"""
        cursor.execute(query)
        random_commercials = cursor.fetchall()

        # If no commercials are available, go to add filler
        if len(random_commercials) == 0:
            log.debug("End of commercial break - Onto the filler")
            break

        # Add commercial to schedule
        random_commercial = random.choice([r for r in random_commercials if r[3] not in chosen_commercials])
        if not random_commercial:
            log.debug("End of commercial break - Onto the filler")
            break

        hours, minutes, seconds = map(int, random_commercial[2].split(":"))
        rc_str = f"{hours:02}:{minutes:02}:{seconds:02}"
        if rc_str <= tr_str:
            log.debug(f"{rc_str} fits under {tr_str}")
            post_marker = marker + timedelta(hours=hours,minutes=minutes,seconds=seconds)
            insert_into_schedule(channel_number, marker, post_marker, random_commercial[3], None, random_commercial[2])
            time_remaining -= timedelta(hours=hours,minutes=minutes,seconds=seconds)
            marker = post_marker

    # Add filler
    log.debug("Adding Filler")
    query = """SELECT * FROM COMMERCIALS WHERE Tags LIKE '%filler%'"""
    cursor.execute(query)
    filler = cursor.fetchall()[0]

    # Get length of filler play time
    insert_into_schedule(channel_number, marker, next_showtime, filler[3], None, filler[2])

    # Move marker
    marker = next_showtime

def add_post_movie(channel_number, next_showtime):
    '''
    Adds a final commercial break after an episode has played
    Will play a filler track for the last ~30 seconds of break

    Args:
        channel_number (string): Number of channel
        next_showtime (string): Next showtime of next playing item - "18:00:00"

    Returns:
        None

    Raises:
    Example:
    '''
    global marker

    # Convert next_showtime to datetime
    nst_hour, nst_minute = map(int, next_showtime.split(":"))
    next_showtime = datetime.now().replace(hour=nst_hour, minute=nst_minute, second=0, microsecond=0)


    # Determine time left between marker and next_showtime as a string
    time_remaining = (next_showtime - timedelta(seconds=60)) - marker
    tr_hours = int(time_remaining.total_seconds() // 3600)
    tr_minutes = int((time_remaining.total_seconds() % 3600) // 60)
    tr_seconds = int(time_remaining.total_seconds() % 60)
    tr_str = f"{tr_hours:02}:{tr_minutes:02}:{tr_seconds:02}"

    # Get all commercials and web content that fit within time remaining
    query = f"""SELECT * FROM COMMERCIALS WHERE Tags LIKE '%trailers%' AND Runtime < '{tr_str}'"""
    cursor.execute(query)
    all_commercials = cursor.fetchall()

    query = f"""SELECT * FROM WEB WHERE Runtime < '{tr_str}'"""
    cursor.execute(query)
    all_web = cursor.fetchall()

    all_media = all_commercials + all_web
    random.shuffle(all_media)
    log.debug(f"Found {len(all_media)} items to fill post movie")

    for random_media in all_media:
        media_hour, media_minute, media_second = map(int, random_media[2].split(":"))
        media_runtime_TD = timedelta(hours=media_hour, minutes=media_minute, seconds=media_second)
        post_marker = marker + (media_runtime_TD + timedelta(seconds=1))
        if post_marker < next_showtime:
            # Insert into schedule and move the marker
            insert_into_schedule(channel_number, str(marker), str(post_marker), random_media[3], None, random_media[2])
            marker = post_marker
        else:
            continue

    # Add final filler
    log.debug("Adding Filler")
    query = """SELECT * FROM COMMERCIALS WHERE Tags LIKE '%filler%'"""
    cursor.execute(query)
    filler = cursor.fetchall()[0]

    insert_into_schedule(channel_number, str(marker), str(next_showtime), filler[3], None, filler[2])

def create_schedule():
    '''
    Creates a schedule for each channel defined in the channels.json file
    By default, each channel will handle TV and movies, with commercials for each TV episode

    Args:
        channel (channel object): All information about channel from channels.json
        dayName (string): Day of the week (Monday, Tuesday, etc)

    Returns:  
        None
        
    Raises:
    Example:
    '''

    global marker

    log.debug("Building a new schedule")

    # Get today's date
    day_name = datetime.today().strftime("%A")
    log.info(f"Today is {day_name}")

    # Process each channel from the channel file
    for channel_metadata in channel_data:
        # Parse channel information
        channel_name = channel_metadata
        channel_schedule = channel_data[channel_name]["schedule"][day_name]
        channel_number = channel_data[channel_name]["channel_number"]

        if channel_number != 2:
            log.debug("Channel not 2")
            continue


        log.info("")
        log.info(f"Working on {channel_name} - {channel_number}")

        # Specialty Channels

        # Non-specialty Channels
        for slot_index, (slot_time, slot_tags) in enumerate(channel_schedule.items()):
            log.info(slot_time)
            log.info(slot_tags)

            # Set marker for slot as start
            slot_hour, slot_minute = map(int, slot_time.split(":"))
            marker = datetime.now().replace(hour=slot_hour, minute=slot_minute, second=0, microsecond=0)

            # Search database for content based on slot tags
            random_media = random.choice(find_all_in_db_by_tag(slot_tags))
            log.debug(f"Chose {random_media['name']} for {slot_time}")

            # Process TV episode
            if "tv" in slot_tags:
                chapters = get_chapters(random_media['filepath'])
                if chapters:
                    log.debug(f"{len(chapters)} chapters detected for this episode")

                    # Calculate max commercial break times
                    max_break_time = calculate_max_break_time(random_media["runtime"], chapters)

                    # Process each chapter
                    for chapter_index, chapter in enumerate(chapters, 1):
                        # Calculate chapter length
                        chapter_length = get_chapter_length(chapter)

                        # Combine final 2 chapters if last chapter is less than 90 seconds
                        if chapter == chapters[-2]:
                            # Get length of final chapter
                            final_chapter_length = get_chapter_length(chapters[-1])
                            if final_chapter_length < "00:01:30":
                                log.debug("Final chapter is less than 90 seconds")
                                log.debug(f"{final_chapter_length=}")

                                # Add last 2 chapters to the schedule database
                                # Second to last chapter
                                chapter_hours, chapter_minutes, chapter_seconds = map(int, chapter_length.split(":"))
                                post_marker = marker + timedelta(hours=chapter_hours, minutes=chapter_minutes, seconds=(chapter_seconds + 1))
                                insert_into_schedule(channel_number, marker, post_marker, random_media["filepath"], chapter_index, chapter_length)
                                marker = post_marker
                                
                                # Final chapter
                                final_chapter_hours, final_chapter_minutes, final_chapter_seconds = map(int, final_chapter_length.split(":"))
                                post_marker = marker + timedelta(hours=final_chapter_hours, minutes=final_chapter_minutes, seconds=(final_chapter_seconds + 1))
                                insert_into_schedule(channel_number, marker, post_marker, random_media["filepath"], (chapter_index + 1), final_chapter_length)
                                marker = post_marker

                                # Break loop
                                break

                        else:
                            # Add chapter to schedule database
                            log.debug(f"Adding chapter {chapter_index} - {chapter_length}")
                            chapter_hours, chapter_minutes, chapter_seconds = map(int, chapter_length.split(":"))
                            post_marker = marker + timedelta(hours=chapter_hours, minutes=chapter_minutes, seconds=(chapter_seconds + 1))
                            insert_into_schedule(channel_number, marker, post_marker, random_media["filepath"], chapter_index, chapter_length)
                            marker = post_marker

                            # Add commercial break
                            log.debug("Adding commercial break")
                            add_commercial_break(channel_number, max_break_time)

                else:
                    # Play episode entirely if no chapters are present
                    log.debug(f"No chapters were found for {random_media['filepath']}")

                    # Add episode to schedule database
                    episode_hours, episode_minutes, episode_seconds = map(int, random_media["runtime"].split(":"))
                    post_marker = marker + timedelta(hours=episode_hours, minutes=episode_minutes, seconds=(episode_seconds + 1))
                    insert_into_schedule(channel_number, marker, post_marker, random_media["filepath"], None, random_media["runtime"])
                    marker = post_marker
                    
                # Add final commercial break after all chapters have been scheduled
                next_showtime = list(channel_schedule.items())[slot_index + 1][0]
                log.debug(f"Adding final commercial break until next showtime: {next_showtime}")
                add_final_commercial_break(channel_number, next_showtime)

                # Update stats

            # Movies and Web Content
            else:
                movie_hours, movie_minutes, movie_seconds = map(int, random_media["runtime"].split(":"))
                post_marker = marker + timedelta(hours=movie_hours, minutes=movie_minutes, seconds=(movie_seconds + 1))
                insert_into_schedule(channel_number, marker, post_marker, random_media["filepath"], None, random_media["runtime"])
                marker = post_marker

                # Add final commercial break after all chapters have been scheduled
                try:
                    next_showtime = list(channel_schedule.items())[slot_index + 1][0]
                    log.debug(f"Adding final commercial break until next showtime: {next_showtime}")
                    add_post_movie(channel_number, next_showtime)
                except Exception as e:
                    continue

                # Update stats



                                


# clear_schedule_table()
# initialize_schedule_db()
# create_schedule()
