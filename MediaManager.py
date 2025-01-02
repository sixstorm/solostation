# Media Manager
import tvdb_v4_official
import glob
import re
import os
import sqlite3
import moviepy.editor as mp
import subprocess as sp
import json
import logging
import time
from rich.console import Console
from rich.logging import RichHandler

# Variables
movie_root = "/media/ascott/USB/movies/"
tv_root = "/media/ascott/USB/tv/"
comm_root = "/media/ascott/USB/bumpers"
web_root = "/media/ascott/USB/web"
music_root = "/media/ascott/USB/music"
mt_root = "/media/ascott/USB/bumpers/Trailers"
tvdb_connected = False

# SQLite
conn = sqlite3.connect("/media/ascott/USB/database/solodb.db")
cursor = conn.cursor()

# Rich log
FORMAT = "%(message)s"
logging.basicConfig(
    level="DEBUG", format=FORMAT, datefmt="[%X]", handlers=[RichHandler()]
)
log = logging.getLogger("rich")
console = Console

# Functions
def connect_tvdb_api():
    """
    Connects to the TVDB API

    Args:
        None

    Returns:
        None

    Raises:
        None

    Example:
        connect_tvdb_api()
    """

    global tvdb, tvdb_connected

    if not tvdb_connected:
        log.debug("Connecting to TVDB")
        apikey = "21857f0d-16b5-4d5e-8505-f46281ceabdd"
        tvdb = tvdb_v4_official.TVDB(apikey)
        tvdb_connected = True

def get_runtime(file):
    """
    Generates and returns the runtime for a video

    Args:
        file (string): Video file path input

    Returns:
        runtime (string): Runtime in 'HH:MM:SS' format

    Raises:

    Example:
        get_runtime("movie.mp4")
    """

    log.debug(f"Getting runtime for {file}")

    # Get runtime of episode in MM:SS
    file_data = mp.VideoFileClip(file)
    file_duration = int(file_data.duration)

    hours = file_duration // 3600
    minutes = (file_duration % 3600) // 60
    seconds = file_duration % 60

    runtime = f"{hours:02}:{minutes:02}:{seconds:02}"

    return runtime


def get_chapters(file):
    """
    DESCRIPTION

    Args:
        file (string): Video file input

    Returns:
        output - Output of ffprobe, chapters parsed into JSON format
        OR
        None (if chapters don't exist for file)

    Raises:

    Example:
        get_chapters("/media/ascott/USB/movies/Batman (1989)/Batman.mp4")
    """

    command = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_chapters",
        "-sexagesimal",
        file,
    ]
    result = sp.run(command, capture_output=True, text=True, check=True)
    output = json.loads(result.stdout)

    if "chapters" in output and len(output["chapters"]) == 0:
        return False
    else:
        return output

def download_episode_metadata(show_name, show_year, episode_json, extended_json):
    """
    Downloads the episode and series extended metadata from TVDB to separate JSON files

    Args:
        show_name (string): Name of the show
        show_year (string): Year of the show
        episode_json (string): Location of the episode JSON file
        extended_json (string): Location of the series extended JSON file

    Returns:
        None
    """

    log.debug("Missing episode or extended data - Downloading from TVDB")
    if not tvdb_connected:
        connect_tvdb_api()

    # Search TVDB using showName and showYear
    seriesDataFull = [s for s in tvdb.search(show_name) if "year" in s]
    seriesData = [
        s for s in seriesDataFull if s["type"] == "series" and s["year"] == show_year
    ][0]
    episode_data = tvdb.get_series_episodes(seriesData["tvdb_id"])
    extended_data = tvdb.get_series_extended(seriesData["tvdb_id"])

    # Save episode data to local JSON file
    with open(episode_json, "w") as file:
        json.dump(episode_data, file, indent=4)

    # Save extended data to local JSON file
    with open(extended_json, "w") as file:
        json.dump(extended_data, file, indent=4)

def download_movie_metadata(movie_name, movie_year, movie_json, movie_extended_json):
    """_summary_

    Args:
        movie_name (_type_): _description_
        movie_year (_type_): _description_
    """

    global tvdb, tvdb_connected

    if not tvdb_connected:
        connect_tvdb_api()

    log.debug(f"Downloading movie metadata for {movie_name}")

    try:
        # Seach TVDB for movie metadata
        movie_metadata = [
            m
            for m in (tvdb.search(movie_name))
            if "year" in m and (m["type"] == "movie" and m["year"] == movie_year)
        ][0]

        # Get extended data and genres for tags
        movie_extended_data = tvdb.get_movie_extended(movie_metadata["tvdb_id"])

        # Save metadata and extended metadata to local json files
        with open(movie_json, "w") as file:
            json.dump(movie_metadata, file, indent=4)
        with open(movie_extended_json, "w") as file:
            json.dump(movie_extended_data, file, indent=4)
    except Exception as e:
        log.debug(f"Error downloading movie metadata: {e}")
        

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

def initialize_all_tables():
    """
    Creates all necessary tables if they don't exist

    Args:
        None

    Returns:
        None

    Raises:

    Example:
        initialize_all_tables()
    """

    # Stats
    log.debug("Initializing Stats database")
    table = """ CREATE TABLE IF NOT EXISTS STATS(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Channel INTEGER,
        TimesPlayed INTEGER,
        Filepath TEXT
    );"""

    cursor.execute(table)

    # Commercials
    log.debug("Initializing Commercial database")
    table = """ CREATE TABLE IF NOT EXISTS COMMERCIALS(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Tags TEXT,
        Runtime TEXT,
        Filepath TEXT
    );"""

    cursor.execute(table)

    # Music
    log.debug("Initializing Music database")
    table = """ CREATE TABLE IF NOT EXISTS MUSIC(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Tags TEXT,
        Artist TEXT,
        Title TEXT,
        Runtime TEXT,
        Filepath TEXT
    );"""

    cursor.execute(table)

    # Web
    log.debug("Initializing Web database")
    table = """ CREATE TABLE IF NOT EXISTS WEB(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Tags TEXT,
        Runtime TEXT,
        Filepath TEXT
    );"""

    cursor.execute(table)

    # TV
    log.debug("Initializing TV database")
    table = """ CREATE TABLE IF NOT EXISTS TV(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT,
        ShowName TEXT,
        Season INTEGER,
        Episode INTEGER,
        Overview TEXT,
        Tags TEXT,
        Runtime TEXT,
        Filepath TEXT
    );"""

    cursor.execute(table)

    # Chapters
    log.debug("Initializing Chapters database")
    table = """ CREATE TABLE IF NOT EXISTS CHAPTERS(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        EpisodeID INTEGER,
        Title TEXT,
        Start TEXT,
        End INT,
        FOREIGN KEY (EpisodeID) REFERENCES TV (ID) ON DELETE CASCADE
    );"""

    cursor.execute(table)

    # Movies
    log.debug("Initializing movie database")
    table = """ CREATE TABLE IF NOT EXISTS MOVIE(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT,
        Year TEXT,
        Overview TEXT,
        Tags TEXT,
        Runtime TEXT,
        Filepath TEXT
    );"""

    cursor.execute(table)

def process_music():
    """
    Go through each music video file and insert metadata into the dasebase

    Args:
        None

    Returns:
        None
    """

    log.debug("")
    log.debug("Searching and processing music videos and idents")

    for file in glob.glob("/media/ascott/USB/music/*.mp4"):
        if not check_if_in_table("MUSIC", file):
            # Get artist and title from filename
            artist = file.split(" - ")[0]
            artist = artist.split("/")[5]
            title = file.split(" - ")[1]
            title = title.split(" (")[0]
            title = title.split(" [")[0]
            title = title.split(".mp4")[0]

            # Get runtime
            runtime = get_runtime(file)

            # Insert into database and commit changes
            cursor.execute(
                "INSERT INTO MUSIC (Tags, Artist, Title, Runtime, Filepath) VALUES (?, ?, ?, ?, ?)",
                ("music", artist, title, runtime, file),
            )
            conn.commit()

    # Process each MTV ident
    for file in glob.glob("/media/ascott/USB/music/idents/*.mp4"):
        if not check_if_in_table("MUSIC", file):
            # Get runtime
            runtime = get_runtime(file)

            # Insert into database and commit changes
            cursor.execute(
                "INSERT INTO MUSIC (Tags, Artist, Title, Runtime, Filepath) VALUES (?, ?, ?, ?, ?)",
                ("ident", None, None, runtime, file),
            )
            conn.commit()

def process_commercials():
    """
    Go through each commercial video file and insert metadata into the dasebase

    Args:
        None

    Returns:
        None
    """

    log.debug("")
    log.debug("Searching and processing commercials")

    for file in glob.glob("/media/ascott/USB/bumpers/*/*.mp4"):
        if not check_if_in_table("COMMERCIALS", file):
            # Get runtime
            runtime = get_runtime(file)

            # Get tags
            tags = ["commercial"]

            # Use folder name as a tag, i.e. "80s" or "Gaming"
            tags.append(file.split("/")[5].lower())
            tags = ",".join(tags)

            # Insert into database
            cursor.execute(
                "INSERT INTO COMMERCIALS (Tags, Runtime, Filepath) VALUES (?, ?, ?)",
                (tags, runtime, file),
            )
            conn.commit()

def process_web():
    """
    Go through each web video file and insert metadata into the dasebase

    Args:
        None

    Returns:
        None
    """

    log.debug("")
    log.debug("Searching and processing web content")

    for file in glob.glob("/media/ascott/USB/web/*.mp4"):
        if not check_if_in_table("WEB", file):
            # Get runtime
            runtime = get_runtime(file)

            # Insert into database
            cursor.execute(
                "INSERT INTO WEB(Tags, Runtime, Filepath) VALUES (?, ?, ?)",
                ("web", runtime, file),
            )
            conn.commit()

def process_tv():
    """
    Go through each TV video file and insert metadata into the dasebase

    Args:
        None

    Returns:
        None
    """

    log.debug("")
    log.debug("Searching and processing TV episodes")

    # Go through each TV show folder
    for tv_root_folder in next(os.walk(tv_root))[1]:
        # Parse metadata of TV show based on folder name
        show_root_folder = f"{tv_root}{tv_root_folder}"
        show_name = re.search(".+?(?=\s\()", tv_root_folder)[0]
        show_year = re.search("\(([0-9]{4})\)", tv_root_folder)[1]
        episode_json = f"{show_root_folder}/episodes.json"
        series_extended_json = f"{show_root_folder}/series-extended.json"

        # Gather all MP4 and MKV files under the current TV show folder
        all_episode_files = glob.glob(
            f"{show_root_folder}/*/*.mp4", recursive=True
        ) + glob.glob(f"{show_root_folder}/*/*.mkv", recursive=True)
        log.debug(f"Found {len(all_episode_files)} episodes for {show_name}")

        # Check for episode and extended data local json files
        log.debug(f"Checking for {show_name} local data")
        if not os.path.exists(episode_json) or not os.path.exists(series_extended_json):
            download_episode_metadata(show_name, show_year, episode_json, series_extended_json)

        # Open episode and extended json files
        log.debug("Opening Episode and Series Local JSON files")
        with open(episode_json, "r") as episode_data_file:
            episode_local_data = json.load(episode_data_file)
        with open(series_extended_json, "r") as series_data_file:
            series_local_data = json.load(series_data_file)

        # Check each episode and make sure that it is in the database
        for episode in all_episode_files:
            if not check_if_in_table("TV", episode):
                # Parse season and episode numbers
                season_number = re.search("S(\d{2})", episode).group(1)
                if season_number.startswith("0"):
                    season_number = season_number.lstrip("0")
                
                episode_number = re.search("E(\d{2})", episode).group(1)
                if episode_number.startswith("0"):
                    episode_number = episode_number.lstrip("0")

                # log.debug([e for e in episode_local_data["episodes"] if e["seasonNumber"] == 1][0])

                # Find episode metadata in local json file
                try:
                    log.debug(f"Searching local files for season {season_number} episode {episode_number}")
                    episode_metadata = [
                        e 
                        for e in episode_local_data["episodes"] 
                        if e["seasonNumber"] == int(season_number)
                        and e["number"] == int(episode_number)
                    ][0]
                except Exception as e:
                    log.debug(f"Episode Metadata Error: {e}")

                if episode_metadata:
                    # Append tags from the show's genres
                    tags = ["tv"]
                    for tag in series_local_data["genres"]:
                        tags.append(tag["name"].lower())
                    tags = str(",".join(tags))

                    # Get runtime for episode
                    runtime = get_runtime(episode)

                    # Insert episode into database
                    cursor.execute(
                        "INSERT INTO TV (Name, ShowName, Season, Episode, Overview, Tags, Runtime, Filepath) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            episode_metadata["name"],
                            show_name,
                            season_number,
                            episode_number,
                            episode_metadata["overview"],
                            tags,
                            runtime,
                            episode
                        ),
                    )
                    conn.commit()

                    episode_id = cursor.lastrowid
                    all_chapters = get_chapters(episode)
                    chapter_number = 1
                    if all_chapters:
                        for chapter in all_chapters["chapters"]:
                            log.debug(f"{chapter=}")
                            # Insert chapter into database
                            cursor.execute(
                                "INSERT INTO CHAPTERS (EpisodeID, Title, Start, End) VALUES (?, ?, ?, ?)",
                                (
                                    episode_id,
                                    chapter_number,
                                    f"0{chapter['start_time'].split('.')[0]}",
                                    f"0{chapter['end_time'].split('.')[0]}",
                                ),
                            )
                            conn.commit()
                            chapter_number += 1

def process_movies():
    """
    Go through each movie video file and insert metadata into the dasebase

    Args:
        None

    Returns:
        None
    """

    log.debug("")
    log.debug("Searching and processing movies")

    # Go through each movie folder
    for movie_folder in next(os.walk(movie_root))[1]:
        # Search for either a MP4 and MKV movie file with the movie root folder
        movie_root_folder = f"{movie_root}{movie_folder}"
        log.debug(f"{movie_folder=}")
        try:
            movie_file = (glob.glob(f"{movie_root_folder}/*.mp4") + glob.glob(f"{movie_root_folder}/*.mkv"))[0]
        except IndexError:
            continue

        # Check and insert movie metadata into the database if it doesn't exist
        if not check_if_in_table("MOVIE", movie_file):
            # Parse movie name and year from filename
            movie_name = re.search(".+?(?=\s\()", movie_folder)[0]
            movie_year = re.search("\(([0-9]{4})\)", movie_folder)[1]
            movie_name_no_spaces = movie_name.replace(" ", "")
            movie_json = f"{movie_root_folder}/{movie_name_no_spaces}.json"
            movie_extended_json = f"{movie_root_folder}/{movie_name_no_spaces}-extended.json"
            log.debug(f"{movie_name=}")
            log.debug(f"{movie_year=}")
            log.debug(f"{movie_name_no_spaces=}")
            log.debug(f"{movie_json=}")
            log.debug(f"{movie_extended_json=}")

            # Download movie metadata json file if not available
            if not os.path.exists(movie_json) or not os.path.exists(movie_extended_json):
                download_movie_metadata(movie_name, movie_year, movie_json, movie_extended_json)

            # Open local json files
            with open(movie_json) as file:
                movie_metadata = json.load(file)

            with open(movie_extended_json) as file:
                movie_extended_metadata = json.load(file)

            tags = ["movie"]
            for tag in movie_extended_metadata["genres"]:
                tags.append(tag["name"].lower())

            # Insert movie into database
            tags = str(",".join(tags))
            log.debug(f"{movie_file=}")
            runtime = get_runtime(movie_file)
            cursor.execute(
                "INSERT INTO MOVIE (Name, Year, Overview, Tags, Runtime, Filepath) VALUES (?, ?, ?, ?, ?, ?)", (movie_metadata['name'], movie_metadata['year'], movie_metadata['overview'], tags, runtime, movie_file)
            )
            conn.commit()


# initialize_all_tables()
# process_commercials()
# process_web()
# process_music()
# process_movies()
# process_tv()