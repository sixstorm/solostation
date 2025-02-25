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

# Classes
class Scheduler:
    def __init__(self, channel_number, marker, end_datetime, db_manager):
        self.channel_number = channel_number
        self.marker = marker
        self.end_datetime = end_datetime
        self.db_manager = db_manager
        self.log = logging.getLogger("rich")

    def schedule(self):
        raise NotImplementedError

    def runtime_to_timedelta(self, runtime):
        """ Convert runtime (datetime) to timedelta """
        h, m, s = map(int, runtime.split(":"))
        return timedelta(hours=h, minutes=m, seconds=s)


class DatabaseManager:
    def __init__(self, db_path):
        load_dotenv()
        self.db_path = db_path or os.getenv("DB_LOCATION")
        self.log = logging.getLogger("rich")
        self.initialize_schedule_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def initialize_schedule_db(self):
        """
        Initializes the Schedule table in the database

        Args:
            None

        Returns:
            None
        """

        query = """ CREATE TABLE IF NOT EXISTS TESTSCHEDULE(
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            Channel INTEGER,
            Showtime TEXT,
            End TEXT,
            Filepath TEXT,
            Chapter INTEGER,
            Runtime TEXT
        );"""

        try:
            with self._connect() as conn:
                conn.execute(query)
            self.log.debug("Schedule has been initialized")
        except sqlite3.error as e:
            self.log.error(f"Failed to initialize Schedule: {e}")
            raise

    def clear_schedule_table(self):
        """ Clears out the Schedule table """
        try:
            with self._connect() as conn:
                # current_time = (datetime.now() - timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
                current_time = datetime.now().replace(hour=0,minute=0,second=0,microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
                query = f"""DELETE FROM TESTSCHEDULE WHERE End > '{current_time}'"""
                conn.execute(query)
                self.log.info("Schedule table has been cleared")
        except sqlite3.error as e:
            self.log.error(f"Something went wrong with clearing the schedule table: {e}")
            raise

    def check_schedule_for_rebuild(self, channel_file):
        """
        Checks all channels in channel_file to see if the schedule needs a rebuild

        Args:
            channel_file (string): Channel file

        """

        try:
            with open(channel_file, "r") as channel_file_input:
                channel_data = json.load(channel_file_input)
            with self._connect() as conn:
                cursor = conn.cursor()
                now = datetime.now().strftime(), "%Y-%m-%d %H:%M:%S"
                for channel in channel_data:
                    channel_number = channel_data[channel]["channel_number"]
                    query = f"SELECT COUNT(*) FROM TESTSCHEDULE WHERE end > '{now}' AND Channel = {channel_number}"
                    cursor.execute(query)
                    if cursor.fetchone()[0] == 0:
                        return True
            return False
        except (sqlite3.Error, FileNotFoundError, json.JSONDecodeError) as e:
            self.log.error(f"Error checking for schedule rebuild: {e}")
            raise

    def insert_into_schedule(self, channel_number, showtime, end, filepath, chapter, runtime):
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

        query = """INSERT INTO TESTSCHEDULE (Channel, Showtime, End, Filepath, Chapter, Runtime) VALUES (?, ?, ?, ?, ?, ?)"""
        params = (
            channel_number,
            showtime.strftime("%Y-%m-%d %H:%M:%S"),
            end.strftime("%Y-%m-%d %H:%M:%S"),
            filepath,
            chapter,
            runtime
        )

        try:
            with self._connect() as conn:
                conn.execute(query, params)
            self.log.debug(f"Inserted {filepath} into the schedule for channel {channel_number}")
        except sqlite3.Error as e:
            self.log.debug(f"Failed to insert into schedule: {e}")
            raise

    def search_database(self, table, tags):
        """
        Searches the Solostation Database based on tags

        Args:
            tags (list): List of strings, search keywords/terms
            table (string): Name of the table on which to search

        Returns:
            results (list): List of dictionaries in tuple format

        Raises:
        Example:
            search_database('movie', ['action', 'movie'])
        """

        final_results = []

        for tag in tags:
            if table == "all":
                query = f"""
                    SELECT 'TV' AS source_table, json_object('ID', ID, 'Name', Name, 'ShowName', ShowName, 'Season', Season, 'Episode', Episode, 'Overview', Overview, 'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath, 'LastPlayed', LastPlayed) AS data
                    FROM TV WHERE Tags LIKE '%{tag}%' OR ShowName LIKE '%{tag}%' OR Name LIKE '%{tag}%'

                    UNION ALL

                    SELECT 'MOVIE' AS source_table, json_object('ID', ID, 'Name', Name, 'Year', Year, 'Overview', Overview, 'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath, 'LastPlayed', LastPlayed) AS data
                    FROM MOVIE WHERE Tags LIKE '%{tag}%' OR Name LIKE '%{tag}%' OR Year LIKE '%{tag}%'

                    UNION ALL

                    SELECT 'MUSIC' AS source_table, json_object('ID', ID, 'Artist', Artist, 'Title', Title, 'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath) AS data
                    FROM MUSIC WHERE Tags LIKE '%{tag}%'

                    UNION ALL

                    SELECT 'WEB' AS source_table, json_object('ID', ID,  'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath) AS data
                    FROM WEB WHERE Tags LIKE '%{tag}%'

                    UNION ALL

                    SELECT 'COMMERCIALS' AS source_table, json_object('ID', ID,  'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath, 'LastPlayed', LastPlayed) AS data
                    FROM COMMERCIALS WHERE Tags LIKE '%{tag}%';
                """
            elif table == "tv":
                query = f""" SELECT 'TV' AS source_table, json_object('ID', ID, 'Name', Name, 'ShowName', ShowName, 'Season', Season, 'Episode', Episode, 'Overview', Overview, 'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath, 'LastPlayed', LastPlayed) AS data
                    FROM TV WHERE Tags LIKE '%{tag}%' OR ShowName LIKE '%{tag}%' OR Name LIKE '%{tag}%' """
            elif table == "movie":
                query = f""" SELECT 'MOVIE' AS source_table, json_object('ID', ID, 'Name', Name, 'Year', Year, 'Overview', Overview, 'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath, 'LastPlayed', LastPlayed) AS data
                    FROM MOVIE WHERE Tags LIKE '%{tag}%' OR Name LIKE '%{tag}%' OR Year LIKE '%{tag}%' """
            elif table == "commercial":
                query = f""" SELECT 'COMMERCIALS' AS source_table, json_object('ID', ID,  'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath, 'LastPlayed', LastPlayed) AS data
                    FROM COMMERCIALS WHERE Tags LIKE '%{tag}%' """
            elif table == "web":
                query = f""" SELECT 'WEB' AS source_table, json_object('ID', ID,  'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath) AS data
                    FROM WEB WHERE Tags LIKE '%{tag}%' """
            elif table == "music":
                query = f""" SELECT 'MUSIC' AS source_table, json_object('ID', ID, 'Artist', Artist, 'Title', Title, 'Tags', Tags, 'Runtime', Runtime, 'Filepath', Filepath) AS data
                    FROM MUSIC WHERE Tags LIKE '%{tag}%' """

        
            try:
                with self._connect() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    final_results.extend(cursor.fetchall())
            except sqlite3.Error as e:
                self.log.error(f"Failed to search by tag: {e}")
                return []
        
        return final_results

    def get_chapters(self, filepath):
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

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                query = f"SELECT ID FROM TV WHERE Filepath = '{filepath}'"
                cursor.execute(query)
                episode_ID = cursor.fetchone()
                if not episode_ID:
                    return None
                episode_ID = episode_ID[0]
                query = f"SELECT Title, Start, End FROM CHAPTERS WHERE EpisodeID = '{episode_ID}'"
                cursor.execute(query)
                return cursor.fetchall()
        except sqlite3.Error as e:
            self.log.error(f"Failed to get chapters: {e}")

    def select_commercial(self, max_break):
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

        from random import choices
        max_seconds = max_break.total_seconds()

        query = """
            SELECT Filepath, Runtime, LastPlayed
            FROM COMMERCIALS
            WHERE CAST(strftime('%s', Runtime) AS INTEGER) <= ?
        """
        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (int(max_seconds),))
                commercials = cursor.fetchall()
                if not commercials:
                    self.log.warning(f"No commercials found under {max_seconds} seconds")
                    return None
                
                weighted_list = []
                now = datetime.now()
                for filepath, runtime, last_played in commercials:
                    weight = 10000 if not last_played else max((now - datetime.strptime(last_played, "%Y-%m-%d %H:%M:%S"))).total_seconds()
                    weighted_list.append((filepath, runtime, last_played))
                
                filepaths, runtimes, weights = zip(*weighted_list)

                selected_filepath = choices(filepaths, weights=weights, k=1)[0]

                # Update LastPlayed with Datetime.Now timestamp
                now_str = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
                cursor.execute(f"Update COMMERCIALS SET LastPlayed = '{now_str}' WHERE Filepath = '{selected_filepath}'")
                conn.commit()

                # Return selected commercials
                for comm in commercials:
                    if comm[0] == selected_filepath:
                        return {"Filepath": comm[0], "Runtime": comm[1], "LastPlayed": now_str}
                return None
        except sqlite3.Error as e:
            self.log.error(f"Failed to select a commercial: {e}")
            return None

    def select_random_movie(self, tags):
        """ """
        try:
            all_movies = [json.loads(data) for table, data in db_manager.search_database("movie", tags)]
            selected_movie = random.choice(all_movies)
            self.update_last_played("movie", selected_movie["Filepath"])
            return selected_movie
        except Exception as e:
            self.log.error(f"Could not select a random movie from the database: {e}")


    def select_weighted_movie(self, tags):
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

        all_movies = []
        weighted_list = []
        now = datetime.now()

        all_movies.extend([json.loads(data) for table, data in db_manager.search_database("movie", tags)])
        for movie in all_movies:
            if movie["LastPlayed"]:
                last_played = datetime.strptime(movie["LastPlayed"], "%Y-%m-%d %H:%M:%S")
                weight = 10000 if not movie["LastPlayed"] else max((now - last_played).total_seconds(), 1)
                weighted_list.append((movie["Filepath"], weight, movie["LastPlayed"], movie["Runtime"]))

        selected_movie = random.choice(sorted(weighted_list, key=lambda tup: tup[1], reverse=True)[:10])
        selected_movie = [m for m in all_movies if m["Filepath"] == selected_movie[0]][0]
        self.update_last_played("movie", selected_movie["Filepath"])
        return selected_movie

    def select_weighted_movies(self, tags):
        """
        Selects movies, filtered by tags, based on the LastPlayed datetime

        Args:
            tags (list):  Strings of tags in which to search the movie database for

        Returns:
            selected_movies (list): Sample of 20 movies

        Raises:
            None

        Example:
            select_weighted_movies(["movie", "action"])
        """

        all_movies = []
        weighted_list = []
        now = datetime.now()

        all_movies.extend([json.loads(data) for table, data in db_manager.search_database("movie", tags)])
        for movie in all_movies:
            if movie["LastPlayed"]:
                last_played = datetime.strptime(movie["LastPlayed"], "%Y-%m-%d %H:%M:%S")
                weight = 10000 if not movie["LastPlayed"] else max((now - last_played).total_seconds(), 1)
                weighted_list.append((movie["Filepath"], weight, movie["LastPlayed"], movie["Runtime"]))

        selected_movies = weighted_list[:25]
        final_selected_movies = []
        for sm in selected_movies:
            final_selected_movies.append([m for m in all_movies if m["Filepath"] == sm[0]][0])
            self.update_last_played("movie", sm[0])
        return final_selected_movies

    def update_last_played(self, table, filepath):
        """ """

        query = f"Update {table} SET LastPlayed = ? WHERE Filepath = ?"
        now_str = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
        try:
            with self._connect() as conn:
                conn.execute(query, (now_str, filepath))
            self.log.debug(f"Updated Last Played for {filepath}")
        except sqlite3.Error as e:
            self.log.error(f"Failed to update last played for {filepath}: {e}")
            raise

class Ch2Scheduler(Scheduler):
    def __init__(self, channel_number, marker, end_datetime, db_manager, tags):
        super().__init__(channel_number, marker, end_datetime, db_manager)
        self.tags = tags

    def get_next_tv_playtime(self, marker, episode_TD):
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

    def schedule(self):
        try:
            # Get all media with this channel's tags - Sample 75 items
            all_media = []
            for tag in self.tags:
                all_media.extend([json.loads(data) for table, data in db_manager.search_database(tag, self.tags)])
            random_media = random.sample(all_media, min(75, len(all_media)))

            # Randomly choose and process media
            while self.marker < self.end_datetime:
                media = random.choice(random_media)
                episode_TD = self.runtime_to_timedelta(media["Runtime"])
                table = "tv" if "tv" in media["Tags"] else "movie"
                chapters = db_manager.get_chapters(media['Filepath'])

                # Get episode block and next play time
                episode_block, next_play_time = self.get_next_tv_playtime(marker, episode_TD)

                self.log.info(f"{media['Filepath']} - {self.marker.strftime('%H:%M:%S')}")

                # Process if media contains chapters
                if chapters:
                    max_break_time = round(((episode_block - episode_TD) // len(chapters)).total_seconds(), 0)

                    for chapter in chapters:
                        # Parse chapter metadata
                        chapter_number, chapter_start, chapter_end = chapter
                        chapter_start_TD  = self.runtime_to_timedelta(chapter_start)
                        chapter_end_TD  = self.runtime_to_timedelta(chapter_end)
                        chapter_duration = chapter_end_TD - chapter_start_TD
                        
                        # Insert chapter into schedule
                        db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + chapter_duration), media["Filepath"], chapter_number, media["Runtime"])
                        self.marker += (chapter_duration + timedelta(seconds=1))
                        db_manager.update_last_played(table, media["Filepath"])

                        # Insert commercial break
                        
                        
                else:
                    db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(media["Runtime"])), media["Filepath"], None, media["Runtime"])
                    self.marker += (self.runtime_to_timedelta(media["Runtime"]) + timedelta(seconds=1))
                    db_manager.update_last_played(table, media["Filepath"])
                
                random_media.pop(random_media.index(media))

        except Exception as e:
            self.log.error(f"Failed to get random media: {e}")
            raise



class LoudScheduler(Scheduler):
    def __init__(self, channel_number, marker, end_datetime, db_manager, tags):
        super().__init__(channel_number, marker, end_datetime, db_manager)
        self.tags = tags

    def schedule(self):
        random_media_list = self._fetch_music_videos()
        all_idents = self._fetch_music_idents()

        while self.marker < self.end_datetime:
            if not random_media_list:
                random_media_list = self._fetch_music_videos()
            
            if not all_idents:
                all_idents = self._fetch_music_idents()

            for music_index, music_video in enumerate(random_media_list):
                self.log.info(f"{music_video['Filepath']} - {self.marker.strftime('%H:%M:%S')}")
                db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(music_video["Runtime"])), music_video["Filepath"], None, music_video["Runtime"])
                self.marker += (self.runtime_to_timedelta(music_video["Runtime"]) + timedelta(seconds=1))
                random_media_list.pop(random_media_list.index(music_video))

                if music_index % 2 == 0:
                    db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(all_idents[0]["Runtime"])), all_idents[0]["Filepath"], None, all_idents[0]["Runtime"])
                    self.marker += (self.runtime_to_timedelta(all_idents[0]["Runtime"]) + timedelta(seconds=1))
                    all_idents.pop(0)
                if self.marker > self.end_datetime:
                    break

    def schedule_no_ident(self):
        random_media_list = self._fetch_music_videos()

        while self.marker < self.end_datetime:
            if not random_media_list:
                random_media_list = self._fetch_music_videos()

            media = random.choice(random_media_list)
            self.log.info(f"{media['Filepath']} - {self.marker.strftime('%H:%M:%S')}")
            db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(media["Runtime"])), media["Filepath"], None, media["Runtime"])
            self.marker += (self.runtime_to_timedelta(media["Runtime"]) + timedelta(seconds=1))
            random_media_list.pop(random_media_list.index(media))

    def _fetch_music_videos(self):
        """ Fetch all music videos """
        try:
            all_music_videos = []
            all_music_videos.extend([json.loads(data) for table, data in db_manager.search_database("music", "music") if "music" in json.loads(data)["Filepath"]])
            random.shuffle(all_music_videos)
            return all_music_videos
        except Exception as e:
            self.log.error(f"Error in fetching music videos: {e}")

    def _fetch_music_idents(self):
        """ Fetch all MTV idents """
        try:
            all_idents = []
            all_idents.extend([json.loads(data) for table, data in db_manager.search_database("music", "ident") if "idents" in json.loads(data)["Filepath"]])
            random.shuffle(all_idents)
            return all_idents
        except Exception as e:
            self.log.error(f"Error in fetching MTV idents: {e}")

class BangScheduler(Scheduler):
    def __init__(self, channel_number, marker, end_datetime, db_manager, tags):
        super().__init__(channel_number, marker, end_datetime, db_manager)
        self.tags = tags

    def schedule(self):
        selected_movies = db_manager.select_weighted_movies(self.tags)
        random.shuffle(selected_movies)
        for movie in selected_movies:
            self.log.info(f"{movie['Filepath']} - {self.marker.strftime('%H:%M:%S')}")
            db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(movie["Runtime"])), movie["Filepath"], None, movie["Runtime"])
            self.marker += (self.runtime_to_timedelta(movie["Runtime"]) + timedelta(seconds=1))
            
            if self.marker > self.end_datetime:
                break

class MotionScheduler(Scheduler):
    def __init__(self, channel_number, marker, end_datetime, db_manager, tags):
        super().__init__(channel_number, marker, end_datetime, db_manager)
        self.tags = tags

    def schedule(self):
        selected_movies = db_manager.select_weighted_movies(self.tags)
        random.shuffle(selected_movies)
        for movie in selected_movies:
            self.log.info(f"{movie['Filepath']} - {self.marker.strftime('%H:%M:%S')}")
            db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(movie["Runtime"])), movie["Filepath"], None, movie["Runtime"])
            self.marker += (self.runtime_to_timedelta(movie["Runtime"]) + timedelta(seconds=1))
            
            if self.marker > self.end_datetime:
                break

class PPVScheduler(Scheduler):
    def __init__(self, channel_number, marker, end_datetime, db_manager, tags):
        super().__init__(channel_number, marker, end_datetime, db_manager)
        self.tags = tags

    def schedule(self):
        # ppv_movie = db_manager.select_weighted_movie(self.tags)
        ppv_movie = db_manager.select_random_movie(self.tags)
        self.log.debug(f"{ppv_movie}")
        while self.marker < self.end_datetime:
            self.log.info(f"{ppv_movie['Filepath']} - {self.marker.strftime('%H:%M:%S')}")
            db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(ppv_movie["Runtime"])), ppv_movie["Filepath"], None, ppv_movie["Runtime"])
            self.marker += (self.runtime_to_timedelta(ppv_movie["Runtime"]) + timedelta(seconds=1))
        
    

load_dotenv()
db_manager = DatabaseManager(os.getenv("DB_LOCATION"))
db_manager.clear_schedule_table()

with open(os.getenv("CHANNEL_FILE"), "r") as f:
    channel_data = json.load(f)

marker = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

# Channel 2
ch2_channel_data = channel_data["channel2"]
ch2_scheduler = Ch2Scheduler(
    channel_number = ch2_channel_data["channel_number"],
    marker = marker,
    end_datetime = marker + timedelta(days=1, seconds=1),
    db_manager = db_manager,
    tags = ch2_channel_data["tags"].split(", ")
)
ch2_scheduler.schedule()

# # Loud
# loud_channel_data = channel_data["loud"]
# loud_scheduler = LoudScheduler(
#     channel_number = loud_channel_data["channel_number"],
#     marker = marker,
#     end_datetime = marker + timedelta(minutes=60, seconds=1),
#     db_manager = db_manager,
#     tags = loud_channel_data["tags"]
# )
# loud_scheduler.schedule()

# # Motion
# motion_channel_data = channel_data["motion"]
# motion_scheduler = MotionScheduler(
#     channel_number = motion_channel_data["channel_number"],
#     marker = marker,
#     end_datetime = marker + timedelta(days=1, seconds=1),
#     db_manager = db_manager,
#     tags = motion_channel_data["tags"]
# )
# motion_scheduler.schedule()

# # Bang
# bang_channel_data = channel_data["bang"]
# bang_scheduler = BangScheduler(
#     channel_number = bang_channel_data["channel_number"],
#     marker = marker,
#     end_datetime = marker + timedelta(days=1, seconds=1),
#     db_manager = db_manager,
#     tags = bang_channel_data["tags"]
# )
# bang_scheduler.schedule()

# # PPV1
# ppv1_channel_data = channel_data["ppv1"]
# ppv1_scheduler = PPVScheduler(
#     channel_number = ppv1_channel_data["channel_number"],
#     marker = marker,
#     end_datetime = marker + timedelta(days=1, seconds=1),
#     db_manager = db_manager,
#     tags = ppv1_channel_data["tags"]
# )
# ppv1_scheduler.schedule()

# # PPV2
# ppv2_channel_data = channel_data["ppv2"]
# ppv2_scheduler = PPVScheduler(
#     channel_number = ppv2_channel_data["channel_number"],
#     marker = marker,
#     end_datetime = marker + timedelta(days=1, seconds=1),
#     db_manager = db_manager,
#     tags = ppv2_channel_data["tags"]
# )
# ppv2_scheduler.schedule()

# # PPV3
# ppv3_channel_data = channel_data["ppv3"]
# ppv3_scheduler = PPVScheduler(
#     channel_number = ppv3_channel_data["channel_number"],
#     marker = marker,
#     end_datetime = marker + timedelta(days=1, seconds=1),
#     db_manager = db_manager,
#     tags = ppv3_channel_data["tags"]
# )
# ppv3_scheduler.schedule()


