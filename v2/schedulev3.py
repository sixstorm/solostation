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

    def seconds_to_hms(self, seconds):
        """ Converts number of seconds to time formatted string """
        s = seconds % 60
        m = (seconds//60) % 60
        h = seconds//3600
        return ( "%02d:%02d:%02d" % (h,m,s) )


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
                # query = f"""DELETE FROM TESTSCHEDULE WHERE End > '{current_time}'"""
                query = """DELETE FROM TESTSCHEDULE"""
                conn.execute(query)
                conn.commit()
                self.log.info("Schedule table has been cleared")
        except sqlite3.Error as e:
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

        # query = """
        #     SELECT Filepath, Runtime, LastPlayed
        #     FROM COMMERCIALS
        #     WHERE CAST(strftime('%s', Runtime) AS INTEGER) <= ?
        # """
        query = """
        SELECT Filepath, Runtime, LastPlayed
        FROM COMMERCIALS
        WHERE (
            CAST(SUBSTR(Runtime, 1, 2) AS INTEGER) * 3600 +  -- Hours to seconds
            CAST(SUBSTR(Runtime, 4, 2) AS INTEGER) * 60 +    -- Minutes to seconds
            CAST(SUBSTR(Runtime, 7, 2) AS INTEGER)           -- Seconds
        ) <= ?
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
                    weight = 10000 if not last_played else round((now - datetime.strptime(last_played, "%Y-%m-%d %H:%M:%S")).total_seconds())
                    weighted_list.append((filepath, runtime, weight))
                
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

    def select_web_content(self, max_break):
        """ """

        max_seconds = max_break.total_seconds()
        query = f""" SELECT * 
        FROM WEB 
        WHERE (
            CAST(SUBSTR(Runtime, 1, 2) AS INTEGER) * 3600 +  -- Hours to seconds
            CAST(SUBSTR(Runtime, 4, 2) AS INTEGER) * 60 +    -- Minutes to seconds
            CAST(SUBSTR(Runtime, 7, 2) AS INTEGER)           -- Seconds
        ) <= ?
        """

        try:
            with self._connect() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (int(max_seconds),))
                all_web_media = cursor.fetchall()
                if not all_web_media:
                    self.log.warning(f"No web content found under {max_seconds} seconds")
                    return None

            random.shuffle(all_web_media)
            return all_web_media
        except Exception as e:
            self.log.error(f"Could not get web content: {e}")


    def select_random_movie(self, tags):
        """
        Selects a single, random movie from the database

        Args:
            tags (list): Strings of tags in which to search the movie database for

        Returns:
            selected_movie (dict):  Randomly selected movie

        Raises:
        
        """
        try:
            all_movies = [json.loads(data) for table, data in db_manager.search_database("movie", tags)]
            selected_movie = random.choice(all_movies)
            self.update_last_played("movie", selected_movie["Filepath"])
            return selected_movie
        except Exception as e:
            self.log.error(f"Could not select a random movie from the database: {e}")
            raise


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

    def get_next_tv_playtime(self, episode_TD):
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

        if episode_TD < timedelta(0):
            raise ValueError("Episode duration cannot be negative")

        self.log.info(f"{self.marker=}")
        
        # Round current time to nearest second
        current_time = self.marker.replace(second=0, microsecond=0)
        minutes_since_midnight = current_time.hour * 60 + current_time.minute
        
        # Define half-hour slots
        half_hour_slots = [0, 30]
        
        # Adjust for exact hour/half-hour matches
        if minutes_since_midnight % 30 == 0:  # On the hour or half-hour
            if minutes_since_midnight % 60 == 0:  # On the hour (00)
                next_minutes = 30
                hours = minutes_since_midnight // 60
            else:  # On the half-hour (30)
                next_minutes = 0
                hours = (minutes_since_midnight // 60) + 1
        else:
            # Find the next half-hour slot
            for slot in half_hour_slots:
                if minutes_since_midnight < slot:
                    next_minutes = slot
                    hours = minutes_since_midnight // 60
                    break
            else:
                next_minutes = 0
                hours = (minutes_since_midnight // 60) + 1

        # Set next play time with safe date handling
        try:
            next_play_time = current_time.replace(hour=hours % 24, 
                                                minute=next_minutes, 
                                                second=0, 
                                                microsecond=0)
        except ValueError:
            base_date = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            next_play_time = base_date + timedelta(days=1, 
                                                hours=hours % 24, 
                                                minutes=next_minutes)

        # Adjust for multi-day rollover
        if hours >= 24:
            next_play_time += timedelta(days=hours // 24)

        # Calculate actual time to next slot
        time_to_next = next_play_time - current_time

        # Set episode_block based on episode duration
        if episode_TD < timedelta(minutes=30):
            episode_block = timedelta(minutes=30)
        else:
            episode_block = timedelta(hours=1)
            if time_to_next <= timedelta(minutes=30):
                next_play_time += timedelta(minutes=30)  # Extend to next slot if too close

        self.log.info(f"episode_block={episode_block}, next_play_time={next_play_time}")
        return episode_block, next_play_time

    def get_next_movie_playtime(self, movie_TD):
        """
        Determine the next available play time for the next item after a movie.
        Next play time is always on the hour (00) or half-hour (30) after the movie ends.

        Args:
            movie_TD (timedelta): Duration of the movie

        Returns:
            tuple: (movie_end: datetime, next_play_time: datetime)
                - movie_end: When the current movie ends
                - next_play_time: Next available start time (on :00 or :30)

        Raises:
            ValueError: If movie_TD is negative

        Example:
            end, next_time = get_next_movie_playtime(timedelta(hours=2))
        """
        if movie_TD < timedelta(0):
            raise ValueError("Movie duration cannot be negative")

        self.log.info(f"{self.marker=}")
        
        # Calculate movie end time
        movie_end = (self.marker + movie_TD).replace(second=0, microsecond=0)
        self.log.info(f"movie_end={movie_end}")

        # Calculate minutes since midnight from movie end
        minutes_since_midnight = movie_end.hour * 60 + movie_end.minute
        
        # Define half-hour slots
        half_hour_slots = [0, 30]
        
        # Adjust for exact hour/half-hour matches at movie end
        if minutes_since_midnight % 30 == 0:  # On the hour or half-hour
            if minutes_since_midnight % 60 == 0:  # On the hour (00)
                next_minutes = 30
                hours = minutes_since_midnight // 60
            else:  # On the half-hour (30)
                next_minutes = 0
                hours = (minutes_since_midnight // 60) + 1
        else:
            # Find the next half-hour slot after movie end
            for slot in half_hour_slots:
                if minutes_since_midnight < slot:
                    next_minutes = slot
                    hours = minutes_since_midnight // 60
                    break
            else:
                next_minutes = 0
                hours = (minutes_since_midnight // 60) + 1

        # Set next play time with safe date handling
        try:
            next_play_time = movie_end.replace(hour=hours % 24, 
                                            minute=next_minutes, 
                                            second=0, 
                                            microsecond=0)
        except ValueError:
            base_date = movie_end.replace(hour=0, minute=0, second=0, microsecond=0)
            next_play_time = base_date + timedelta(days=1, 
                                                hours=hours % 24, 
                                                minutes=next_minutes)

        # Adjust for multi-day rollover
        if hours >= 24:
            next_play_time += timedelta(days=hours // 24)

        # Log the time to next slot
        time_to_next = next_play_time - movie_end
        self.log.info(f"time_to_next={time_to_next.total_seconds()} seconds")
        self.log.info(f"next_play_time={next_play_time}")

        return next_play_time

    def get_next_movie_playtime2(self, movie_TD):
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

        if movie_TD < timedelta(0):
            raise ValueError("Movie duration cannot be negative")

        self.log.info(f"{self.marker=}")
        # Calculate end time of current movie and round down to nearest minute
        post_marker = (self.marker + movie_TD).replace(second=0, microsecond=0)
        self.log.info(f"{post_marker=}")

        # Convert to minutes since midnight for easier calculation
        minutes_since_midnight = post_marker.hour * 60 + post_marker.minute
        
        # Define quarter-hour slots (0:00, 0:15, 0:30, 0:45)
        quarter_hours = [0, 15, 30, 45]
        
        # Find next quarter hour that's at least 15 minutes after movie ends
        for i, quarter in enumerate(quarter_hours):
            if minutes_since_midnight < quarter:
                if i > 0 and (quarter - minutes_since_midnight) < 15:
                    continue  # Ensure at least 15-minute gap
                next_minutes = quarter
                break
        else:
            # If we're past all quarter hours, go to next hour's first slot
            next_minutes = 0
            post_marker += timedelta(hours=1)

        # Calculate hours and adjust for day rollover
        hours = minutes_since_midnight // 60
        if next_minutes < minutes_since_midnight % 60:
            hours += 1

        # Handle day/month/year rollover safely
        try:
            next_play_time = post_marker.replace(hour=hours % 24, 
                                            minute=next_minutes, 
                                            second=0, 
                                            microsecond=0)
        except ValueError:
            # If we hit a month boundary, add days instead
            base_date = post_marker.replace(hour=0, minute=0, second=0, microsecond=0)
            next_play_time = base_date + timedelta(days=1, 
                                                hours=hours % 24, 
                                                minutes=next_minutes)

        # Ensure we're not crossing midnight incorrectly
        if hours >= 24:
            next_play_time += timedelta(days=hours // 24)

        # Verify we have at least 15 minutes gap
        gap = next_play_time - post_marker
        if gap < timedelta(minutes=15):
            next_play_time += timedelta(minutes=15 - gap.total_seconds() // 60)

        self.log.info(f"{next_play_time=}")
        return next_play_time

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

                self.log.info(f"{media['Filepath']} - {self.marker.strftime('%H:%M:%S')}")
                self.log.info(f"{table=}")

                # Get episode block and next play time
                episode_block, next_play_time = self.get_next_tv_playtime(episode_TD)
                if table == "tv":
                    episode_block, next_play_time = self.get_next_tv_playtime(episode_TD)
                    self.log.info(f"Next TV Play Time: {next_play_time}")
                if table == "movie":
                    next_play_time = self.get_next_movie_playtime(episode_TD)
                    self.log.info(f"Next Movie Play Time: {next_play_time}")


                # Process if media contains chapters
                if chapters:
                    max_break_time = timedelta(seconds=round(((episode_block - episode_TD) // len(chapters)).total_seconds(), 0))

                    for chapter in chapters:
                        # Parse chapter metadata
                        chapter_number, chapter_start, chapter_end = chapter
                        chapter_start_TD  = self.runtime_to_timedelta(chapter_start)
                        chapter_end_TD  = self.runtime_to_timedelta(chapter_end)
                        chapter_duration = chapter_end_TD - chapter_start_TD
                        
                        # Insert chapter into schedule
                        db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + chapter_duration), media["Filepath"], chapter_number, media["Runtime"])
                        self.marker += (chapter_duration + timedelta(seconds=1))
                        self.log.info(f"Chapter added: {self.marker}")
                        db_manager.update_last_played(table, media["Filepath"])

                        # Insert commercial break
                        if int(chapter_number) < len(chapters):
                            # Standard commercial break
                            self._standard_comm_break(max_break_time)
                        else:
                            # Post episode commercial break
                            self._final_comm_break(next_play_time)
                else:
                    db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(media["Runtime"])), media["Filepath"], None, media["Runtime"])
                    self.marker += (self.runtime_to_timedelta(media["Runtime"]) + timedelta(seconds=1))
                    db_manager.update_last_played(table, media["Filepath"])
                    self._final_comm_break(next_play_time)

                    # Post episode commercials
                    # if table == "tv":
                    #     self.log.info(f"Going into FCB: {self.marker.strftime('%H:%M:%S')}")
                    #     self._final_comm_break(next_play_time)
                    # if table == "movie":
                    #     self.log.info(f"Going into FCB: {self.marker.strftime('%H:%M:%S')}")
                    #     self._add_post_movie(next_play_time)

                
                random_media.pop(random_media.index(media))

        except Exception as e:
            self.log.error(f"Failed to get random media: {e}")
            raise

    def _standard_comm_break(self, max_break_time):
        """
        Creates a commercial break between episode chapters

        Args:
            max_break_time (timedelta):  Seconds of largest possible commercial break time

        Returns:
            None

        Raises:
            None

        Example:
            post_episode(next_play_time, channel_number)
        """

        self.log.info(f"Entering standard commercial break")
        try:
            while max_break_time > timedelta(seconds=14):
                commercial = db_manager.select_commercial(max_break_time)
                commercial_TD = self.runtime_to_timedelta(commercial["Runtime"])
                db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(commercial["Runtime"])), commercial["Filepath"], None, commercial["Runtime"])
                self.marker += (self.runtime_to_timedelta(commercial["Runtime"]) + timedelta(seconds=1))
                max_break_time -= commercial_TD
        except Exception as e:
            self.log.error(f"Error adding a standard commercial break: {e}")
            raise

    def _final_comm_break(self, next_play_time):
        """
        Fills the remaining timeblock with commercials post episode

        Args:
            next_play_time (datetime):  When next item is to play

        Returns:
            None

        Raises:
            None

        Example:
            post_episode(next_play_time, channel_number)
        """

        try:
            self.log.info(f"FCB Entry: {self.marker.strftime('%H:%M:%S')}")
            self.log.info(f"FCB NPT: {next_play_time.strftime('%H:%M:%S')}")
            max_break_time = next_play_time - self.marker
            self.log.info(f"FCB MBT: {max_break_time}")
            while self.marker < (next_play_time - timedelta(minutes=1)):
                commercial = db_manager.select_commercial(max_break_time)
                commercial_TD = self.runtime_to_timedelta(commercial["Runtime"])
                db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + self.runtime_to_timedelta(commercial["Runtime"])), commercial["Filepath"], None, commercial["Runtime"])
                self.marker += (self.runtime_to_timedelta(commercial["Runtime"]) + timedelta(seconds=1))
            self.log.info(f"FCB PA: {self.marker.strftime('%H:%M:%S')} - {next_play_time}")
            self._add_filler(next_play_time)
            self.marker = next_play_time
        except Exception as e:
            self.log.warning(f"Error adding the final commercial break: {e}")

    def _add_post_movie(self, next_play_time):
        """ Add web content until next play time """

        self.log.info("Adding post movie")
        try:
            while self.marker < (next_play_time - timedelta(minutes=3)):
                try:
                    media = db_manager.select_web_content(next_play_time - self.marker)[0]
                    m_id, table, runtime, filepath = media
                    media_TD = self.runtime_to_timedelta(runtime)
                    self.log.info(f"Chose web - {filepath}-{runtime}")
                    db_manager.insert_into_schedule(self.channel_number, self.marker, (self.marker + media_TD), filepath, None, runtime)
                    self.marker += (media_TD + timedelta(seconds=1))
                except IndexError as e:
                    self._add_filler(next_play_time)
            self._add_filler(next_play_time)
        except Exception as e:
            self.log.error(f"Could not add post movie content: {e}")
            raise

    def _add_filler(self, next_play_time):
        """ """

        self.log.info(f"Adding final filler from {self.marker.strftime('%H:%M:%S')} until {next_play_time.strftime('%H:%M:%S')}")
        try:
            time_remaining = self.seconds_to_hms((next_play_time - self.marker).total_seconds())
            db_manager.insert_into_schedule(self.channel_number, self.marker, next_play_time, os.getenv("FILLER_VIDEO"), None, time_remaining)
            self.marker = next_play_time
        except Exception as e:
            self.log.error(f"Could not add final filler: {e}")
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


