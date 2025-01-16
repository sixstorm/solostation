from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
import asyncio
import sqlite3
import logging
import socket
import json
import os
import time
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

# Generate FastAPI instance and mount static folder
app = FastAPI()
# app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/static", StaticFiles(directory="static", html=True), name="static")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Functions
def import_schedule():
    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()

    # Query schedule in database for given channel number 
    now = datetime.strftime(datetime.now(), "%Y-%m-%d %HH:%MM:%SS")
    cursor.execute(f"SELECT * FROM SCHEDULE ORDER BY Showtime ASC")

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

def get_media_metadata(filepath):
    conn = sqlite3.connect(os.getenv("DB_LOCATION"))
    cursor = conn.cursor()
    match filepath:
        case t if "tv" in filepath:
            cursor.execute(f"SELECT * FROM TV WHERE Filepath = '{filepath}'")
        case t if "movie" in filepath:
            cursor.execute(f"SELECT * FROM MOVIE WHERE Filepath = '{filepath}'")
        case t if "bumper" in filepath:
            cursor.execute(f"SELECT * FROM COMMERCIALS WHERE Filepath = '{filepath}'")
        case t if "music" in filepath:
            cursor.execute(f'SELECT * FROM MUSIC WHERE Filepath = "{filepath}"')
        case t if "web" in filepath:
            cursor.execute(f'SELECT * FROM WEB WHERE Filepath = "{filepath}"')
        case _:
            log.debug(f"No metadata found for {filepath}")

    metadata = cursor.fetchone()
    conn.close()
    return metadata

def update_data(schedule):
    now = datetime.now()
    data = []

    all_playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]]
    all_playing_now = sorted(all_playing_now, key=lambda c: c["channel"])
    for item in all_playing_now:
        playing_now_metadata = get_media_metadata(item["filepath"])

        # Convert channel number to name
        match item["channel"]:
            case 2:
                channel_name = "channel2"
            case 3:
                channel_name = "Loud!"
            case 4:
                channel_name = "motion"
            case 5:
                channel_name = "PPV1"
            case 6:
                channel_name = "BANG!"

        remaining_time = (item["end"] - now).total_seconds() if now < item["end"] else 0
        hours, remainder = divmod(remaining_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        formatted_time = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        if "tv" in item["filepath"]:
            playing_now_title = f"{playing_now_metadata[2]} - {playing_now_metadata[1]}"
        elif "movies" in item["filepath"]:
            playing_now_title = f"{playing_now_metadata[1]}"
        elif "music" in item["filepath"]:
            playing_now_title = f"{playing_now_metadata[2]} - {playing_now_metadata[3]}"
        elif "web" in item["filepath"]:
            playing_now_title = f"{playing_now_metadata[3]}"
        elif "bumpers" in item["filepath"]:
            playing_now_title = f"{playing_now_metadata[3]}"
        else:
            playing_now_title = "commercials"


        input_data = {
            "channel_number": item["channel"],
            "channel_name": channel_name,
            "playing_now_title": playing_now_title,
            "time_remaining": formatted_time
        }

        data.append(input_data)
    
    return data

schedule = import_schedule()
# now = datetime.now()
# playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"]]
# log.debug(sorted(playing_now, key=lambda c: c["channel"]))


# Serve HTML
@app.get("/")
async def get():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Solostation Dashboard</title>
        <link rel="stylesheet" type="text/css" href="/static/css/styles.css">
    </head>
    <body>
        <h1>Solostation Dashboard</h1>
        <div id="channels"></div>
        <script>
            const ws = new WebSocket("ws://localhost:8086/ws");

            ws.onmessage = function(event) {
                const channels = JSON.parse(event.data);
                const container = document.getElementById("channels");
                container.innerHTML = ""; // Clear previous content

                channels.forEach(channel => {
                    const channelDiv = document.createElement("div");
                    channelDiv.className = "channel";
                    channelDiv.innerHTML = `
                        <div class="channel-title">${channel.channel_number} - ${channel.channel_name}</div>
                        <div>Now Playing: ${channel.playing_now_title}</div>
                        <div>Time Remaining: ${channel.time_remaining} seconds</div>
                    `;
                    container.appendChild(channelDiv);
                });
            };

            ws.onclose = function() {
                console.log("WebSocket connection closed");
            };
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

# WebSocket for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        try:
            data = update_data(schedule)
            await websocket.send_json(data)
            await asyncio.sleep(1)  # Update every second
        except Exception as e:
            print(f"WebSocket error: {e}")
            break
    await websocket.close()