from fastapi import FastAPI, WebSocket
from fastapi.responses import HTMLResponse
from datetime import datetime, timedelta
import asyncio
import sqlite3
import logging

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

# Open Database
conn = sqlite3.connect("/media/ascott/USB/database/solodb.db")
cursor = conn.cursor()

app = FastAPI()

# Functions
def import_schedule():
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

    return all_scheduled_items


# Helper function to format channel data
def get_channel_data():
    now = datetime.now()
    data = []

    # Query for unique channel numbers
    cursor.execute("SELECT DISTINCT Channel FROM SCHEDULE")
    channels = list(cursor.fetchall()[0])

    schedule = import_schedule()
    
    for channel in channels:
        # Get now playing for this channel
        playing_now = [s for s in schedule if now >= s["showtime"] and now < s["end"] and s["channel"] == channel][0]

        try:
            playing_now_index = schedule.index(playing_now)
            playing_next = schedule[playing_now_index + 1]
        except IndexError:
            playing_next = None


        remaining_time = (playing_now["end"] - now).total_seconds() if now < playing_now["end"] else 0
        hours, remainder = divmod(remaining_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        formatted_time = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        if playing_now["chapter"] is not None:
            current_title = f"{playing_now['filepath']} - Chapter {playing_now['chapter']}"
        else:
            current_title = f"{playing_now['filepath']}"


        if playing_next["chapter"] is not None:
            next_title = f"{playing_next['filepath']} - Chapter {playing_next['chapter']}"
        else:
            next_title = f"{playing_next['filepath']}"


        data.append({
            "channel_number": playing_now["channel"],
            "current_title": current_title,
            "time_remaining": formatted_time,
            "next_title": next_title,
            "next_start_at": str(playing_next["showtime"])
        })
    
    return data

schedule = import_schedule()

# Serve HTML
@app.get("/")
async def get():
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Channel Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; }
            .channel { margin-bottom: 20px; }
            .channel-title { font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>Channel Dashboard</h1>
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
                        <div class="channel-title">Channel ${channel.channel_number}</div>
                        <div>Now Playing: ${channel.current_title}</div>
                        <div>Time Remaining: ${channel.time_remaining} seconds</div>
                        <div>Next: ${channel.next_title}</div>
                        <div>Starts At: ${channel.next_start_at}</div>
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
            data = get_channel_data()
            await websocket.send_json(data)
            await asyncio.sleep(1)  # Update every second
        except Exception as e:
            print(f"WebSocket error: {e}")
            break
    await websocket.close()
