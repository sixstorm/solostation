# SoloStation

> "An exciting, new cable experience!"

SoloStation is a self made, cable TV station that provides yet another purpose to my growing library of TV shows, movies, music videos, and other web content.  

Here are the current channels planned to be available:

- channel2 - A scheduled mix of all media, including commercials from the 70s-00s.
- Loud! - A clone of MTV, including some classic MTV idents and info "cards".
- BANG - All action movies, no commercials.
- PPV1 - Classic Pay Per View experience.  One movie, plays back to back all day.
- Motion - A random mix of movies all day, no commercials.

This is designed with specific software and hardware requirements:

- Designed for a Raspberry Pi 4b with a 1080P display (4K is a no go for the Pi).
- All content must be pre-formatted (more info coming soon).

# Current Status

## Last Update:
### 2025-01-21
Running more tests; this time importing new media and seeing how the Media Manager would handle it.  Other than some basic error handling, everything was perfect.  "Loud!" is showing Artist and Title information (MTV info cards) but still needs some work.  Finally, I have implemented channel changing using "w" and "s" keyboard keys.  This works enough for now.

### 2025-01-15
Running these scripts over the last few days has shown that everything is running right on time.  However, the transition from one video to another is a bit rough looking.  I'm also testing a new function that will choose commercials differently so that you won't see the same thing twice in a single break.

### 2025-01-09
Additional channels have been added and ran through some thorough testing with scheduling; everything appears to be doing exactly as it should.  I will be letting this run throughout the next few days to verify that things are playing as scheduled.  Also, I want to do some work on the admin console (APITest.py) to have a tool that will help me validate playback.

### 2025-01-08
Playback is currently under testing, which means allowing it to run for most of the day and checking it every hour, making sure things are on schedule.  There are some bugs, but I'm working on them.  Once playback is solid, I'll add the rest of the channels back, work on the MTV info cards, and some other items to get to a "0.1" state.

Media Management and Scheduling is basically done.  I'm currently re-writing my code to fit better Python practices, while simplifying some of the complicated things.  Most of my focus now is on the main script that plays the schedule, give music information cards on screen at the proper time, etc.  Finally, once all of this is set in stone, I will spend my remaining time adding more content!