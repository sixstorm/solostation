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

## Last Update:  2025-01-08
Playback is currently under testing, which means allowing it to run for most of the day and checking it every hour, making sure things are on schedule.  There are some bugs, but I'm working on them.  Once playback is solid, I'll add the rest of the channels back, work on the MTV info cards, and some other items to get to a "0.1" state.

Media Management and Scheduling is basically done.  I'm currently re-writing my code to fit better Python practices, while simplifying some of the complicated things.  Most of my focus now is on the main script that plays the schedule, give music information cards on screen at the proper time, etc.  Finally, once all of this is set in stone, I will spend my remaining time adding more content!