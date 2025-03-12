# SoloStation

> "An exciting, new cable experience!"

SoloStation is a self made, cable TV station that provides yet another purpose to my growing library of TV shows, movies, music videos, and other web content.  It's another way to enjoy my content with a side of nostolgia.

Here are the current channels available:

- channel2 - A scheduled mix of **all** media, including commercials from the 70s-00s.
- Loud! - A clone of MTV, including some classic MTV idents and info "cards".
- BANG - All action movies, all day, no commercials.
- PPV1-3 - Classic Pay Per View experience.  One movie, plays back to back all day.
- Motion - A random mix of movies all day, no commercials.

This is designed with specific software and hardware requirements:

- Designed for a Raspberry Pi 4b with the latest Raspbian version installed
- 1080P display max (4K is a no go for the Pi)
- All content must be pre-formatted in either MP4 or MKV format (see below)

# Current Status

## Latest Update:
### 2025-03-12

After running a few experiments with some "assisted AI" coding, I decided to keep the code where it is and just run with it for now.  All channels work, music videos now show the artist and song title during playback, scheduling doesn't appear to have any bugs; it is at a point now where I could just simply watch it.  Schedules will need to be rebuilt every day, but that only takes ~15 seconds.  I've also added a feature where the last viewed channel will remain persistant the next time you run the play script (or if it crashes).  Check out my latest blog post [here](https://blog2.teamtuck.xyz/blog/solostation-release/).

### 2025-02-19
Testing a way to extend the schedule past the next day so that playback can continue "indefinitely".  This may or may not make it to the feature freeze but I thought I would try anyway.

### 2025-02-18
Repo has been cleaned up, so everything is now under "v2".  Pay-per-view channels 2 and 3 were added.  Getting close to a feature freeze and then directing my attention over to the hardware side of things, then work on ironing out any bugs or issues.

# Media Layout/Organization
For mass storage, I have an EXT4 formatted 2TB SSD connected to the Raspberry Pi; Raspbian auto mounts the drive to /media/USERNAME/usb.  Here is the file/folder org for this drive (don't judge my folder naming too harshly):

├── bumpers
├── database
├── fonts
├── movies
├── music
├── tv
├── web

## bumpers
All commercials

## database 
A single SQLite3 database

## fonts
All fonts files

## movies
Each movie has its own folder in "Title (Year)" format.  For example, "Batman (1989)".

## tv 
Each series has its own folder in "Series Name (Year)" format; i.e. "Friends (1994)".  Each series has subfolders for each season; i.e. "Season 1".  Finally, each episode is in a specific format for season and episode number; i.e. "S01E01.mp4".