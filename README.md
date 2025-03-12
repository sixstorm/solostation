# SoloStation - v3

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

The v3 branch is "experimental", as I've had a little help from AI on getting things together.  This is more for me, gaining a deeper understanding of Python.  The main branch still works and I will continue to improve on it as I have time.

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