import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP  TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songsplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EEXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
event_id INT IDENTITY(0,1),
artist_name VARCHAR(255),
auth VARCHAR(50),
user_first_name VARCHAR(255),
user_gender VARCHAR(1),
item_in_session INT,
user_last_name VARCHAR(255),
song_length DOUBLE PRECISION,
user_level VARCHAR(50),
location VARCHAR (255),
method VARCHAR(25),
page VARCHAR(35),
registration VARCHAR(50),
session_id BIGINT,
song_title VARCHAR(255),
status INTEGER, 
ts VARCHAR(50),
user_agent TEXT,
user_id VARCHAR(100),
PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
song_id VARCHAR(100),
num_songs INTEGER,
artist_id VARCHAR(100),
artist_latitude DOUBLE PRECISION,
artist_longitude DOUBLE PRECISION,
artist_location VARCHAR(255),
artist_name VARCHAR(255),
title VARCHAR(255),
duration DOUBLE PRECISION,
year INTEGER,
PRIMARY KEY (song_id))
""")
songplay_table_create = ("""CREATE TABLE songsplay\
(songsplay_id serial PRIMARY KEY,\
start_time timestamp NOT NULL,\
user_id int NOT NULL,\
level varchar NOT NULL,\
song_id varchar,\
artist_id varchar,\
session_id int NOT NULL,\
location varchar NOT NULL,\
user_agent varchar NOT NULL)""")

user_table_create = ("""CREATE TABLE users\
(user_id int PRIMARY KEY,\
first_name varchar NOT NULL,\
last_name varchar NOT NULL,\
gender varchar NOT NULL,\
level varchar NOT NULL)""")

song_table_create = (""" CREATE TABLE songs\
(song_id varchar PRIMARY KEY,\
title varchar NOT NULL,\
artist_id varchar NOT NULL,\
year int NOT NULL,\
duration numeric NOT NULL)""")

artist_table_create = ("""CREATE TABLE artists\
(artist_id varchar PRIMARY KEY,\
name varchar NOT NULL,\
location varchar NOT NULL,\
latitude numeric,\
longitude numeric)""")

time_table_create = ("""CREATE TABLE time\
(start_time timestamp PRIMARY KEY,\
hour int NOT NULL,\
day int NOT NULL,\
week int NOT NULL,\
month int NOT NULL,\
year int NOT NULL,\
weekday int NOT NULL)""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {};
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songsplay_table_insert = ("""
INSERT INTO songsplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
e.user_id, 
e.level,
s.song_id,
s.artist_id,
e.session_id,
e.location,
e.user_agent
FROM staging_events e
LEFT JOIN staging_song s ON
e.song = s.title AND
e.artist = s.artist_name AND
ABS(e.length - s.duration) < 2
WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id, artist, first_name, last_name, gender, level)
SELECT DISTINCT
user_id,
first_name,
last_name,
gender,
level
FROM staging_events WHERE
page = 'NextSong'

""")

songs_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
user_id, 
title,
artist_id, 
year,
duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artists_table_insert = ("""
INSERT INTO artists(artist_id, name, location, lattitude, longitude)
SELECT DISTINCT

artist_id,
name,
location,
lattitude,
longitude
FROM staging_songs 
WHERE artist_id IS NOT NULL

""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weeekday)
SELECT start_time,
EXTRACT (hour from start_time),
EXTRACT (day from start_time),
EXTRACT (week from start_time),
EXTRACT (month from start_time),
EXTRACT (year from start_time),
EXTRACT (dayofweek form start_time)
FROM staging_songs
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
