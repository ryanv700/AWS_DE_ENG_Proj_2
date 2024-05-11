import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# Below we create 7 tables, 2 staging and 5 for the final STAR schema

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    userAgent VARCHAR,
    userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INT
);
""")

# This is the fact table for our final schema

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR
);
""")

# Below are the 4 dimension tables for our final schema

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT,
    duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES
# In these queries we will COPY from the existing Udacity S3 buckets to our staging tables
# we will read the S3 buckets in from dwh.cfg using config parser 
# Amazon redshift COPY command also has REGION as a possible parameter if we want to change the region

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT AS JSON {}
REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH']
           )


staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'],
           )


# FINAL TABLES
# In the final step we insert from the staging tables into our final STAR schema tables

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
        e.userId AS user_id,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId AS session_id,
        e.location,
        e.userAgent AS user_agent
    FROM staging_events e
    JOIN staging_songs s ON (e.song = s.title AND e.artist = s.artist_name)
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour FROM start_time) AS hour,
        EXTRACT(day FROM start_time) AS day,
        EXTRACT(week FROM start_time) AS week,
        EXTRACT(month FROM start_time) AS month,
        EXTRACT(year FROM start_time) AS year,
        EXTRACT(weekday FROM start_time) AS weekday
    FROM staging_events
    WHERE page = 'NextSong'
""")


# Test queries to test the STAR Schema after final etl.py run

most_played_10_songs = ("""
    SELECT song_id, COUNT(*) as play_count
    FROM songplays
    GROUP BY song_id
    ORDER BY play_count DESC
    LIMIT 5;
""")

hour_of_the_day_with_most_activity = ("""
    SELECT hour, COUNT(*) as play_count
    FROM time
    GROUP BY hour
    ORDER BY play_count DESC
    LIMIT 5;
""")

most_popular_10_artists = ("""
    SELECT name as artist_name, COUNT(*) as play_count
    FROM artists
    JOIN songplays ON artists.artist_id = songplays.artist_id
    GROUP BY name
    ORDER BY play_count DESC
    LIMIT 5;
""")

most_popular_10_recording_years = ("""
    SELECT s.year as year_song_recorded, COUNT(*) as play_count
    FROM songs s
    JOIN songplays sp ON s.song_id = sp.song_id
    GROUP BY s.year
    ORDER BY play_count DESC
    LIMIT 5;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
final_test_queries = [most_played_10_songs, hour_of_the_day_with_most_activity, most_popular_10_artists, most_popular_10_recording_years]



