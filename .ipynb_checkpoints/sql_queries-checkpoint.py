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

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                event_id int IDENTITY(0,1),
                                artist varchar,
                                auth varchar,
                                firstName varchar,
                                gender varchar,
                                iteminSession varchar,
                                lastName varchar,
                                length varchar,
                                level varchar,
                                location varchar,
                                method varchar,
                                page varchar,
                                registration varchar,
                                sessionId varchar,
                                song varchar,
                                status varchar,
                                ts varchar,
                                userAgent varchar,
                                userId varchar); 
                            """)

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                    num_songs int, 
                                    artist_id varchar, 
                                    artist_latitude varchar, 
                                    artist_longitude varchar, 
                                    artist_location varchar, 
                                    artist_name varchar,
                                    song_id varchar PRIMARY KEY, 
                                    title varchar, 
                                    duration decimal, 
                                    year int);""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                            songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
                            start_time decimal NOT NULL, 
                            user_id varchar, 
                            level varchar, 
                            song_id varchar, 
                            artist_id varchar, 
                            session_id varchar, 
                            location varchar, 
                            user_agent varchar);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id varchar PRIMARY KEY, 
                        first_name varchar, 
                        last_name varchar, 
                        gender varchar, 
                        level varchar);
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id varchar PRIMARY KEY, 
                        title varchar, 
                        artist_id varchar, 
                        year int, 
                        duration decimal NOT NULL);
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                        artist_id varchar PRIMARY KEY, 
                        name varchar, 
                        location varchar, 
                        latitude decimal, 
                        longitude decimal);
                        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        ts varchar,
                        start_time timestamp, 
                        hour int, 
                        day int, 
                        week int, 
                        month int,
                        year int,
                        weekday int);
                    """)

# STAGING TABLES

staging_events_copy = ("""
    
    COPY staging_events 
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}' 
    REGION 'us-west-2'
    FORMAT AS JSON '{}';
    """).format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
    
    COPY staging_songs 
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}' 
    REGION 'us-west-2'
    JSON 'auto';
    """).format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(
                    start_time, 
                    user_id, 
                    level, 
                    song_id, 
                    artist_id, 
                    session_id, 
                    location, 
                    user_agent)
    SELECT staging_events.ts as start_time,
            staging_events.userID as userId,
            staging_events.level as level,
            staging_songs.song_id as song_id,
            staging_songs.artist_id as artist_id,
            staging_events.sessionId as session_id,
            staging_events.location as location,
            staging_events.userAgent as user_agent
    FROM staging_events
    JOIN staging_songs ON staging_events.song = staging_songs.title AND staging_events.artist = staging_songs.artist_name
    WHERE staging_events.page = 'NextSong'
        AND staging_events.userId NOT IN (SELECT DISTINCT s.user_id 
                            FROM songplays as s 
                            WHERE s.user_id = user_id AND s.start_time = start_time AND s.session_id = session_id )
        AND staging_events.ts != 'null'
        AND staging_events.ts IS NOT null
""")

user_table_insert = ("""
    INSERT INTO users (
                        user_id, 
                        first_name, 
                        last_name, 
                        gender, 
                        level)
    SELECT DISTINCT userId as user_id,
                    firstName as first_name,
                    lastName as last_name,
                    gender,
                    level
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
    """)

song_table_insert = ("""
    INSERT INTO songs (
                song_id, 
                title, 
                artist_id, 
                year, 
                duration)
    SELECT song_id,
            title,
            artist_id,
            year, 
            duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
    AND duration IS NOT null
""")

artist_table_insert = ("""
    INSERT INTO artists (
                    artist_id, 
                    name, 
                    location, 
                    latitude, 
                    longitude)
    SELECT DISTINCT
            artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
INSERT INTO time (
                ts,
                start_time, 
                hour, 
                day, 
                week, 
                month,
                year,
                weekday)
SELECT  ts as ts,
        timestamp 'epoch' + (ts::bigint/1000) * interval '1 second' as start_time, 
        EXTRACT(hour FROM start_time) as hour,
        EXTRACT(day FROM start_time) as day,
        EXTRACT(week FROM start_time) as week,
        EXTRACT(month FROM start_time) as month,
        EXTRACT(year FROM start_time) as year,
        EXTRACT(weekday FROM start_time) as weekday
FROM(
  SELECT DISTINCT ts
  FROM staging_events
  WHERE ts IS NOT null)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
