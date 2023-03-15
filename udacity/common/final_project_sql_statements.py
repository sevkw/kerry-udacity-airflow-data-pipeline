class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO fct_songplays (songplay_id, start_time, userid, level, song_id, artist_id, sessionid, location, useragent)
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM stage_events
            WHERE page='NextSong') events
            LEFT JOIN stage_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        INSERT INTO dim_users (user_id, firstname, lastname, gender, level)
        SELECT distinct userid, firstname, lastname, gender, level
        FROM stage_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        INSERT INTO dim_songs (song_id, title, artist_id, year, duration)
        SELECT distinct song_id, title, artist_id, year, duration
        FROM stage_songs
    """)

    artist_table_insert = ("""
        INSERT INTO dim_artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM stage_songs
    """)

    time_table_insert = ("""
        INSERT INTO dim_time (start_time, hour, day, week, month, year, dayofweek)
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM fct_songplays
    """)

CREATE_STAGE_EVENTS_TABLE_SQL = """
    DROP TABLE IF EXISTS stage_events;
    CREATE TABLE stage_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    )
"""

CREATE_STAGE_SONGS_TABLE_SQL = """
    DROP TABLE IF EXISTS stage_songs;
    CREATE TABLE stage_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_location VARCHAR,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration NUMERIC,
        year SMALLINT
    )
"""

SONGPLAY_TABLE_CREATE = """
    DROP TABLE IF EXISTS fct_songplays;
    CREATE TABLE fct_songplays (
        songplay_id VARCHAR PRIMARY KEY,
        start_time TIMESTAMP,
        userid INTEGER,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        sessionid INTEGER,
        location VARCHAR,
        useragent VARCHAR
    )
"""

DIM_USERS_TABLE_CREATE = """
    DROP TABLE IF EXISTS dim_users;
    CREATE TABLE dim_users (
        user_id INTEGER PRIMARY KEY,
        firstname VARCHAR,
        lastname VARCHAR,
        gender VARCHAR,
        level VARCHAR
    )
"""

DIM_SONGS_TABLE_CREATE = """
    DROP TABLE IF EXISTS dim_songs;
    CREATE TABLE dim_songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INTEGER,
        duration NUMERIC
    )
"""

DIM_ARTISTS_TABLE_CREATE = """
    DROP TABLE IF EXISTS dim_artists;
    CREATE TABLE dim_artists (
        artist_id VARCHAR PRIMARY KEY,
        artist_name VARCHAR,
        artist_location VARCHAR,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR
    )
"""

DIM_TIME_TABLE_CREATE = """
    DROP TABLE IF EXISTS dim_time; 
    CREATE TABLE dim_time (
        start_time TIMESTAMP PRIMARY KEY,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        dayofweek SMALLINT
    )
"""