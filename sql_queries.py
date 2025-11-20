import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA     = config.get("S3", "LOG_DATA")
SONG_DATA    = config.get("S3", "SONG_DATA")
IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
#CREATE SCHEMA IF NOT EXISTS dist;

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
 CREATE TABLE staging_events_table (
    artist_name          TEXT,
    auth            VARCHAR(20),
    firs_tname       VARCHAR(20),
    gender          VARCHAR(8),
    item_in_session   INT,
    last_name        VARCHAR(20),
    length          FLOAT,
    level           VARCHAR(10),
    location        TEXT,
    method          VARCHAR(10),
    page            VARCHAR(50),
    registration    BIGINT,
    session_id       INT,
    song            SONG,
    status          INT,
    ts              BIGINT,
    usera_gent       TEXT,
    user_id          VARCHAR(20)
);
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs_table(
        song_id          varchar(20),
        num_song        integer
        title           TEXT,
        artistname      varchar(50),
        latitude  float,
        longitude float,
        year            integer,
        duration        float,
        artist_id        varchar(20),
        location  TEXT
    );
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     INT NOT NULL,
    level       VARCHAR(10) NOT NULL,
    song_id     VARCHAR(20) NOT NULL,
    artist_id   VARCHAR(20) NOT NULL,
    session_id  integer NOT NULL,
    location    TEXT NOT NULL,
    user_agent  TEXT NOT NULL
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id     varchar(50) PRIMARY KEY NOT NULL,
    first_name  VARCHAR(20) NOT NULL,
    last_name   VARCHAR(20) NOT NULL,
    gender      CHAR(6) NOT NULL,
    level       VARCHAR(20) NOT NULL
);

""")

song_table_create = ("""
CREATE TABLE songs (
    song_id    VARCHAR(20) PRIMARY KEY NOT NULL,
    title      TEXT NOT NULL,
    artist_id  VARCHAR(20) NOT NULL,
    year       INT NOT NULL,
    duration   float NOT NULL
);


""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR(20) PRIMARY KEY NOT NULL,
    artist_name      VARCHAR(50) NOT NULL,
    location  TEXT NOT NULL,
    latitude  float NOT NULL,
    longitude float NOT NULL
);


""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY NOT NULL,
    hour       INT NOT NULL,
    day        INT NOT NULL,
    week       INT NOT NULL,
    month      INT NOT NULL,
    year       INT NOT NULL,
    weekday    INT NOT NULL
);

""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events_table
FROM {}
IAM_ROLE {}
FORMAT AS JSON 'auto'
REGION 'us-west-2'
TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA,IAM_ROLE_ARN)

staging_songs_copy = ("""
COPY staging_songs_table
FROM {}
IAM_ROLE {}
FORMAT AS JSON 'auto'
REGION 'us-west-2';
""").format(SONG_DATA,IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
)
SELECT 
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    se.user_id::INT,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM staging_events_table se
LEFT JOIN staging_songs_table ss
       ON se.song = ss.title
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events_table
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs_table
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, artist_name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    location,
    latitude,
    longitude
FROM staging_songs_table
WHERE artist_id IS NOT NULL;

""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
    t.start_time,
    EXTRACT(hour FROM t.start_time),
    EXTRACT(day FROM t.start_time),
    EXTRACT(week FROM t.start_time),
    EXTRACT(month FROM t.start_time),
    EXTRACT(year FROM t.start_time),
    EXTRACT(weekday FROM t.start_time)
FROM (
    SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
    FROM staging_events_table
    WHERE ts IS NOT NULL
);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
