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
   CREATE TABLE staging_events_table(
       ts              BIGINT          not null,
       artist          varchar(50)     not null,
       firstname       varchar(20)     not null,
       lastname        varchar(20)     not null,
       gender          varchar(6)      not null,
       length          float           not null,
       level           varchar(20)     not null,
       location        TEXT            not null,
       sessionid       integer         not null,
       song            varchar(50)     not null,  
       useragent       TEXT            not null,
       userid          varchar(50)     not null,
       page            varchar(50)     not null
   ); 
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs_table(
        songid          varchar(20)    PRIMARY KEY,
        title           TEXT           not null,
        artistname      varchar(50)    not null,
        artistlatitude  float          not null,
        artistlongitude float          not null,
        year            integer        not null,
        duration        float          not null,
        artistid        varchar(20)    not null,
        artistlocation  TEXT           not null
    );
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id IDENTITY(0,1) PRIMARY KEY,
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
    artistname      VARCHAR(50) NOT NULL,
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
    se.userid::INT,
    se.level,
    ss.songid,
    ss.artistid,
    se.sessionid,
    se.location,
    se.useragent
FROM staging_events_table se
LEFT JOIN staging_songs_table ss
       ON se.song = ss.title
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userid,
    firstname,
    lastname,
    gender,
    level
FROM staging_events_table
WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    songid,
    title,
    artistid,
    year,
    duration
FROM staging_songs_table
WHERE songid IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, artistname, location, latitude, longitude)
SELECT DISTINCT
    artistid,
    artistname,
    artistlocation,
    artistlatitude,
    artistlongitude
FROM staging_songs_table
WHERE artistid IS NOT NULL;

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
