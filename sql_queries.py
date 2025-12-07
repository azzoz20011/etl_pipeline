import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA     = config.get("S3", "LOG_DATA")
SONG_DATA    = config.get("S3", "SONG_DATA")
IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_JSONPATH = config.get("S3", "SONG_JSONPATH")

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
    artist         TEXT,
    auth            TEXT,
    firstName       varchar(20),
    gender          VARCHAR(8),
    itemInSession   BIGINT,
    lastName        varchar(20),
    length          double precision,
    level           VARCHAR(50),
    location        TEXT,
    method          VARCHAR(20),
    page            VARCHAR(50),
    registration    BIGINT,
    sessionId       integer,
    song            TEXT,
    status          integer,
    ts              BIGINT,
    userAgent       TEXT,
    userId          integer
);
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs_table(
        song_id          varchar(30),
        num_song        integer,
        title            TEXT,
        artist      TEXT,
        latitude  double precision,
        longitude double precision,
        year            integer,
        duration        double precision,
        artist_id        varchar(40),
        location  TEXT
    );
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL SORTKEY,
    userId     integer ,
    level       VARCHAR(10) ,
    song_id     VARCHAR(30)  DISTKEY,
    artist_id   VARCHAR(40) ,
    sessionId  integer ,
    location    TEXT ,
    userAgent  TEXT 
);
""")

user_table_create = ("""
CREATE TABLE users (
    userId     integer PRIMARY KEY ,
    firstName  varchar(20) ,
    lastName   varchar(20) ,
    gender      VARCHAR(6) ,
    level       VARCHAR(20) 
);

""")

song_table_create = ("""
CREATE TABLE songs (
    song_id    VARCHAR(30) PRIMARY KEY DISTKEY,
    title       TEXT ,
    artist_id  VARCHAR(50) ,
    year       integer,
    duration   double precision
);


""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR(40) PRIMARY KEY ,
    artist      TEXT ,
    location  TEXT,
    latitude  double precision ,
    longitude double precision 
);


""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
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
FORMAT AS JSON {}
REGION 'us-west-2'
TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA,IAM_ROLE_ARN,LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs_table
FROM {}
IAM_ROLE {}
FORMAT AS JSON {}
REGION 'us-west-2';
""").format(SONG_DATA,IAM_ROLE_ARN,SONG_JSONPATH)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (
    start_time, userId, level, song_id, artist_id, sessionId, location, userAgent
)
SELECT 
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events_table se
LEFT JOIN staging_songs_table ss
       ON se.song = ss.title
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(userId, firstName, lastName, gender, level)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events_table
WHERE userId IS NOT NULL;
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
INSERT INTO artists(artist_id, artist, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist,
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
)t;
""")
#Count the rows in each table
staging_events_count = ("SELECT COUNT(*) FROM staging_events_table;")

staging_songs_count = ("SELECT COUNT(*) FROM staging_songs_table;")

songplay_table_count = ("SELECT COUNT(*) FROM songplays;")

user_table_count = ("SELECT COUNT(*) FROM users;")

songs_table_count = ("SELECT COUNT(*) FROM songs;")

artist_table_count = ("SELECT COUNT(*) FROM artists;")

time_table_count = ("SELECT COUNT(*) FROM time;")

#some analysis i made

# how many listener in each hour?
most_listened_hour = ("""select t.hour, count(s.songplay_id)
from songplays s
JOIN time t 
ON s.start_time = t.start_time
group by 1
order by 2 DESC;""")

#Who are the most famous artists?
most_listened_artist = ("""SELECT a.artist, count(s.songplay_id)
FROM songplays s
JOIN artists a
ON s.artist_id = a.artist_id
GROUP BY 1
order by 2 DESC
limit 10;""")

#how many artists with no audiance?
artist_with_no_listeners = ("""SELECT COUNT(artist)
FROM (SELECT a.artist, count(s.songplay_id) as num_of_listeners
    FROM songplays s
    RIGHT JOIN artists a
    ON s.artist_id = a.artist_id
    GROUP BY 1)
where num_of_listeners=0""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
count_table_queries = [staging_events_count,staging_songs_count, songplay_table_count, user_table_count, songs_table_count, artist_table_count, time_table_count]
analyze_questions = ["in which hour users listened to more songs?",
                     "who are the most famous artists?",
                     "how many artiste with no listeners?"]
analysis_queries = [most_listened_hour,most_listened_artist,artist_with_no_listeners]
tables = ["staging_events_table","staging_songs_table","songplay_table","user_table","song_table","artist_table","time_table"]