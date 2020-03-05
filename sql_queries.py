import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stating_songs"
songplay_table_drop = "DROP TABLE IF EXISTS factSongplay"
user_table_drop = "DROP TABLE IF EXISTS Dimuser"
song_table_drop = "DROP TABLE IF EXISTS dimSong"
artist_table_drop = "DROP TABLE IF EXISTS dimArtist"
time_table_drop = "DROP TABLE IF EXISTS dimTime"

# CREATE TABLES

# ['artist"', 'auth"', 'firstName"', 'gender"', 'itemInSession"', 'lastName"', 
# 'length"', 'level"', 'location"', 'TX', 'method"', 'page"', 'registration"', 
# 'sessionId"', 'song"', 'status"', 'ts"', 'userAgent"', 'userId"']

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
(
  artist            varchar(256),
  auth              varchar(45),
  firstName         varchar(50),
  gender            varchar(1),
  itemInSession     smallint ,
  lastName          varchar(50),
  length            float,
  level             varchar(10),
  location          varchar(256),
  method            varchar(10),
  page              varchar(50),
  registration      float,
  sessionId         varchar(256), 
  song              varchar(256), 
  status            smallint, 
  ts                bigint,
  userAgent         varchar(1024),
  userId            int
)
""")
# ['song_id"', '"num_songs"', '"title"', '"artist_name"', '"artist_latitude"', 
# '"year"', '"duration"', '"artist_id"', '"artist_longitude"', '"artist_location"']

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
(
  num_songs         bigint, 
  artist_id         varchar(20), 
  artist_latitude   float, 
  artist_longitude  float, 
  artist_location   varchar(256),  
  artist_name       varchar(256), 
  song_id           varchar(20),
  title             varchar(256),  
  duration          float, 
  year              int
)
""")


## Dimension Tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS dimUser
(
    user_id bigint NOT NULL PRIMARY KEY SORTKEY,
    first_name varchar,
    last_name varchar,
    gender varchar(1),
    level varchar NOT NULL
    )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS dimSong
(
    song_id varchar NOT NULL PRIMARY KEY SORTKEY,
    title varchar NOT NULL,
    artist_id varchar NOT NULL,
    year int,
    duration numeric
    )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS dimArtist
(
    artist_id varchar NOT NULL PRIMARY KEY SORTKEY,
    name varchar NOT NULL,
    location varchar,
    latitude numeric,
    longitude numeric
    )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTime
(
    start_time timestamp NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday varchar
    )
""")


## Fact Tables
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS factSongplay
(
    songplay_id int IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp NOT NULL DISTKEY,
    user_id bigint NOT NULL ,
    level varchar NOT NULL,
    song_id varchar,
    artist_id varchar SORTKEY,
    session_id varchar NOT NULL,
    location varchar,
    user_agent varchar
    )
""")

# COPY data STAGING TABLES
staging_events_copy = (""" copy staging_events 
    from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = (""" copy staging_songs 
    from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON 'auto' truncatecolumns;
""").format(config.get('IAM_ROLE', 'ARN'))


# INSERT data with SELECT in FINAL TABLES
songplay_table_insert = ("""INSERT INTO factSongplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' AS start_time,
        se.userId AS user_id,
        se.level AS level,
        ss.song_id AS song_id,
        ss.artist_id AS artist_id,
        se.sessionId AS session_id,
        se.location AS location,
        se.userAgent AS user_agent
    FROM staging_events AS se, staging_songs AS ss
    WHERE se.song = ss.title
    AND se.artist = ss.artist_name;                 
""")

user_table_insert = ("""INSERT INTO dimUser(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id,
                    firstName AS first_name,
                    lastName AS last_name,
                    gender,
                    level
    FROM staging_events
    WHERE user_id IS NOT NULL;
                    
""")

song_table_insert = ("""INSERT INTO dimSong(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
                    title, 
                    artist_id,
                    year,
                    duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO dimArtist(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id,
                    artist_name AS name, 
                    artist_location AS location,
                    artist_latitude AS latitude, 
                    artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO dimTime(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        TIMESTAMP 'epoch' + CAST(se.ts AS BIGINT)/1000 * interval '1 second' AS start_time,
        EXTRACT (hour FROM start_time),
        EXTRACT (day FROM start_time),
        EXTRACT (week FROM start_time),
        EXTRACT (month FROM start_time),
        EXTRACT (year FROM start_time),
        EXTRACT (weekday FROM start_time)
    FROM staging_events AS se
    WHERE ts is not NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]