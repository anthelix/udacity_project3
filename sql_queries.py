import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stating_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_song
""")
## Fact Tables
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay
                         (
                         songplay_id int IDENTITY(0,1),
                         start_time bigint NOT NULL,
                         user_id bigint NOT NULL,
                         level varchar NOT NULL,
                         song_id varchar,
                         artist_id varchar,
                         session_id bigint NOT NULL,
                         location varchar,
                         user_agent varchar
                         )
""")
## Dimension Tables
user_table_create = ("""CREATE TABLE IF NOT EXISTS user
                     (
                     user_id bigint NOT NULL,
                     first_name varchar,
                     last_name varchar,
                     gender varchar(1),
                     level varchar NOT NULL
                     )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song
                     (
                     song_id varchar NOT NULL,
                     title varchar NOT NULL,
                     artist_id varchar NOT NULL,
                     year int,
                     duration numeric
                     )
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist
                       (
                       artist_id varchar NOT NULL,
                       name varchar NOT NULL,
                       location varchar,
                       latitude numeric,
                       longitude numeric
                       )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                     (
                     start_time timestamp NOT NULL,
                     hour int,
                     day int,
                     week int,
                     month int,
                     year int,
                     weekday varchar
                     )
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
