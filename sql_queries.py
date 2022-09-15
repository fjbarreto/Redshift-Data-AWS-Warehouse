import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events(
                                    artist varchar(MAX),
                                    auth varchar(MAX),
                                    first_name varchar(MAX),
                                    gender varchar(MAX),
                                    item_in_session int,
                                    last_name varchar(MAX),
                                    length numeric, 
                                    level varchar(MAX),
                                    location varchar(MAX),
                                    method varchar(MAX),
                                    page varchar(MAX),
                                    registration varchar(MAX),
                                    session_id int,
                                    song varchar(MAX),
                                    status varchar(MAX),
                                    start_time numeric,
                                    user_agent varchar(MAX),
                                    user_id int 
                                    );
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs(
                                    num_songs int,
                                    artist_id varchar(MAX), 
                                    artist_latitude double precision,
                                    artist_longitude double precision,
                                    artist_location varchar(MAX),
                                    artist_name varchar(MAX),
                                    song_id varchar(MAX),
                                    title varchar(MAX),
                                    duration numeric,
                                    year int
                                    );
""")

songplay_table_create = ("""CREATE TABLE song_plays (
                            songplay_id int IDENTITY(0,1) PRIMARY KEY, 
                            start_time timestamp NOT NULL sortkey,
                            user_id int NOT NULL,
                            level varchar,
                            song_id varchar distkey,
                            artist_id varchar,
                            session_id int,
                            location varchar,
                            user_agent varchar
                            );
""")


user_table_create = ("""CREATE TABLE users (
                        user_id int NOT NULL PRIMARY KEY sortkey,
                        first_name varchar,
                        last_name varchar,
                        gender varchar,
                        level varchar
                        );
""")

song_table_create = ("""CREATE TABLE songs (
                        song_id varchar PRIMARY KEY distkey sortkey,
                        title varchar NOT NULL,
                        artist_id varchar,
                        year int,
                        duration numeric NOT NULL
                        );
""")

artist_table_create = ("""CREATE TABLE artists (
                          artist_id varchar NOT NULL PRIMARY KEY sortkey,
                          name varchar NOT NULL,
                          location varchar,
                          latitude double precision,
                          longitude double precision
                          );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time timestamp NOT NULL PRIMARY KEY distkey sortkey,
                        hour int,
                        day int,
                        week int,
                        month int,
                        year int, 
                        weekday int
                        ); 
""")

# STAGING TABLES

staging_events_copy = ("""  COPY staging_events 
                            FROM {log_data}
                            iam_role {IAM}
                            COMPUPDATE OFF region 'us-west-2'
                            TIMEFORMAT as 'epochmillisecs'
                            STATUPDATE ON
                            FORMAT AS JSON {json_paths};
""").format(log_data = config['S3']['LOG_DATA'], IAM = config['IAM_ROLE']['ARN'], json_paths = config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""   COPY staging_songs 
                            FROM {song_data}
                            iam_role {IAM}
                            COMPUPDATE OFF region 'us-west-2'
                            TIMEFORMAT as 'epochmillisecs'
                            STATUPDATE ON
                            json 'auto';
""").format(song_data = config['S3']['SONG_DATA'], IAM = config['IAM_ROLE']['ARN'])


#FINAL TABLES

songplay_table_insert = ("""INSERT INTO song_plays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT  TIMESTAMP WITHOUT TIME ZONE 'epoch' + (start_time / 1000) * INTERVAL '1 second',
                                    user_id, 
                                    level, 
                                    song_id, 
                                    artist_id, 
                                    session_id, 
                                    location, 
                                    user_agent
                            FROM staging_events 
                            JOIN staging_songs 
                            ON staging_events.artist = staging_songs.artist_name 
                            AND staging_events.song = staging_songs.title 
                            AND staging_events.length = staging_songs.duration
                            WHERE page = 'NextSong';
""")

user_table_insert = ("""   INSERT INTO users (user_id, first_name, last_name, gender, level)
                           SELECT DISTINCT user_id,
                                           first_name,
                                           last_name,
                                           gender,
                                           level
                           FROM staging_events
                           WHERE page = 'NextSong' AND
                           user_id NOT IN (SELECT DISTINCT user_id FROM users);
                           
""") 

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                        SELECT DISTINCT song_id, 
                                        title, 
                                        artist_id, 
                                        year, 
                                        duration
                        FROM staging_songs
                        WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs);
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
                          SELECT DISTINCT artist_id, 
                                          artist_name as name, 
                                          artist_location as location, 
                                          artist_latitude as latitude, 
                                          artist_longitude as longitude
                          FROM staging_songs 
                          WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists);
""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT a.start_time,
                                        EXTRACT(HOUR FROM a.start_time) as timestamp,
                                        EXTRACT(DAY FROM a.start_time) as day,
                                        EXTRACT(WEEK FROM a.start_time) as week,
                                        EXTRACT(MONTH FROM a.start_time) as month,
                                        EXTRACT(YEAR FROM a.start_time) as year,
                                        EXTRACT(DOW FROM a.start_time) as weekday
                        FROM (SELECT TIMESTAMP 'epoch' + start_time/1000 *INTERVAL '1 second' as start_time FROM staging_events) a;
""")  

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, time_table_insert, user_table_insert, song_table_insert, artist_table_insert]
                        

