# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
songplay_table_create = """
CREATE TABLE songplays
(songplayid int PRIMARY KEY, 
 startTime bigint REFERENCES time(startTime) ON DELETE RESTRICT, 
 userID Int REFERENCES users(userID) ON DELETE RESTRICT, 
 level Varchar(200), 
 songID Varchar(200) REFERENCES songs(songID) ON DELETE RESTRICT, 
 artistID Varchar(200), 
 sessionID Int, 
 location Varchar(200), 
 user_agent Varchar(200));
"""

user_table_create = """
CREATE TABLE users
(
userID Int PRIMARY KEY,
firstName Varchar(200),
lastName Varchar(200),
gender Varchar(3),
level Varchar(200));
"""

song_table_create = """
CREATE TABLE songs
(songID Varchar PRIMARY KEY, 
 title Varchar(300),
 artistID Varchar(20),
 year INT,
 duration FLOAT);
"""
artist_table_create = """
CREATE TABLE artists
(artistID Varchar(20), 
 name Varchar(200),
 location Varchar(200),
 latitude FLOAT,
 longitude FLOAT);
"""

time_table_create = """
CREATE TABLE time
(startTime bigint PRIMARY KEY, 
 hour Int,
 day Int,
 week Int,
 month Int,
 year Int,
 weekday Int);
"""

drop_table_list = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
create_table_list = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]

# FIND SONGS

song_select = """
SELECT songs.songID, artists.artistID FROM songs
JOIN artists ON songs.artistID=artists.artistID; 
"""
