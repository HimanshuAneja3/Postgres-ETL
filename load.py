from extraction import getLogFiles, getSongFiles
from transformation import tranformaLogData, tranformaSongData
import psycopg2


# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

user_table_insert = """
INSERT INTO users (userID, firstname, lastname, gender, level)
VALUES (%s, %s, %s, %s, %s);
"""

song_table_insert = """
INSERT INTO songs (songID, title, artistID, year, duration)
VALUES (%s, %s, %s, %s, %s);
"""


artist_table_insert = """
INSERT INTO artists (artistID, name, location, latitude, longitude)
VALUES (%s, %s, %s, %s, %s);
"""


time_table_insert = """
INSERT INTO time (starttime, hour, day, week, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""


def insertSongArtistRecords(song_data, artist_data, cur, conn):
    for item in song_data:
        cur.execute(song_table_insert, item)
        conn.commit()
    for item in artist_data:
        cur.execute(song_table_insert, item)
        conn.commit()


def insertUserTimeRecords(udf, tdf, cur, conn):
    for i, row in udf.iterrows():
        print(row)
        cur.execute(user_table_insert, row)
        conn.commit()
    for i, row1 in tdf.iterrows():
        cur.execute(time_table_insert, list(row1))
        conn.commit()


def main():
    conn = psycopg2.connect(
        database="postgresetl",
        user="postgres",
        host="localhost",
        password="postgres",
        port=5432,
    )
    cur = conn.cursor()
    song_file_path = "/Users/sahil/Desktop/Data Engineering/Project/Postgres-ETL/resources/song_data/"
    songfile = getSongFiles(song_file_path)
    song_data, artist_data = tranformaSongData(songfile)
    insertSongArtistRecords(song_data, artist_data, cur, conn)
    log_file_path = (
        "/Users/sahil/Desktop/Data Engineering/Project/Postgres-ETL/resources/log_data/"
    )
    logFiles = getLogFiles(log_file_path)
    udf, tdf = tranformaLogData(logFiles)
    log_file_path = (
        "/Users/sahil/Desktop/Data Engineering/Project/Postgres-ETL/resources/log_data/"
    )
    logFiles = getLogFiles(log_file_path)
    user_data, time_data = tranformaLogData(logFiles)
    insertUserTimeRecords(user_data, time_data, cur, conn)
    cur.close()


if __name__ == "__main__":
    main()
