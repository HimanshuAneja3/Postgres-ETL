from extraction import getLogFiles, getSongFiles
from transformation import tranformaLogData, tranformaSongData
from tableSchema import song_select, test
import psycopg2


# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays (songplayid, startTime, userID, level, songID, artistID, sessionID, location, user_agent)
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
        cur.execute(artist_table_insert, item)
        conn.commit()


def insertUserTimeRecords(udf, tdf, cur, conn):
    for i, row in udf.iterrows():

        cur.execute(user_table_insert, row)
        conn.commit()
    for i, row1 in tdf.iterrows():
        cur.execute(time_table_insert, list(row1))
        conn.commit()


def insertSongPlayRecords(log_df, cur, conn):
    for i, row in log_df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        songplay_data = (
            i,
            row["ts"],
            row["userId"],
            row["level"],
            songid,
            artistid,
            row["sessionId"],
            row["location"],
            row["userAgent"],
        )

        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()


def main():
    conn = psycopg2.connect(
        database="postgresetl",
        user=POSTGRESDB_USERNAME,
        host="localhost",
        password=POSTGRESDB_PASSWORD,
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
    user_data, time_data, log_df = tranformaLogData(logFiles)
    insertUserTimeRecords(user_data, time_data, cur, conn)
    insertSongPlayRecords(log_df, cur, conn)
    cur.close()


if __name__ == "__main__":
    main()
