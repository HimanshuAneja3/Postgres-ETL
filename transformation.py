import pandas as pd

from extraction import getLogFiles


def tranformaLogData(logFiles):
    for file in logFiles:
        log_df = pd.read_json(file, lines=True)
    # user df
    user_df = log_df[["userId", "firstName", "lastName", "gender", "level"]]
    # convert timestamp column to datetime
    user_df.drop_duplicates(inplace=True)
    user_df.dropna(inplace=True)
    ts = pd.to_datetime(log_df["ts"])
    time_data = [
        (
            item.value,
            item.hour,
            item.day,
            item.week,
            item.month,
            item.year,
            item.weekday(),
        )
        for item in ts
    ]
    column_labels = ("timestamp", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(data=time_data, columns=column_labels)
    time_df.drop_duplicates(inplace=True)
    time_df.dropna(inplace=True)
    return user_df, time_df


log_file_path = (
    "/Users/sahil/Desktop/Data Engineering/Project/Postgres-ETL/resources/log_data/"
)
logFiles = getLogFiles(log_file_path)
print(logFiles)
user_data, time_data = tranformaLogData(logFiles)
print(user_data)


def tranformaSongData(songfile):
    songs_data = []
    artist_data = []
    for file in songfile:
        data_df = pd.read_json(file, lines=True)
        columns_rename = {
            "num_songs": "numsongs",
            "artist_id": "artistID",
            "artist_latitude": "latitude",
            "artist_longitude": "longitude",
            "artist_location": "location",
            "artist_name": "name",
            "song_id": "songID",
            "title": "title",
            "duration": "duration",
            "year": "year",
        }
        data_df.rename(columns=columns_rename, inplace=True)
        song_df = list(
            data_df[["songID", "title", "artistID", "year", "duration"]].values[0]
        )
        songs_data.append(song_df)
        artist_df = list(
            data_df[
                [
                    "artistID",
                    "name",
                    "location",
                    "latitude",
                    "longitude",
                ]
            ].values[0]
        )
        artist_data.append(artist_df)
    return songs_data, artist_data
