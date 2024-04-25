import os
import glob


# walk it generates the file names in a directory tree by walking the tree top down or bottom-up
def getLogFiles(log_file_path):
    all_log_files = []
    for root, dir, files1 in os.walk(log_file_path):
        files = glob.glob(os.path.join(root, "*.json"))
        for file in files:
            all_log_files.append(file)
    return all_log_files


# print(getLogFiles(file_path))
# lines Read the file as a json object per line.
# log_df = pd.read_json(file_path, lines=True)


def getSongFiles(song_file_path):
    all_songs_files = []
    for root, dir, files1 in os.walk(song_file_path):
        files = glob.glob(os.path.join(root, "*.json"))
        for file in files:
            all_songs_files.append(file)
    return all_songs_files


# log_file_path = (
#     "/Users/sahil/Desktop/Data Engineering/Project/Postgres-ETL/resources/log_data/"
# )
# song_file_path = (
#     "/Users/sahil/Desktop/Data Engineering/Project/Postgres-ETL/resources/song_data/"
# )
# print(len(getSongFiles(song_file_path)))
