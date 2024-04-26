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


def getSongFiles(song_file_path):
    all_songs_files = []
    for root, dir, files1 in os.walk(song_file_path):
        files = glob.glob(os.path.join(root, "*.json"))
        for file in files:
            all_songs_files.append(file)
    return all_songs_files
