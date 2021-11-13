""" 
Sparkify ETL process to load JSON files into Postgres database

Note: run create_tables.py before runnign this script
"""

# import libraties
import os
import glob
import psycopg2
import pandas as pd

# import SQL queries from sql_queries.py
from sql_queries import *


def process_song_file(cur, filepath):
    """Process song files and load data into DB

    Args:
        cur (psycopg2.extensions.cursor): Postgres DB cursor
        filepath (str): Filepath to JSON song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = (
        df[
            [
                "song_id",
                "title",
                "artist_id",
                "year",
                "duration",
            ]
        ]
        .values[0]
        .tolist()
    )
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print(e)

    # insert artist record
    artist_data = (
        df[
            [
                "artist_id",
                "artist_name",
                "artist_location",
                "artist_latitude",
                "artist_longitude",
            ]
        ]
        .values[0]
        .tolist()
    )
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print(e)


def process_log_file(cur, filepath):
    """Process log files and load data into DB

    Args:
        cur (psycopg2.extensions.cursor): Postgres DB cursor
        filepath (str): Filepath to JSON log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == "NextSong"]

    # convert timestamp column to datetime
    df["timestamp"] = pd.to_datetime(df.ts, unit="ms")
    t = pd.to_datetime(df.ts, unit="ms")

    # insert time data records
    time_data = (
        t,
        t.dt.hour,
        t.dt.day,
        t.dt.isocalendar().week,
        t.dt.month,
        t.dt.year,
        t.dt.weekday,
    )
    column_labels = (
        "timestamp",
        "hour",
        "day",
        "week_of_year",
        "month",
        "year",
        "weekday",
    )
    time_dict = {column_labels[i]: time_data[i] for i in range(len(column_labels))}
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[
        [
            "userId",
            "firstName",
            "lastName",
            "gender",
            "level",
        ]
    ]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print(e)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            row.timestamp,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Load JSON song and log files from local folders

    Args:
        cur (psycopg2.extensions.cursor): Postgres DB cursor
        conn (psycopg2.extensions.connection): Postgres DB connection
        filepath (str): Filepath to JSON file
        func (function): Process data functions
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    """
    Sparkify ETL to load data

    1. Load song data from JSON files
    2. Load log data from JSON files
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
