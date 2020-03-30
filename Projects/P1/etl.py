import os
import glob
import psycopg2
import pandas as pd
# import * is bad: https://stackoverflow.com/questions/2386714/why-is-import-bad
from sql_queries import insert_table_queries, song_select

# Fix the problem where psycopg2 can't handle np.int64
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].iloc[0, :].tolist()
    cur.execute(insert_table_queries["songs"], song_data)
    
    # insert artist record
    cols = [
        "artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"
    ]
    artist_data = df[cols].iloc[0, :].tolist()
    cur.execute(insert_table_queries["artists"], artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')
    
    # insert time data records
    time_data = {
        "start_time": df["ts"],
        "hour": t.dt.hour,
        "day": t.dt.day,
        "week": t.dt.week,
        "month": t.dt.month,
        "year": t.dt.year,
        "weekday": t.dt.weekday,
    }
    time_df = pd.DataFrame(time_data)

    for row in time_df.itertuples(index=False):
        cur.execute(insert_table_queries["time"], list(row))

    # load user table
    cols = ["userId", "firstName", "lastName", "gender", "level"]
    user_df = df[cols].drop_duplicates()

    # insert user records
    for row in user_df.itertuples(index=False):
        cur.execute(insert_table_queries["users"], list(row))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = (
            [f"{filepath}/{index}"] + # unique id
            row[["ts", "userId", "level"]].tolist() +
            [song_id, artist_id] + 
            row[["sessionId", "location", "userAgent"]].tolist()
        )
        cur.execute(insert_table_queries["songplays"], songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            
            # Avoid problems with jupyter checkpoints
            if ".ipynb_checkpoints" not in f:
                all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed.')


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()