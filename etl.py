import os
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Gets song files and process it.
    This function reads all the .json files in filepath, then it makes an INSERT into songs table taking song_id, title, artist_id, year, duration columns.
    It does the same for data of the artist, inserting artist_id, artist_name, artist_location, artist_latitude, artist_longitude into artists table.
    
    Parameters:
    cur (Object): object used to make the connection to the DB for executing SQL queries.
    filepath (str): It is the filepath that contains song files.    
    
    Returns:
    No parameters returned.
    '''
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    df['year'] = df['year'].astype(float)
    df['duration'] = df['duration'].astype(float)
    # duplicated song_id check
    if df.duplicated(subset=['song_id'],keep=False).sum() == 0:
        # insert song record
        song_data = df[['song_id','title','artist_id','year','duration']].iloc[0].values.tolist()
        cur.execute(song_table_insert, song_data)
    else:
        print('Duplicated song_id found in song_data')
    

    # duplicated artist_id check
    if df.duplicated(subset=['artist_id'],keep=False).sum() == 0:
        # insert artist record
        artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].iloc[0].values.tolist() 
        cur.execute(artist_table_insert, artist_data)
    else:
        print('Duplicated artist_id found in artist_data')

def process_log_file(cur, filepath):
    '''
    Gets log files and process it.
    This function reads all the .json files in filepath, loading them in a pandas dataframe and filtering the dataframe by NextSong page.
    It makes an INSERT into time table taking timestamp, hour, day, weekofyear, month, year, weekday columns.
    It makes an INSERT into users table taking userId, firstName, gender, lastName, level columns.
    Lastly it makes an INSERT into songplays fact table taking ts, user_id, level,session_id, location, user_agent, song_id, artist_id columns.
    
    Parameters:
    cur (Object): object used to make the connection to the DB for executing SQL queries.
    filepath (str): It is the filepath that contains log files.    
    
    Returns:
    No parameters returned.
    
    '''    
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[(df['page'] == 'NextSong')]

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = pd.Series(df['ts'])
    
    # insert time data records in time_df
    time_data = (t.dt.time,t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ('timestamp','hour','day','weekofyear','month','year','weekday')
    time_df = pd.DataFrame({column_labels[0]:time_data[0],
                           column_labels[1]:time_data[1],
                           column_labels[2]:time_data[2],
                           column_labels[3]:time_data[3],
                           column_labels[4]:time_data[4],
                           column_labels[5]:time_data[5],
                           column_labels[6]:time_data[6]}) 

    # Iters on time_df rows to execute an INSERT into time table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    df['userId'] = pd.to_numeric(df['userId'])
    
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
        songplay_data = (row.ts,row.userId,row.level,row.sessionId,row.location,row.userAgent,songid, artistid) #row.songplay_id,
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Gets the data and process it.
    
    This function is meant to get all files matching .json extension from the directory which the path is specified in filepath.
    Then it iterates over all files printing the file number that is being processed out of the total number of files found in directory.
    
    Parameters:
    cur (Object): object used to make the connection to the DB for executing SQL queries.
    conn (Object): Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
    filepath (str): It is the filepath.
    func (Object): It is the function which processes the file, it can assume process_song_file or process_log_file.
    
    Returns:
    No parameters returned.
    
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed. File name: {}. File size: {}'.format(i, num_files, datafile.split('/')[-1],'Empty File' if os.path.getsize(datafile) < 2 else os.path.getsize(datafile)))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()