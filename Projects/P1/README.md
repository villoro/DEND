# Sparkify database and ETL

## 1. Database schema

The database follows a star design. There one **fact table** called `songplays` and 4 **dimensions tables**:

* users
* songs
* artists
* time

This allows maximum integrity with the data.

The definition of the tables is as it follows:

```yaml
songplays:
    id: TEXT PRIMARY KEY NOT NULL
    start_time: BIGINT
    user_id: INT
    level: TEXT
    song_id: TEXT
    artist_id: TEXT
    session_id: INT
    location: TEXT
    user_agent: TEXT
users: 
    id: INT PRIMARY KEY NOT NULL
    first_name: TEXT
    last_name: TEXT
    gender: TEXT
    level: TEXT
songs: 
    id: TEXT PRIMARY KEY NOT NULL
    title: TEXT
    artist_id: TEXT
    year: INT
    duration: REAL
artists: 
    id: TEXT PRIMARY KEY NOT NULL
    name: TEXT
    location: TEXT
    latitude: REAL
    longitude: REAL
time: 
    start_time: BIGINT 
    hour: INT 
    day: INT 
    week: INT
    month: INT
    year: INT 
    weekday: INT
```

## 2. Tables creation

To create all the tables run:

```python
python create_tables.py
```

> **WARNING:** this will delete all existing data!

## 3. ETL 

The **ETL** process can be run with:

```python
python etl.py
```

This will process all files from `song_data` and `log_data`.

### 3.1. song_data

These files contains the data for the tables `songs` and `artists`.

> Since there are duplicates for `artists` table there is the sentence `ON CONFLICT (id) DO NOTHING` to avoid problems in the ETL.

### 3.2. log_data

These files contains the data for the tables `time`, `users` and `song_plays`.

> Since there are duplicates for `users` table there is the sentence `ON CONFLICT (id) DO NOTHING` to avoid problems in the ETL.

For the table `song_plays` the ids for `users` and `artists` will be retrive from the database. And for the id it is used the filename concatenated with the row number.

## 4. Files

There are the following files in this repo:

* python scripts
    * create_tables.py: code for tables creation
    * etl.py: code for the ETL
* jupyter notebooks
    * test.ipynb: notebook for checking the content of the database
    * etl.ipynb: temporal and auxiliar notebook for creating the ETL.
