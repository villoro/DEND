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

There are also 2 stagging tables:
```yaml
staging_songs:
    id: INT IDENTITY(01)
    artist_id: TEXT
    artist_latitude: FLOAT
    artist_location: TEXT
    artist_longitude: FLOAT
    artist_name: TEXT
    duration: FLOAT
    num_songs: INT
    song_id: TEXT
    title: TEXT
    year: INT
staging_events:
    id: INT IDENTITY(01)
    artist: TEXT
    auth: TEXT
    firstName: TEXT
    gender: TEXT
    itemInSession: INT
    lastName: TEXT
    length: FLOAT
    level: TEXT
    location: TEXT
    method: TEXT
    page: TEXT
    registration: BIGINT
    sessionId: INT
    song: TEXT
    status: INT
    ts: BIGINT
    userAgent: TEXT
    userId: INT
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

This will process all files from `song_data` and `log_data` extracted from **S3** buckets.

This will be done in 2 steps:

1. Load data into the stagging tables
2. Extract data from the stagging tables into the final tables

### 3.1. song_data

These files contains the data for the tables `songs` and `artists`.
Those tables will be extracted from the `staging_songs` table.

### 3.2. log_data

These files contains the data for the tables `time`, `users` and `song_plays`.
Those tables will be extracted from the `staging_events` table.

## 4. Files

There are the following files in this repo:

* create_tables.py: code for tables creation
* etl.py: code for the ETL
* dwh.cfg: config file for connecting to the Redshift cluster
