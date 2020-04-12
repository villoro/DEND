# Sparkify database and ETL

## 1. Database schema

The database follows a star design. There one **fact table** called `songplays` and 4 **dimensions tables**:

* users
* songs
* artists
* time

The definition of the tables is as it follows:

```
songs
 |-- id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- duration: double (nullable = true)
 |-- year: integer (nullable = true)
 |-- artist_id: string (nullable = true)

artists
 |-- artist_id: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_longitude: double (nullable = true)

users
 |-- userId: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)

time
 |-- start_time: long (nullable = true)
 |-- hour: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)

songplays
 |-- start_time: long (nullable = true)
 |-- user_id: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- session_id: long (nullable = true)
 |-- location: string (nullable = true)
 |-- user_agent: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```

## 2. ETL 

The **ETL** process can be run with:

```python
python etl.py
```

This will process all files from `song_data` and `log_data` extracted from **S3** buckets.

> **WARNING:** this will delete all existing data!

### 2.1. song_data

These files contains the data for the tables `songs` and `artists`.

### 2.2. log_data

These files contains the data for the tables `time`, `users` and `song_plays`.

## 3. Files

There are the following files in this repo:

* etl.py: code for the ETL
* dl.cfg: config file for connecting to AWS S3
