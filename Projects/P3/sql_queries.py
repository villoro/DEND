"""
    SQL queries that are created using the tables dict.
    
    tables is a dict with all tables explaining the column that it has and the creation instructions
    
    Then there is:
        create_table_queries: dict with queries for creating all tables
        drop_table_queries:   dict with queries for creating all tables
        copy_table_queries:   dict with queries for inserting data into stagging tables
        insert_table_queries: dict with queries for inserting data from stagging to final tables
"""

import configparser

# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# TABLES DEFINITION
tables = {
    "staging_songs": {
        "id": "INT IDENTITY(0,1)",
        "artist_id": "TEXT",
        "artist_latitude": "FLOAT",
        "artist_location": "TEXT",
        "artist_longitude": "FLOAT",
        "artist_name": "TEXT",
        "duration": "FLOAT",
        "num_songs": "INT",
        "song_id": "TEXT",
        "title": "TEXT",
        "year": "INT",
    },
    "staging_events": {
        "id": "INT IDENTITY(0,1)",
        "artist": "TEXT",
        "auth": "TEXT",
        "firstName": "TEXT",
        "gender": "TEXT",
        "itemInSession": "INT",
        "lastName": "TEXT",
        "length": "FLOAT",
        "level": "TEXT",
        "location": "TEXT",
        "method": "TEXT",
        "page": "TEXT",
        "registration": "BIGINT",
        "sessionId": "INT",
        "song": "TEXT",
        "status": "INT",
        "ts": "BIGINT",
        "userAgent": "TEXT",
        "userId": "INT",
    },
    "songplays": {
        "id": "INT PRIMARY KEY NOT NULL",
        "start_time": "BIGINT",
        "user_id": "INT",
        "level": "TEXT",
        "song_id": "TEXT",
        "artist_id": "TEXT",
        "session_id": "INT",
        "location": "TEXT",
        "user_agent": "TEXT",
    },
    "users": {
        "id": "INT PRIMARY KEY NOT NULL",
        "first_name": "TEXT",
        "last_name": "TEXT",
        "gender": "TEXT",
        "level": "TEXT",
    },
    "songs": {
        "id": "TEXT PRIMARY KEY NOT NULL",
        "title": "TEXT",
        "artist_id": "TEXT",
        "year": "INT",
        "duration": "FLOAT",
    },
    "artists": {
        "id": "TEXT PRIMARY KEY NOT NULL",
        "name": "TEXT",
        "location": "TEXT",
        "latitude": "FLOAT",
        "longitude": "FLOAT",
    },
    "time": {
        "start_time": "BIGINT",
        "hour": "INT",
        "day": "INT",
        "week": "INT",
        "month": "INT",
        "year": "INT",
        "weekday": "INT",
    },
}


# CREATION AND DELETION
create_table_queries = {
    tablename: "CREATE TABLE IF NOT EXISTS {name} ({data});".format(
        name=tablename, data=", ".join([f"{col} {text}" for col, text in table.items()])
    )
    for tablename, table in tables.items()
}
drop_table_queries = {name: f"DROP TABLE IF EXISTS {name}" for name in tables.keys()}


# STAGING TABLES
raw_copy = """COPY {table} FROM {s3}
iam_role {arn}
region 'us-west-2'
json {json_fmt};"""

staging_events_copy = raw_copy.format(
    table="staging_events",
    s3=config.get("S3", "LOG_DATA"),
    arn=config.get("IAM_ROLE", "ARN"),
    json_fmt=config.get("S3", "LOG_JSONPATH"),
)

staging_songs_copy = raw_copy.format(
    table="staging_songs",
    s3=config.get("S3", "SONG_DATA"),
    arn=config.get("IAM_ROLE", "ARN"),
    json_fmt="'auto'",
)

copy_table_queries = {"staging_events": staging_events_copy, "staging_songs": staging_songs_copy}


# FINAL TABLES
song_table_insert = """INSERT INTO songs
(
    SELECT song_id AS id, title, artist_id, year, duration
    FROM staging_songs
);"""

artist_table_insert = """INSERT INTO artists
(
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
);"""

time_table_insert = """INSERT INTO time
(
    SELECT
        ts AS start_time,
        EXTRACT(hour FROM dt) AS hour,
        EXTRACT(day FROM dt) AS day,
        EXTRACT(week FROM dt) AS week,
        EXTRACT(month FROM dt) AS month,
        EXTRACT(year FROM dt) AS year,
        EXTRACT(weekday FROM dt) AS weekday
    FROM (
        SELECT DISTINCT
            ts,
            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS dt -- fast cast to timestamp
        FROM staging_events
    )
);"""

user_table_insert = """INSERT INTO users
(
    SELECT DISTINCT
        userId AS id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM staging_events
    WHERE userId IS NOT NULL
);"""

songplay_table_insert = """INSERT INTO songplays
(
    SELECT
        e.id,
        e.ts AS start_time,
        e.userId AS user_id,
        e.level,
        s.id AS song_id,
        a.id AS artist_id,
        e.sessionId AS session_id,
        e.location,
        e.userAgent AS user_agent
    FROM staging_events e
    LEFT JOIN songs s    ON s.title = e.song
    LEFT JOIN artists a  ON a.name = e.artist
);"""

insert_table_queries = {
    "songs": song_table_insert,
    "artists": artist_table_insert,
    "time": time_table_insert,
    "users": user_table_insert,
    "songplays": songplay_table_insert,
}
