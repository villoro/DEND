"""
    SQL queries that are created using the tables dict.
    
    tables is a dict with all tables explaining the column that it has and the creation instructions
    
    Then there is:
        create_table_queries: list with queries for creating all tables
        drop_table_queries: list with queries for creating all tables
        drop_table_queries: dict with queries for inserting data where tablename is the key
"""

# DEFINITION OF THE TABLES
tables = {
    "songplays": {
        "id": "TEXT PRIMARY KEY NOT NULL",
        "start_time": "BIGINT",
        "user_id": "INT",
        "level": "TEXT",
        "song_id": "TEXT",
        "artist_id": "TEXT",
        "session_id": "INT",
        "location": "TEXT",
        "user_agent": "TEXT"
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
        "duration": "REAL",
    },
    "artists": {
        "id": "TEXT PRIMARY KEY NOT NULL",
        "name": "TEXT",
        "location": "TEXT",
        "latitude": "REAL",
        "longitude": "REAL",
    },
    "time": {
        "start_time": "BIGINT", 
        "hour": "INT", 
        "day": "INT", 
        "week": "INT",
        "month": "INT",
        "year": "INT", 
        "weekday": "INT",
    }
}


# CREATION AND DELETION
create_table_queries = [
    "CREATE TABLE IF NOT EXISTS {name} ({data});".format(
        name=name,
        data=", ".join([f"{name} {text}" for name, text in table.items()])
    ) for name, table in tables.items()
]
drop_table_queries = [f"DROP TABLE IF EXISTS {name}" for name in tables.keys()]


# INSERT RECORDS
insert_table_queries = {
    name: "INSERT INTO {name} ({columns}) VALUES ({data})".format(
        name=name,
        columns=", ".join(table.keys()),
        data=", ".join(["%s"]*len(table.keys()))
    ) for name, table in tables.items()
}

# Avoid duplicates
insert_table_queries["artists"] += " ON CONFLICT (id) DO NOTHING"
insert_table_queries["users"] += " ON CONFLICT (id) DO NOTHING"

# FIND SONGS
song_select = """
    SELECT s.id, a.id
    FROM songs s
    JOIN artists a ON s.artist_id = a.id
    WHERE s.title = %s AND a.name = %s AND s.duration = %s
"""


