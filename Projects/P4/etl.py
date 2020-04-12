import os
import configparser
from datetime import datetime

from pyspark.sql import SparkSession


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """ Creates the spark session including packages to access AWS S3 """

    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def process_song_data(spark, path_in, path_out):
    """
        Exctract data form songs stored in S3

        Args:
            spark:      spark object
            path_in:    root path for input files
            path_out:   root path for output files
    """

    # read song data file
    sdf = spark.read.json(f"{path_in}/song_data/*/*/*/*.json")

    # extract columns to create songs table
    songs = sdf.selectExpr("song_id AS id", "title", "artist_id", "year", "duration")

    # write songs table to parquet files partitioned by year and artist
    path = f"{path_out}/songs.parquet"
    songs.write.partitionBy("year", "artist_id").parquet(path, mode="overwrite")

    # extract columns to create artists table
    artists = sdf.select(
        "artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"
    )

    # write artists table to parquet files
    artists.write.parquet(f"{path_out}/artists.parquet", mode="overwrite")


def process_log_data(spark, path_in, path_out):
    """
        Exctract data form logs stored in S3

        Args:
            spark:      spark object
            path_in:    root path for input files
            path_out:   root path for output files
    """

    # read log data file filter by actions for song plays
    sdf = spark.read.json(f"{path_in}/log_data/*/*/*.json").where("page = 'NextSong'")

    # extract columns for users table
    users = sdf.selectExpr(
        "userId AS user_id", "firstName AS first_name", "lastName AS last_name", "gender", "level"
    ).distinct()

    # write users table to parquet files
    users.write.parquet(f"{path_out}/users.parquet", mode="overwrite")

    # extract columns to create time table
    timestamps = (
        sdf.selectExpr("ts", "from_unixtime(ts / 1000) AS time")
        .distinct()
        .selectExpr(
            "ts AS start_time",
            "hour(time) AS hour",
            "day(time) AS day",
            "weekofyear(time) AS week",
            "month(time) AS month",
            "year(time) AS year",
        )
    )

    # write time table to parquet files partitioned by year and month
    timestamps.write.partitionBy("year", "month").parquet(
        f"{path_out}/time.parquet", mode="overwrite"
    )

    # read in songs and artists data to use as keys for songplays
    spark.read.parquet(f"{path_out}/songs.parquet").createOrReplaceTempView("songs")
    spark.read.parquet(f"{path_out}/artists.parquet").createOrReplaceTempView("artists")

    # extract columns from joined song and log datasets to create songplays table
    sdf.createOrReplaceTempView("events")
    songplays = spark.sql(
        """SELECT
            e.ts AS start_time,
            e.userId AS user_id,
            e.level,
            s.id AS song_id,
            a.artist_id,
            e.sessionId AS session_id,
            e.location,
            e.userAgent AS user_agent,
            year(from_unixtime(e.ts / 1000)) AS year,
            month(from_unixtime(e.ts / 1000)) AS month
        FROM events e
        LEFT JOIN songs s    ON s.title = e.song
        LEFT JOIN artists a  ON a.artist_name = e.artist"""
    )

    # write songplays table to parquet files partitioned by year and month
    songplays.write.partitionBy("year", "month").parquet(
        f"{path_out}/songplays.parquet", mode="overwrite"
    )


def main():
    """
        Process all files from S3 and store relevant data in S3 as parquets
    """

    spark = create_spark_session()
    path_in = "s3a://udacity-dend/"
    path_out = "s3a://v-dend-bucket/P4/"

    process_song_data(spark, path_in, path_out)
    process_log_data(spark, path_in, path_out)


if __name__ == "__main__":
    main()
