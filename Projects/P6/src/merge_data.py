"""
    Merge data from airports
"""

from pyspark.sql import SparkSession

from utilities import log, config


def main(
    uri_in=f"{config['PATHS']['DATA']}flights/*.csv",
    uri_out=f"{config['PATHS']['DATA']}flights.parquet",
):
    """ Retreive airports """

    spark = SparkSession.builder.getOrCreate()
    sdf = spark.read.option("header", "true").csv(uri_in)

    log.info(f"Merging {sdf.count()} rows of flights data")

    sdf.write.partitionBy("Inserted").parquet(uri_out, mode="overwrite")
    log.info(f"File '{uri_out}' exported")
