"""
    Merge data from airports
"""

from pyspark.sql import SparkSession

from utilities import log, config


def check(path=f"{config['PATHS']['DATA']}flights/",):
    """ Check data there is data in the flights folder """

    spark = SparkSession.builder.getOrCreate()
    sdf = spark.read.option("header", "true").csv(f"{path}*.csv")

    n_rows = sdf.count()

    if n_rows == 0:
    	msg = f"There is no data in {path}"
        log.info(msg)
        raise ValueError(msg)

    log.info(f"There are {n_rows} rows of flights data")
