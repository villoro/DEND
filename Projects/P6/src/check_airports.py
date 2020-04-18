"""
    Merge data from airports
"""

import pandas as pd

from utilities import log, config


def main():
    """ Check that the airports passed are valid iata codes """

    uri = f"{config['PATHS']['DATA']}airports.pickle"
    df = pd.read_pickle(uri)

    valid_codes = df["iata_code"].dropna().unique()

    airports = config["AIRPORTS"]["ORIGINS"].split(",")

    invalid_airports = [x for x in airports if x not in valid_codes]

    if invalid_airports:
        msg = f"Airports {invalid_airports} are not valid IATA codes"
        log.error(msg)
        raise ValueError(msg)

    log.info("All airports are valid IATA codes")
