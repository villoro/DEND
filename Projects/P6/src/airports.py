"""
    Code to retrive airports
"""

import requests
from io import StringIO

import pandas as pd

from utilities import log, config


def get_airports():
    """ Retreive airports """

    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

    url = "https://datahub.io/core/airport-codes/r/airport-codes.csv"
    response = requests.get(url, headers=header)
    response.raise_for_status()

    log.debug("Airport data retrived")

    df = pd.read_csv(StringIO(response.text))
    log.debug("Airport data parsed")

    return df


def fix_encodings(dfi):
    """
        Fix some encoding problems with latin1.
        
        For example:
             AerÃÂ²drom de Pals --> Aeròdrom de Pals
    """

    df = dfi.copy()

    def _fix_latin(x):
        """ Decode latin1 and encode as utf8 """

        if pd.isna(x):
            return x
        return x.encode("latin1").decode("utf8", "ignore")

    for col in ["name", "municipality"]:
        # Not sure why but it needs to be applyed twice
        df[col] = df[col].apply(_fix_latin).apply(_fix_latin)

    log.debug("String encodings fixed")

    return df


def main():
    """ Read the airport data from internet and store it as a pickle """

    df = get_airports()
    df = fix_encodings(df)

    uri = f"{config['PATHS']['DATA']}airports.pickle"
    df.to_pickle(uri)
    log.info(f"Airport data exported to '{uri}'")
