import pandas as pd

from utilities import log, config

from . import constants as c
from .api import query_pair


def retrive_all_flights(airports_pairs):
    """
        Get a dataframe with all flights

        Args:
            airports_pairs:  iterable with tuples containing tuple with 2 airports
                             for example [("BCN", "CAG"), ("GRO", "CAG")]
    """

    total_pairs = len(airports_pairs)

    for i, (origin, dest) in enumerate(airports_pairs):

        log.info(f"Quering flights from '{origin}' to '{dest}' ({i + 1}/{total_pairs})")
        df = query_pair(origin, dest)

        if df is not None:
            uri = f"{config['PATHS']['DATA']}flights/{origin}_{dest}.csv"
            df.to_csv(uri, index=False)
            log.debug(f"Exporting '{uri}'")
