import os
import requests
from time import sleep
from datetime import date, timedelta

import pandas as pd

from utilities import config, log

BASE_URL = (
    "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/browseroutes/v1.0/"
)

HEADERS = {
    "x-rapidapi-host": "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
    "x-rapidapi-key": config["RAPIDAPI"]["KEY"],
}


def query_flights(
    origin,
    destination,
    day,
    max_attempts=20,
    seconds_sleep=1,
    country="ES",
    currency="EUR",
    locale="en-US",
):
    """
        Query flights iterating until there is a result

        Args:
            origin:         code for origin airport
            destination:    code for destination airport
            day:            day for the flights [date]
            max_attempts:   number of retries
            seconds_sleep:  seconds to sleep before returning a result
            country:        code for country (default: ES)
            currency:       code for currency (default: EUR)
            locale:         code for output info (default: en-US)
    """

    url = f"{BASE_URL}{country}/{currency}/{locale}/{origin}/{destination}/{day:%Y-%m-%d}"

    for attemp_num in range(max_attempts):

        log.debug(f"Quering {origin}-{destination} for date '{day}' (attempt {attemp_num})")

        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            sleep(seconds_sleep)
            return response

        # If there are 'Too many requests' sleep a little
        elif response.status_code == 429:
            log.warning(f"API limit reached at attempt {attemp_num + 1}")
            sleep(2 * attemp_num + 1)

        # Raise unknown cases
        else:
            response.raise_for_status()

    log.error(f"Number max of attempts reached ({max_attempts})")
    raise TimeoutError("TimeOut")


def fix_places(df, data):
    """
        Map Places id to actual airport code
        
        Args:
            df:     dataframe with flights
            data:   output of the request as dict containg 'Places' info
    """

    # Get places
    df_places = pd.DataFrame(data["Places"])
    places = df_places.set_index("PlaceId")["IataCode"].to_dict()

    # Rename places for origin and destination
    for x in ["Origin", "Destination"]:
        df[x] = df[f"{x}Id"].replace(places)
        df = df.drop(f"{x}Id", axis=1)

    return df


def fix_carriers(df, data):
    """
        Map Carriers id to actual carrier name
        
        Args:
            df:     dataframe with flights
            data:   output of the request as dict containg 'Carriers' info
    """
    # Get carriers
    df_carriers = pd.DataFrame(data["Carriers"])
    carriers = df_carriers.set_index("CarrierId")["Name"].to_dict()

    # Rename carriers
    df["Carrier"] = df["Carrier"].replace(carriers)

    return df


def retrive_quotes(data):
    """
        Get info from quotes as a pandas dataframe
    """

    out = []
    for quote in data["Quotes"]:
        # Extract data from "OutboundLeg" nested dict
        quote.update(quote.pop("OutboundLeg"))

        # For all possible Carrier, create an entry
        for carrier in quote.pop("CarrierIds"):
            aux = quote.copy()
            aux.update({"Carrier": carrier})

            out.append(aux)

    # Create pandas dataframe
    df = pd.DataFrame(out).set_index("QuoteId")
    df.index.name = ""

    return df


def parse_data(data):
    """
        Parse all data from the request and create a pandas dataframe with fligths
    """

    df = retrive_quotes(data)

    # Rename columns
    df = df.rename(
        columns={"MinPrice": "Price", "QuoteDateTime": "Quote_date", "DepartureDate": "Date"}
    )

    # Fix dates
    for x in ["Quote_date", "Date"]:
        df[x] = pd.to_datetime(df[x])

    df = fix_places(df, data)
    df = fix_carriers(df, data)

    df["Inserted"] = date.today().strftime("%Y_%m_%d")

    return df


def query_pair(origin, destination, n_days=366):
    """
        Query all flights between 2 airports

        Args:
            origin:         code for origin airport
            destination:    code for destination airport
            n_days:         max days of history
    """

    # Start at day 1 since it will only query when day==1
    start_day = date.today()

    dfs = []
    for x in range(n_days):
        query_day = start_day + timedelta(x)

        # Only do first day of month
        if (query_day.day != 1) and (query_day != start_day):
            log.trace(f"Skiping day '{query_day}'")
            continue

        response = query_flights(origin, destination, query_day)
        data = response.json()

        if data["Quotes"]:
            dfs.append(parse_data(data))

    if dfs:
        return pd.concat(dfs).reset_index(drop=True)
    else:
        log.warning(f"No flights from '{origin}' to '{destination}'")


def get_pairs():
    """
        Create a list with tuples with pairs of airports. Like:
            [("BCN", "CAG"), ("GRO", "CAG")]

        This is created using all possible combinations from:
            congig.cfg/AIRPORTS/ORIGINDS

        And might be limited the size using congig.cfg/AIRPORTS/LIMIT
    """

    # My airports
    airports = config["AIRPORTS"]["ORIGINS"].split(",")

    pairs = []

    for origin in airports:
        for dest in airports:
            if dest != origin:
                # Append flights from both directions
                pairs.append((dest, origin))
                pairs.append((origin, dest))

    log.info(f"There are {len(pairs)} airport pairs")

    limit = int(config["AIRPORTS"]["LIMIT"])

    if limit > 0:

        pairs = pairs[:limit]
        log.info(f"Limiting the query to {limit} pairs")

    return pairs


def main():
    """
        Get all flights of each pair and export them as a csv for each pair
    """

    path_raw = config["PATHS"]["DATA"] + f"flights/{date.today():%Y_%m_%d}/"

    # Create folder
    os.makedirs(path_raw, exist_ok=True)

    airports_pairs = get_pairs()
    total_pairs = len(airports_pairs)

    for i, (origin, dest) in enumerate(airports_pairs):

        log.info(f"Quering flights from '{origin}' to '{dest}' ({i + 1}/{total_pairs})")
        df = query_pair(origin, dest)

        if df is not None:
            uri = f"{path_raw}{origin}_{dest}.csv"
            df.to_csv(uri, index=False)
            log.debug(f"Exporting '{uri}'")
