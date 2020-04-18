"""
    Log utilities using loguru
"""

import sys
import configparser
from datetime import date

from loguru import logger as log


config = configparser.ConfigParser()
config.read("../config.cfg")

LOG_CONFIG = {
    "handlers": [
        {"sink": sys.stdout, "level": "DEBUG"},
        {"sink": f"{config['PATHS']['LOGS']}/{date.today():%Y_%m_%d}.log", "level": "INFO"},
    ]
}
log.configure(**LOG_CONFIG)
log.enable("vflights")
