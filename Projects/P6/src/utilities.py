"""
    Log utilities using loguru
"""

import os
import sys
import configparser
from datetime import date

from loguru import logger as log


config = configparser.ConfigParser()

# Handle readings from inside src folder
path_root = "../" if os.getcwd().endswith("src") else ""
config.read(f"{path_root}config.cfg")

LOG_CONFIG = {
    "handlers": [
        {"sink": sys.stdout, "level": config["LOG"]["LEVEL"]},
        {"sink": f"{config['PATHS']['LOGS']}/{date.today():%Y_%m_%d}.log", "level": "INFO"},
    ]
}
log.configure(**LOG_CONFIG)
log.enable("vflights")
