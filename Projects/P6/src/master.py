from datetime import datetime

import luigi

from luigi_utils import StandardTask
from utilities import log


class GetAirportsTask(StandardTask):
    module = "airports"


class RapidApiTask(StandardTask):
    module = "rapidapi"

    def requires(self):
        yield GetAirportsTask()


class CheckCsvsTask(StandardTask):
    module = "check_csvs"

    def requires(self):
        yield RapidApiTask()


class MergeDataTask(StandardTask):
    module = "merge_data"

    def requires(self):
        yield CheckCsvsTask()


class DoAllTask(luigi.WrapperTask):
    def requires(self):
        yield MergeDataTask()


if __name__ == "__main__":

    log.info("Starting all tasks")
    luigi.build([DoAllTask()])

    log.info("End of all tasks")
