# Udacity Data Engineer Final Project

In this project there is all code to automatically extract flight data for further analysis.

It uses `luigi` as an orchestrator and a mix of `pandas` and `pyspark` for the ETL.

This ETL basically does two thins:

1. Create a table with all airports and their data
2. Create a table with prices for the asked flights

This will allow users to analyze prizes and help them book cheap flights.

## 1. Usage and configuration

### 1.1. RapidAPI key

Rapid API [Flight Search](https://rapidapi.com/skyscanner/api/skyscanner-flight-search) is used in order to retrive flights prices.
This API allowes to query some pairs of airports daily for free.

> You will need a rapidapi key. You can register [here](https://rapidapi.com/skyscanner/api/skyscanner-flight-search) to get one (for free).

### 1.2. Files and folders

This is the expected project structure:

```
Project/
│
├── data/
│   ├── flights/
│   │   └── YYYY_MM_DD/
│   │   
│   ├── flights.parquet
│   └── airports.pickle
│
├── logs/
│   └── YYYYMMDD.log
│
├── runs/
│   └── YYYYMMDD/
│
├── src/
│   ├── airports.py
│   ├── check_airports.py
│   ├── check_csvs.py
│   ├── luigi_utils.py
│   ├── master.py
│   ├── merge_data.py
│   ├── rapidapi.py
│   └── utilities.py
│
├── .gitignore
├── config.cfg
├── luigi.cfg
├── pyproject.toml
├── README.md
└── requirements.txt
```

There are some required files that are `gitignored` and need to be created:

* folders:
    * logs
    * runs
    * data
    * data/flights
* files:
    * config.cfg

For the `config.cfg` below is an example of the needed parameters:

```cfg
[RAPIDAPI]
KEY=xxxx

[PATHS]
DATA=data/
LOGS=logs/
RUNS=runs/

[AIRPORTS]
ORIGINS=BCN,CAG,GRO,AHO,BRU,VIE,LGW
LIMIT=5000

[LOG]
LEVEL=DEBUG
```

**You must create the file `config.cfg` with the required parameters.**

### 1.3. Runing the pipeline

You will need 2 process to run the ETL:

The first is for the luigi scheduler and can be started with:

```sh
luigid
```

And the second is for running the tasks with:

```sh
# You need to run it at the root level, the same where this README is located
python src/master.py
```

### 1.4. Configuration

All configurations are done using the `config.cfg` file. As an example:

* AIRPORTS
    * ORIGINS: this is a list with airports IATA codes
    * LIMIT: max pairs of airports to query (allows to do fast testing)

* LOG
    * LEVEL: String representing minimum log level (like `DEBUG`, `INFO`, `WARNING`...)

## 2. How it works?

### 2.1. Luigi custom StandardTask

I have created a `StandardTask` that extends the **Luigi** original task so that it is really easy to add new tasks.

What you basically need to do is to create a new task with the following code:

```python
class ReportsTask(StandardTask):
    module = "reports"
    priority = 80

    def requires(self):
        yield MoneyLoverTask(self.mdate)
```

And you need a `script` or `package` inside the `src` folder that has a function called `main`.
Essentially what **Luigi** will do is:

```python
module = __import__(self.module)
module.main(self.mdate)
```

You can read more about it [here](https://villoro.com/post/luigi).

### 2.1. Tasks

There 5 tasks:

1. GetAirports: Extract the airports data from internet.
2. CheckAirports: Check that the airports codes in `config.cfg` are valid IATA. 
3. RapidApi: Use RapidAPI to query the flight prices
4. CheckCsvs: Check that there is flight data to merge
5. MergeData: Merges all csv created in RapidApi and exports a parquet file

The task `RapidApi` will extract csv files and then `MergeData` will join them into a parquet file.
The idea behind this design is that this way is possible to desing multiple crawlers that scan some pairs of airports in parallel. And then `MergeData` can unify everything very fast using spark.

> NOTE: There are some sleeps in `RapidApi` to avoid exceeding the free quota. If reached it will sleep more. It is possible to changes those times in order to minimize the total querying time. However the default values are well tested and give a good performance.

## 3. Data

The data extracted will consist of those 2 tables:

```yaml
airports:
    ident: string
    type: string
    name: string
    elevation_ft: float
    continent: string
    iso_country: string
    iso_region: string
    municipality: string
    gps_code: string
    iata_code: string
    local_code: string
    coordinates: string

flights:
    Price: float
    Direct: Boolean
    Quote_date: datetime
    Date: datetime
    Carrier: string
    Origin: string
    Destination: string
    Inserted: string (represents a date with format yyyymmdd)
```

### 3.1. Airports

This table is really small so is stored as a `pickle`. It can easily be modified to stored it in a SQL database.

### 3.2. Flights

This table is expected to be big. Is stored as a `parquet` partitioned by the `Inserted` column. The ETL will only update the current date leaving the old data untouched.

## 4. Next steps

This project is a base for creating a database with flight prices.
With the current state is designed to work locally but it can be easily changed to handle a lot of data

### 4.1. Using the cloud

One way of improving it is to store data in a cloud provider (AWS, Azure, GCP...).
It is only needed to change the paths and some conections.

With the design that first extract the `csv` and the gets merged into a `parquet` it should be able to handle billions of data and more

### 4.2. Running it automatically

Since it uses `luigi` there are only 2 things to do to achive that:

1. Create a service in order to have `luigid` running in the background
2. Create a cron that calls `python src/master.py` each day at the desired hour

### 4.3. Improving the performance

The weakest point is the `RapidApi` task. The problem is that there is some query limits so it is really slow. One way to solve that is to have multiple workers (**with multiple API keys**) that handle queries in parallel.

This can be done with different machines or using some serverless solutions like **AWS lambda**.

## 5. Authors
* [Arnau Villoro](villoro.com)

## 6. License
The content of this repository is licensed under a [MIT](https://opensource.org/licenses/MIT).
