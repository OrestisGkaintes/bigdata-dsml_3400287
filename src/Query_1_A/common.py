"""Common helper functions for the LSDM 2025-26 semester project.

This file is used by the different project scripts. When running the code on
the cluster, it should be included with `--py-files src/common.py` so that Spark
can find and import it correctly.
"""
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

# --- HDFS locations -------------------------------------------------------
HDFS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DATA = f"{HDFS}/data"
# Your private HDFS area (used for the Parquet copy in Requirement 1).
USER_HDFS = f"{HDFS}/user/dsml00287"

CRIME_2010_2019 = f"{DATA}/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020_2025 = f"{DATA}/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
CENSUS_BLOCKS   = f"{DATA}/LA_Census_Blocks_2020.geojson"
CENSUS_FIELDS   = f"{DATA}/LA_Census_Blocks_2020_fields.csv"
INCOME          = f"{DATA}/LA_income_2021.csv"
POLICE_STATIONS = f"{DATA}/LA_Police_Stations.csv"
CRIME_PARQUET   = f"{USER_HDFS}/crime_parquet"


def get_spark(app_name: str) -> SparkSession:
    """Create and return a SparkSession for the current script.

    The cluster resources, such as executors, cores, and memory, are given from
    the command line. This makes it easier to test the same code with different
    Spark configurations.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


@contextmanager
def timed(label: str):
    """Measure the execution time of a Spark action or code block.

    The action, for example count(), show(), collect(), or write(), must be
    inside this block. This is needed because Spark uses lazy evaluation and
    does not execute transformations immediately.
    """
    start = time.perf_counter()
    yield
    print(f"[TIMING] {label}: {time.perf_counter() - start:.3f} s")


def part_of_day_py(hour):
    """Return the part of the day based on the given hour.

    This simple Python function is reused in the UDF and RDD solutions.
    """
    if hour is None:
        return None
    if 5 <= hour <= 11:
        return "Morning"      # 05:00 - 11:59
    if 12 <= hour <= 16:
        return "Afternoon"    # 12:00 - 16:59
    if 17 <= hour <= 20:
        return "Evening"      # 17:00 - 20:59
    return "Night"            # 21:00 - 04:59


def read_crime(spark: SparkSession, fmt: str = "csv"):
    """Read the two crime CSV files and combine them into one DataFrame.

    Before combining them, the column names are cleaned with strip(), because
    one of the files has small formatting differences in some column names
    such as an extra space. Schema inference is not used here, so the columns
    are first read as strings and are converted later where needed.
    """
    if fmt == "parquet":
        return spark.read.parquet(CRIME_PARQUET)

    df1 = spark.read.option("header", True).csv(CRIME_2010_2019)
    df2 = spark.read.option("header", True).csv(CRIME_2020_2025)
    df1 = df1.toDF(*[c.strip() for c in df1.columns])
    df2 = df2.toDF(*[c.strip() for c in df2.columns])
    return df1.unionByName(df2, allowMissingColumns=True)
