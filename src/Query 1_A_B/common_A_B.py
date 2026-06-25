"""Common helper functions for the LSDM 2025-26 semester project."""
import time
from contextlib import contextmanager

from pyspark.sql import SparkSession

# --- HDFS locations -------------------------------------------------------
HDFS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DATA = f"{HDFS}/data"
USER_HDFS = f"{HDFS}/user/dsml00287"

CRIME_2010_2019 = f"{DATA}/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020_2025 = f"{DATA}/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
CENSUS_BLOCKS   = f"{DATA}/LA_Census_Blocks_2020.geojson"
CENSUS_FIELDS   = f"{DATA}/LA_Census_Blocks_2020_fields.csv"
INCOME          = f"{DATA}/LA_income_2021.csv"
POLICE_STATIONS = f"{DATA}/LA_Police_Stations.csv"
CRIME_PARQUET   = f"{USER_HDFS}/crime_parquet"


def get_spark(app_name: str) -> SparkSession:
    """Create and return a SparkSession. Resources come from spark-submit."""
    return SparkSession.builder.appName(app_name).getOrCreate()


@contextmanager
def timed(label: str, enabled: bool = True):
    """Method A: measure wall time of the enclosed Spark action.

    The body always runs (so the action executes). The [TIMING] line is only
    printed when enabled=True, so Method A can be turned off from the terminal.
    """
    start = time.perf_counter()
    yield
    if enabled:
        print(f"[TIMING] {label}: {time.perf_counter() - start:.3f} s")


def print_app_info(spark: SparkSession):
    """Method B: print what is needed to look the run up in the History Server.

    Spark's own runtime ('Duration') is read from the History Server / UI,
    located by the Application ID printed here. Use a unique app name per run.
    """
    sc = spark.sparkContext
    print(f"[METHOD B] APP_NAME={sc.appName}")
    print(f"[METHOD B] APPLICATION_ID={sc.applicationId}")
    print("[METHOD B] Read the 'Duration' for this Application ID in the "
          "Spark History Server / UI for the official runtime.")


def part_of_day_py(hour):
    """Return the part of the day based on the given hour (reused in UDF/RDD)."""
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
    """Read the crime data (csv or parquet) into one DataFrame."""
    if fmt == "parquet":
        return spark.read.parquet(CRIME_PARQUET)

    df1 = spark.read.option("header", True).csv(CRIME_2010_2019)
    df2 = spark.read.option("header", True).csv(CRIME_2020_2025)
    df1 = df1.toDF(*[c.strip() for c in df1.columns])
    df2 = df2.toDF(*[c.strip() for c in df2.columns])
    return df1.unionByName(df2, allowMissingColumns=True)