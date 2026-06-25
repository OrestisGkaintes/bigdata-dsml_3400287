"""
Query 1 - DataFrame API using a Python UDF, with two time-measuring methods.

This version maps each crime time to a part of the day using a Python UDF.
The UDF is simple to write, but slower than native Spark expressions because
it is applied row by row through Python (no Catalyst codegen; JVM<->Python
serialization per row).

  Method A: perf_counter inside the code -> prints a [TIMING] line.
  Method B: Spark application duration    -> prints APPLICATION_ID so the
            runtime is read from the Spark History Server / UI.

Usage:
  spark-submit --py-files src/common.py src/query1_dataframe_udf.py [FORMAT] \
      [--timing {A,B,both}] [--app-name NAME]

  FORMAT is csv (default) or parquet.
"""
import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common_A_B import get_spark, read_crime, timed, print_app_info, part_of_day_py


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("format", nargs="?", default="csv",
                   choices=["csv", "parquet"], help="input format")
    p.add_argument("--timing", default="both",
                   choices=["A", "B", "both"], help="time measuring method")
    p.add_argument("--app-name", default=None,
                   help="unique app name for Method B / History Server")
    return p.parse_args()


def main():
    args = parse_args()
    fmt = args.format
    use_a = args.timing in ("A", "both")
    use_b = args.timing in ("B", "both")

    app_name = args.app_name or f"Q1-DataFrame-UDF-{fmt}"
    spark = get_spark(app_name)

    # Method B: print the application id early so it is available even on failure.
    if use_b:
        print_app_info(spark)

    crime = read_crime(spark, fmt)

    # Register the Python function as a Spark UDF.
    part_udf = F.udf(part_of_day_py, StringType())

    # Extract the hour from TIME OCC (e.g. 1430 -> 14), then label via the UDF.
    crime = crime.withColumn("hour", (F.col("TIME OCC").cast("int") / 100).cast("int"))
    crime = crime.withColumn("part", part_udf(F.col("hour")))

    # Method A: identical timing boundary to the plain DataFrame version --
    # transformations defined here, final show() action materialises inside.
    with timed(f"Q1 DataFrame+UDF ({fmt})", enabled=use_a):
        per_part = (crime.groupBy("part")
                    .agg(F.sum(F.when(F.col("Premis Desc") == "STREET", 1)
                                .otherwise(0)).alias("street_count"),
                         F.count(F.lit(1)).alias("part_total")))
        result = (per_part
                  .withColumn("pct", F.round(100.0 * F.col("street_count") / F.col("part_total"), 2))
                  .select("part", "street_count", "part_total", "pct")
                  .orderBy(F.desc("pct")))
        result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()