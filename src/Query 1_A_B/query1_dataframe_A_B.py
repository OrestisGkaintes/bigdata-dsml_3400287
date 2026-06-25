"""Query 1 - DataFrame API (no UDF), with two time-measuring methods.

  Method A: perf_counter inside the code  -> prints a [TIMING] line.
  Method B: Spark application duration     -> prints APPLICATION_ID so the
            runtime is read from the Spark History Server / UI.

Usage:
  spark-submit --py-files src/common.py src/query1_dataframe.py [FORMAT] \
      [--timing {A,B,both}] [--app-name NAME]

  FORMAT is csv (default) or parquet.
"""
import argparse
from pyspark.sql import functions as F

from common_A_B import get_spark, read_crime, timed, print_app_info


def part_col(hour):
    return (F.when((hour >= 5) & (hour <= 11), "Morning")
             .when((hour >= 12) & (hour <= 16), "Afternoon")
             .when((hour >= 17) & (hour <= 20), "Evening")
             .otherwise("Night"))


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

    app_name = args.app_name or f"Q1-DataFrame-{fmt}"
    spark = get_spark(app_name)

    # Method B: print the application id early so it is available even on failure.
    if use_b:
        print_app_info(spark)

    crime = read_crime(spark, fmt)
    hour = (F.col("TIME OCC").cast("int") / 100).cast("int")
    crime = crime.withColumn("part", part_col(hour))

    # Method A: time the final action (everything materialises at show()).
    with timed(f"Q1 DataFrame ({fmt})", enabled=use_a):
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