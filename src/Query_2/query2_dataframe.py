"""
Query 2 - DataFrame API.

For each year, find the 3 months with the most crimes.
DATE OCC has format: yyyy MMM dd hh:mm:ss a
"""

import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import get_spark, read_crime, timed


def main():
    fmt = sys.argv[1] if len(sys.argv) > 1 else "csv"
    spark = get_spark("Q2-DataFrame")

    try:
        crime = read_crime(spark, fmt)

        with timed(f"Q2 DataFrame ({fmt})"):
            w = Window.partitionBy("year").orderBy(
                F.desc("crime_total"),
                F.asc("month")
            )

            result = (
                crime
                .select(
                    F.to_timestamp("DATE OCC", "yyyy MMM dd hh:mm:ss a").alias("date_occ")
                )
                .where(F.col("date_occ").isNotNull())
                .groupBy(
                    F.year("date_occ").alias("year"),
                    F.month("date_occ").alias("month")
                )
                .count()
                .withColumnRenamed("count", "crime_total")
                .withColumn("ranking", F.row_number().over(w))
                .where(F.col("ranking") <= 3)
                .orderBy("year", "ranking")
            )

            result.show(60, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
