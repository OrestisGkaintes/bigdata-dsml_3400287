"""Query 2 - DataFrame API.

For each year, the 3 months with the most crimes. year/month are parsed from
DATE OCC ("MM/DD/YYYY HH:MM:SS AM/PM") by splitting on '/'. We rank per year
with a window and keep the top 3, then order by year asc / crime_total desc.
"""
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import get_spark, read_crime, timed


def main():
    fmt = sys.argv[1] if len(sys.argv) > 1 else "csv"
    spark = get_spark("Q2-DataFrame")
    crime = read_crime(spark, fmt)

    df = (crime
          .withColumn("month", F.split(F.col("DATE OCC"), "/").getItem(0).cast("int"))
          .withColumn("year",
                      F.substring(F.split(F.col("DATE OCC"), "/").getItem(2), 1, 4).cast("int")))

    with timed(f"Q2 DataFrame ({fmt})"):
        counts = df.groupBy("year", "month").agg(F.count(F.lit(1)).alias("crime_total"))
        w = Window.partitionBy("year").orderBy(F.desc("crime_total"))
        ranked = (counts.withColumn("ranking", F.row_number().over(w))
                  .filter(F.col("ranking") <= 3))
        result = ranked.orderBy(F.asc("year"), F.desc("crime_total"))
        result.show(60, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
