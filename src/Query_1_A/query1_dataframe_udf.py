"""
Query 1 - DataFrame API using a Python UDF.

This version maps each crime time to a part of the day using a Python UDF.
The UDF is simple to write, but slower than native Spark expressions because
it is applied row by row through Python.
"""
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from common import get_spark, read_crime, timed, part_of_day_py


def main():
    # Get input format; default is CSV.
    fmt = sys.argv[1] if len(sys.argv) > 1 else "csv"

    # Start Spark and load the dataset.
    spark = get_spark("Q1-DataFrame-UDF")
    crime = read_crime(spark, fmt)

    # Register the Python function as a Spark UDF.
    part_udf = F.udf(part_of_day_py, StringType())

    # Extract the hour from TIME OCC, e.g. 1430 becomes 14.
    crime = crime.withColumn("hour", (F.col("TIME OCC").cast("int") / 100).cast("int"))

    # Convert each hour into a part of the day using the UDF.
    crime = crime.withColumn("part", part_udf(F.col("hour")))

    # Time only the main query operations.
    with timed(f"Q1 DataFrame+UDF ({fmt})"):

        # For each part of the day, count STREET crimes and total crimes.
        per_part = (crime.groupBy("part")
                    .agg(F.sum(F.when(F.col("Premis Desc") == "STREET", 1)
                                .otherwise(0)).alias("street_count"),
                         F.count(F.lit(1)).alias("part_total")))

        # Calculate the percentage of STREET crimes for each part of the day.
        # The denominator is the total number of crimes IN THAT PART of the day,
        # so each percentage stays between 0 and 100.
        # Then keep only the needed columns and sort from highest to lowest.
        result = (per_part
                  .withColumn("pct", F.round(100.0 * F.col("street_count") / F.col("part_total"), 2))
                  .select("part", "street_count", "part_total", "pct")
                  .orderBy(F.desc("pct")))

        # Show final answer.
        result.show(truncate=False)

    # Close Spark session.
    spark.stop()

# Run main only when this file is executed directly.
if __name__ == "__main__":
    main()