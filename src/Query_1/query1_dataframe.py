"""Query 1 - Solution using the DataFrame API.

In this query, I calculate for each part of the day how many crimes happened
on STREET. Then I compare this number with the total number of crime
records and calculate the percentage.

The parts of the day are:
- Morning
- Afternoon
- Evening
- Night

This version does not use a UDF. It uses Spark DataFrame functions.
"""
import sys
from pyspark.sql import functions as F

from common import get_spark, read_crime, timed


def part_col(hour):
    """Return the part of the day based on the hour of the crime."""
    return (F.when((hour >= 5) & (hour <= 11), "Morning")
             .when((hour >= 12) & (hour <= 16), "Afternoon")
             .when((hour >= 17) & (hour <= 20), "Evening")
             .otherwise("Night"))


def main():
    # Read the input format from the command line.
    # If no format is given, csv is used by default.
    fmt = sys.argv[1] if len(sys.argv) > 1 else "csv"

    # Create the Spark session for this query.
    spark = get_spark("Q1-DataFrame")

    # Read the crime dataset.
    crime = read_crime(spark, fmt)

    # Convert the TIME OCC column to an hour.
    # For example, 1345 becomes 13.
    hour = (F.col("TIME OCC").cast("int") / 100).cast("int")

    # Add a new column called part, which shows the part of the day.
    crime = crime.withColumn("part", part_col(hour))

    # Measure the execution time of the main Spark operations.
    with timed(f"Q1 DataFrame ({fmt})"):

        # Group the records by part of the day.
        # For each part, count:
        # 1. how many crimes happened on STREET
        # 2. how many total crimes happened in that part of the day
        per_part = (crime.groupBy("part")
                    .agg(F.sum(F.when(F.col("Premis Desc") == "STREET", 1)
                                .otherwise(0)).alias("street_count"),
                         F.count(F.lit(1)).alias("part_total")))

        # Calculate the percentage of street crimes for each part of the day.
        # The denominator is the total number of crimes IN THAT PART of the day,
        # so each percentage stays between 0 and 100.
        # Then keep only the needed columns and sort the result by percentage.
        result = (per_part
                  .withColumn("pct", F.round(100.0 * F.col("street_count") / F.col("part_total"), 2))
                  .select("part", "street_count", "part_total", "pct")
                  .orderBy(F.desc("pct")))

        # Print the final result.
        result.show(truncate=False)

    # Stop the Spark session.
    spark.stop()
    

if __name__ == "__main__":
    main()
