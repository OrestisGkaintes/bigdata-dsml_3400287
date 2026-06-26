"""
Query 1 - RDD API.

This version uses RDDs instead of DataFrames.
It reads the two crime CSV files as raw text and parses each line with csv.reader,
because some fields may contain commas inside quotes.
"""

import sys
import csv
import io

from common import (get_spark, part_of_day_py, timed,
                    CRIME_2010_2019, CRIME_2020_2025)


def to_records(raw):
    """Convert a raw text RDD into (TIME OCC, Premis Desc) pairs."""

    # Get the first line of the file, which contains the column names.
    header = raw.first()

    # Parse the header using csv.reader, not split(","), to handle quoted commas.
    cols = next(csv.reader(io.StringIO(header)))

    # Find the positions of the columns we need.
    ti, pi = cols.index("TIME OCC"), cols.index("Premis Desc")

    def parse(line):
        # Parse one CSV line and return only the needed fields.
        try:
            f = next(csv.reader(io.StringIO(line)))
            return (f[ti], f[pi])
        except Exception:
            # If a line is broken or cannot be parsed, skip it later.
            return None

    # Remove the header, parse each line, and keep only valid records.
    return (raw.filter(lambda l: l != header)
               .map(parse)
               .filter(lambda x: x is not None))


def to_part(t):
    """Convert TIME OCC into a part of the day."""

    try:
        # Convert time to hour.
        # Example: 1345 -> 13, 45 -> 0.
        return part_of_day_py(int(float(t)) // 100)
    except Exception:
        # If the time is missing or invalid, return None.
        return None


def main():
    # Start Spark.
    spark = get_spark("Q1-RDD")
    sc = spark.sparkContext

    # Read both CSV files as text RDDs, parse them, and combine them.
    recs = to_records(sc.textFile(CRIME_2010_2019)) \
        .union(to_records(sc.textFile(CRIME_2020_2025)))

    # Measure the time of the main RDD operations.
    with timed("Q1 RDD"):

        # For each record, create:
        # (part, (street_flag, total_flag))
        #
        # street_flag = 1 if Premis Desc is STREET, else 0
        # total_flag = 1 for every valid record
        per_part = (recs
                    .map(lambda x: (to_part(x[0]),
                                    (1 if x[1] == "STREET" else 0, 1)))

                    # Remove records where the time could not be converted.
                    .filter(lambda kv: kv[0] is not None)

                    # Add the STREET counts and total counts for each part.
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))

        # Convert each result into:
        # (part, street_count, part_total, percentage)
        result = (per_part
                  .map(lambda kv: (kv[0], kv[1][0], kv[1][1],
                                   round(100.0 * kv[1][0] / kv[1][1], 2)))

                  # Sort by percentage from highest to lowest.
                  .sortBy(lambda r: -r[3])

                  # Bring the final small result back to the driver.
                  .collect())

    # Print the result in a simple table format.
    print("part\tstreet_count\tpart_total\tpct")
    for part, cnt, part_total, pct in result:
        print(f"{part}\t{cnt}\t{part_total}\t{pct}")

    # Stop Spark.
    spark.stop()


if __name__ == "__main__":
    main()
