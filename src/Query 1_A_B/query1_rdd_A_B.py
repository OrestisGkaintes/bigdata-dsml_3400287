"""
Query 1 - RDD API, with two time-measuring methods.

Uses RDDs instead of DataFrames. Reads the two crime CSV files as raw text
and parses each line with csv.reader (some fields contain commas inside quotes).

  Method A: perf_counter inside the code -> prints a [TIMING] line.
  Method B: Spark application duration    -> prints APPLICATION_ID so the
            runtime is read from the Spark History Server / UI.

Usage:
  spark-submit --py-files src/common.py src/query1_rdd.py [FORMAT] \
      [--timing {A,B,both}] [--app-name NAME]

  FORMAT only accepts 'csv' here: the RDD text API reads raw CSV. Parquet is a
  columnar binary format and is not read natively as text, so it is not
  supported by this implementation (by design, see Requirement 2).
"""
import argparse
import csv
import io

from common_A_B import (get_spark, part_of_day_py, timed, print_app_info,
                    CRIME_2010_2019, CRIME_2020_2025)


def to_records(raw):
    """Convert a raw text RDD into (TIME OCC, Premis Desc) pairs."""
    header = raw.first()
    cols = next(csv.reader(io.StringIO(header)))
    ti, pi = cols.index("TIME OCC"), cols.index("Premis Desc")

    def parse(line):
        try:
            f = next(csv.reader(io.StringIO(line)))
            return (f[ti], f[pi])
        except Exception:
            return None

    return (raw.filter(lambda l: l != header)
               .map(parse)
               .filter(lambda x: x is not None))


def to_part(t):
    """Convert TIME OCC into a part of the day (e.g. 1345 -> 13, 45 -> 0)."""
    try:
        return part_of_day_py(int(float(t)) // 100)
    except Exception:
        return None


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("format", nargs="?", default="csv",
                   choices=["csv"], help="input format (RDD supports csv only)")
    p.add_argument("--timing", default="both",
                   choices=["A", "B", "both"], help="time measuring method")
    p.add_argument("--app-name", default=None,
                   help="unique app name for Method B / History Server")
    return p.parse_args()


def main():
    args = parse_args()
    use_a = args.timing in ("A", "both")
    use_b = args.timing in ("B", "both")

    app_name = args.app_name or "Q1-RDD-csv"
    spark = get_spark(app_name)
    sc = spark.sparkContext

    # Method B: print the application id early so it is available even on failure.
    if use_b:
        print_app_info(spark)

    # Read both CSV files as text RDDs, parse them, and combine them.
    recs = to_records(sc.textFile(CRIME_2010_2019)) \
        .union(to_records(sc.textFile(CRIME_2020_2025)))

    # Method A: time the whole compute span, ending on the collect() action.
    with timed("Q1 RDD (csv)", enabled=use_a):
        per_part = (recs
                    .map(lambda x: (to_part(x[0]),
                                    (1 if x[1] == "STREET" else 0, 1)))
                    .filter(lambda kv: kv[0] is not None)
                    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))

        result = (per_part
                  .map(lambda kv: (kv[0], kv[1][0], kv[1][1],
                                   round(100.0 * kv[1][0] / kv[1][1], 2)))
                  .sortBy(lambda r: -r[3])
                  .collect())

    print("part\tstreet_count\tpart_total\tpct")
    for part, cnt, part_total, pct in result:
        print(f"{part}\t{cnt}\t{part_total}\t{pct}")

    spark.stop()


if __name__ == "__main__":
    main()