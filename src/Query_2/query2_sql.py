"""
Query 2 - SQL version.

In this script, I solve Query 2 using Spark SQL.
The result should be the same as in query2_dataframe.py, where I used the
DataFrame API.

Because Spark uses the Catalyst optimizer for both approaches, the execution
plans should be very similar, so the performance should also be similar.
"""

import sys

from common import get_spark, read_crime, timed


QUERY = """
WITH base AS (
  SELECT
    TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a') AS date_occ_ts
  FROM crime
),
counts AS (
  SELECT
    YEAR(date_occ_ts) AS year,
    MONTH(date_occ_ts) AS month,
    COUNT(*) AS crime_total
  FROM base
  WHERE date_occ_ts IS NOT NULL
  GROUP BY YEAR(date_occ_ts), MONTH(date_occ_ts)
),
ranked AS (
  SELECT
    year,
    month,
    crime_total,
    ROW_NUMBER() OVER (
      PARTITION BY year
      ORDER BY crime_total DESC, month ASC
    ) AS ranking
  FROM counts
)
SELECT
  year,
  month,
  crime_total,
  ranking
FROM ranked
WHERE ranking <= 3
ORDER BY year ASC, ranking ASC
"""


def main():
    fmt = sys.argv[1] if len(sys.argv) > 1 else "csv"

    spark = get_spark("Q2-SQL")

    try:
        read_crime(spark, fmt).createOrReplaceTempView("crime")

        with timed(f"Q2 SQL ({fmt})"):
            spark.sql(QUERY).show(60, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
