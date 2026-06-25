"""Query 2 - SQL API.

Same result as query2_dataframe.py, expressed in Spark SQL. Catalyst should
produce a near-identical plan, so the execution times are expected to be very
close (comment on this in the report).
"""
import sys

from common import get_spark, read_crime, timed

QUERY = """
WITH base AS (
  SELECT
    CAST(SPLIT(`DATE OCC`, '/')[0] AS INT)                 AS month,
    CAST(SUBSTRING(SPLIT(`DATE OCC`, '/')[2], 1, 4) AS INT) AS year
  FROM crime
),
counts AS (
  SELECT year, month, COUNT(*) AS crime_total
  FROM base
  GROUP BY year, month
),
ranked AS (
  SELECT year, month, crime_total,
         ROW_NUMBER() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
  FROM counts
)
SELECT year, month, crime_total, ranking
FROM ranked
WHERE ranking <= 3
ORDER BY year ASC, crime_total DESC
"""


def main():
    fmt = sys.argv[1] if len(sys.argv) > 1 else "csv"
    spark = get_spark("Q2-SQL")
    read_crime(spark, fmt).createOrReplaceTempView("crime")

    with timed(f"Q2 SQL ({fmt})"):
        spark.sql(QUERY).show(60, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
