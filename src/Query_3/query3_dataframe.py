"""
Query 3 - DataFrame API

Average annual per-capita income per ZIP Code, combining:
- 2020 census population/housing data from LA_Census_Blocks_2020.geojson
- 2021 median household income from LA_income_2021.csv

Formula:
    per_capita_income = median_income * households / population

Where:
    population = sum of POP20 by ZIP
    households = sum of HOUSING20 by ZIP
"""

import sys
from pyspark.sql import functions as F

from common import get_spark, timed, CENSUS_BLOCKS, INCOME


def load_blocks(spark):
    """
    Load census block GeoJSON and aggregate population/housing by ZIP.
    """
    return (
        spark.read
        .option("multiLine", True)
        .json(CENSUS_BLOCKS)
        .select(F.explode("features").alias("feature"))
        .select(
            F.col("feature.properties.ZCTA20").alias("zip"),
            F.col("feature.properties.POP20").cast("long").alias("population"),
            F.col("feature.properties.HOUSING20").cast("long").alias("households")
        )
        .where(F.col("zip").isNotNull())
        .groupBy("zip")
        .agg(
            F.sum("population").alias("population"),
            F.sum("households").alias("households")
        )
    )


def load_income(spark):
    """
    Load income CSV and clean median income values like '$52,806'.
    Invalid rows such as '---' are removed.
    """
    return (
        spark.read
        .option("header", True)
        .option("delimiter", ";")
        .csv(INCOME)
        .select(
            F.col("Zip Code").alias("zip"),
            F.regexp_replace(
                F.regexp_extract("Estimated Median Income", r"^\$([\d,]+)$", 1),
                ",",
                ""
            ).cast("double").alias("median_income")
        )
        .where(F.col("median_income").isNotNull())
    )


def main():
    # Argument kept for compatibility with the project runner.
    _ = sys.argv[1] if len(sys.argv) > 1 else "csv"

    spark = get_spark("Q3-DataFrame")

    try:
        with timed("Q3 DataFrame"):
            census_by_zip = load_blocks(spark)
            income_by_zip = load_income(spark)

            result = (
                census_by_zip
                .join(income_by_zip, "zip")
                .where(F.col("population") > 0)
                .withColumn(
                    "per_capita_income",
                    F.round(
                        F.col("median_income") * F.col("households") / F.col("population"),
                        2
                    )
                )
                .select(
                    "zip",
                    "population",
                    "households",
                    "median_income",
                    "per_capita_income"
                )
                .orderBy("zip")
            )

            result.show(60, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
