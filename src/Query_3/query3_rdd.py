"""
Query 3 - RDD API

Average annual per-capita income per ZIP Code, combining:
- 2020 census population/housing data from LA_Census_Blocks_2020.geojson
- 2021 median household income from LA_income_2021.csv

Formula:
    per_capita_income = median_income * households / population

Where:
    population = sum of POP20 by ZIP
    households = sum of HOUSING20 by ZIP

Notes:
    The census file is a multiline GeoJSON FeatureCollection. Spark's textFile()
    is not convenient for parsing it directly as an RDD because the logical JSON
    object spans multiple lines. Therefore, this script uses Spark's JSON reader
    only to load and explode the GeoJSON features. After that, the aggregation,
    join, filtering, calculation, and sorting are performed with RDD operations.
"""

import re
import sys

from pyspark.sql import functions as F

from common import get_spark, timed, CENSUS_BLOCKS, INCOME


def safe_int(value):
    """
    Convert a value to int.

    Returns 0 for None or invalid values. This is useful for census numeric
    fields such as POP20 and HOUSING20.
    """
    try:
        return int(value) if value is not None else 0
    except (TypeError, ValueError):
        return 0


def parse_income(value):
    """
    Convert formatted income strings to float.

    Examples:
        '$52,806' -> 52806.0
        '$104,252' -> 104252.0
        '---' -> None
        'No 2021 estimate available' -> None
    """
    if value is None:
        return None

    match = re.match(r"^\$([\d,]+)$", value.strip())

    if not match:
        return None

    return float(match.group(1).replace(",", ""))


def load_blocks_rdd(spark):
    """
    Load census block GeoJSON and aggregate population/housing by ZIP using RDDs.

    Returns:
        RDD[(zip, (population, households))]
    """
    blocks_df = (
        spark.read
        .option("multiLine", True)
        .json(CENSUS_BLOCKS)
        .select(F.explode("features").alias("feature"))
        .select(
            F.col("feature.properties.ZCTA20").alias("zip"),
            F.col("feature.properties.POP20").alias("population"),
            F.col("feature.properties.HOUSING20").alias("households")
        )
    )

    return (
        blocks_df.rdd
        .filter(lambda row: row["zip"] is not None)
        .map(
            lambda row: (
                row["zip"],
                (
                    safe_int(row["population"]),
                    safe_int(row["households"])
                )
            )
        )
        .reduceByKey(
            lambda a, b: (
                a[0] + b[0],
                a[1] + b[1]
            )
        )
    )


def load_income_rdd(sc):
    """
    Load income CSV and clean median income values using RDD transformations.

    Returns:
        RDD[(zip, median_income)]
    """
    return (
        sc.textFile(INCOME)
        .map(lambda line: line.split(";"))
        .filter(lambda parts: len(parts) >= 3)
        .filter(lambda parts: parts[0] != "Zip Code")
        .map(lambda parts: (parts[0], parse_income(parts[2])))
        .filter(lambda pair: pair[1] is not None)
    )


def compute_per_capita_income(pop_by_zip, income_by_zip):
    """
    Join census and income RDDs and compute per-capita income.

    Input:
        pop_by_zip:
            RDD[(zip, (population, households))]

        income_by_zip:
            RDD[(zip, median_income)]

    Output:
        RDD[(zip, population, households, median_income, per_capita_income)]
    """
    return (
        pop_by_zip
        .join(income_by_zip)
        .filter(lambda pair: pair[1][0][0] > 0)
        .map(
            lambda pair: (
                pair[0],
                pair[1][0][0],
                pair[1][0][1],
                pair[1][1],
                pair[1][1] * pair[1][0][1] / pair[1][0][0]
            )
        )
        .sortBy(lambda row: row[0])
    )


def main():
    # Argument kept for compatibility with the project runner.
    _ = sys.argv[1] if len(sys.argv) > 1 else "csv"

    spark = get_spark("Q3-RDD")
    sc = spark.sparkContext

    try:
        with timed("Q3 RDD"):
            pop_by_zip = load_blocks_rdd(spark)
            income_by_zip = load_income_rdd(sc)

            result = compute_per_capita_income(pop_by_zip, income_by_zip)

            print("zip\tpopulation\thouseholds\tmedian_income\tper_capita_income")

            for zip_code, population, households, median_income, per_capita_income in result.collect():
                print(
                    f"{zip_code}\t"
                    f"{population}\t"
                    f"{households}\t"
                    f"{median_income:.0f}\t"
                    f"{per_capita_income:.2f}"
                )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
