"""Requirement 1 - convert the crime CSV data to Parquet (run once)."""

from common import get_spark, read_crime, timed, CRIME_PARQUET


def main():
    spark = get_spark("convert-crime-to-parquet")
    df = read_crime(spark, fmt="csv")

    with timed("convert csv -> parquet"):
        df.write.mode("overwrite").parquet(CRIME_PARQUET)

    print(f"[INFO] rows: {spark.read.parquet(CRIME_PARQUET).count():,}")

    spark.stop()


if __name__ == "__main__":
    main()
