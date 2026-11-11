"""Create NYC Taxi tip-rate summaries by distance and trip-duration buckets."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F

from pyspark_aggregate import normalized_trips


def write_summary(df, group_column: str, output_path: str) -> None:
    (
        df.groupBy(group_column)
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("tip_amount").alias("average_tip_amount"),
            F.avg(F.col("tip_amount") / F.col("fare_amount")).alias("average_tip_rate"),
        )
        .orderBy(group_column)
        .coalesce(1)
        .write.mode("overwrite").option("header", True).csv(output_path)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Parquet path or glob, local or gs:// URI")
    parser.add_argument("--output", required=True, help="Output directory, local or gs:// URI")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("nyc-taxi-phase4-analysis").getOrCreate()
    trips = normalized_trips(spark.read.parquet(args.input))
    distance = trips.withColumn(
        "distance_bucket",
        F.when(F.col("trip_distance") < 1, "under_1_mile")
        .when(F.col("trip_distance") < 3, "1_to_3_miles")
        .when(F.col("trip_distance") < 7, "3_to_7_miles")
        .when(F.col("trip_distance") < 15, "7_to_15_miles")
        .otherwise("15_plus_miles"),
    )
    duration = trips.withColumn(
        "duration_bucket",
        F.when(F.col("duration_minutes") < 10, "under_10_minutes")
        .when(F.col("duration_minutes") < 20, "10_to_20_minutes")
        .when(F.col("duration_minutes") < 40, "20_to_40_minutes")
        .when(F.col("duration_minutes") < 60, "40_to_60_minutes")
        .otherwise("60_plus_minutes"),
    )
    write_summary(distance, "distance_bucket", f"{args.output.rstrip('/')}/tip_by_distance")
    write_summary(duration, "duration_bucket", f"{args.output.rstrip('/')}/tip_by_duration")
    spark.stop()


if __name__ == "__main__":
    main()
