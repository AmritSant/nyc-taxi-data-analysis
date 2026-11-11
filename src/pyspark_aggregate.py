"""Aggregate NYC Taxi Parquet files while tolerating numeric schema differences."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F


def first_existing(df: DataFrame, candidates: list[str]) -> str:
    """Return the first matching column name, accounting for schema variations."""
    available = {column.lower(): column for column in df.columns}
    for candidate in candidates:
        if candidate.lower() in available:
            return available[candidate.lower()]
    raise ValueError(f"None of these columns were found: {', '.join(candidates)}")


def normalized_trips(df: DataFrame) -> DataFrame:
    """Select a common, numeric representation across taxi-record schema versions."""
    distance = first_existing(df, ["trip_distance"])
    tip = first_existing(df, ["tip_amount"])
    fare = first_existing(df, ["fare_amount"])
    payment = first_existing(df, ["payment_type"])

    pickup = next((column for column in ["tpep_pickup_datetime", "lpep_pickup_datetime"] if column in df.columns), None)
    dropoff = next((column for column in ["tpep_dropoff_datetime", "lpep_dropoff_datetime"] if column in df.columns), None)
    if not pickup or not dropoff:
        raise ValueError("Pickup and dropoff timestamp columns were not found.")

    return (
        df.select(
            F.col(distance).cast("double").alias("trip_distance"),
            F.col(tip).cast("double").alias("tip_amount"),
            F.col(fare).cast("double").alias("fare_amount"),
            F.col(payment).cast("int").alias("payment_type"),
            F.col(pickup).cast("timestamp").alias("pickup_at"),
            F.col(dropoff).cast("timestamp").alias("dropoff_at"),
        )
        .where(F.col("trip_distance").isNotNull() & (F.col("trip_distance") >= 0))
        .where(F.col("fare_amount").isNotNull() & (F.col("fare_amount") > 0))
        .withColumn("duration_minutes", (F.unix_timestamp("dropoff_at") - F.unix_timestamp("pickup_at")) / 60)
        .where(F.col("duration_minutes") > 0)
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Parquet path or glob, local or gs:// URI")
    parser.add_argument("--output", required=True, help="Output directory, local or gs:// URI")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("nyc-taxi-aggregate").getOrCreate()
    trips = normalized_trips(spark.read.parquet(args.input))
    summary = trips.agg(
        F.count("*").alias("trip_count"),
        F.avg("trip_distance").alias("average_trip_distance"),
        F.avg("duration_minutes").alias("average_duration_minutes"),
        F.avg("tip_amount").alias("average_tip_amount"),
        F.avg(F.col("tip_amount") / F.col("fare_amount")).alias("average_tip_rate"),
    )
    summary.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
