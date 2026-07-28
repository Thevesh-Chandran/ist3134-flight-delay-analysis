"""Distributed PySpark implementation of the flight-delay analysis."""

from __future__ import annotations

import argparse
import platform
import sys
import time

from analysis_common import CAUSE_COLUMNS, EXPECTED_COLUMNS, validate_columns


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Local or s3:// input CSV")
    parser.add_argument("--output", required=True, help="Local or s3:// output prefix")
    parser.add_argument("--minimum-route-flights", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    from pyspark.sql import SparkSession, functions as F, types as T

    args = parse_args()
    if args.minimum_route_flights < 1:
        raise ValueError("--minimum-route-flights must be at least 1.")

    spark = SparkSession.builder.appName("IST3134FlightDelayAnalysis").getOrCreate()
    started = time.perf_counter()

    numeric_double = {
        "op_carrier_fl_num",
        "dep_time",
        "dep_delay",
        "taxi_out",
        "wheels_off",
        "wheels_on",
        "taxi_in",
        "arr_time",
        "arr_delay",
        "crs_elapsed_time",
        "actual_elapsed_time",
        "air_time",
        "distance",
    }
    numeric_integer = {
        "year",
        "month",
        "day_of_month",
        "day_of_week",
        "crs_dep_time",
        "crs_arr_time",
        "cancelled",
        "diverted",
        *CAUSE_COLUMNS.keys(),
    }
    fields = []
    for column in EXPECTED_COLUMNS:
        if column in numeric_double:
            data_type = T.DoubleType()
        elif column in numeric_integer:
            data_type = T.IntegerType()
        elif column == "fl_date":
            data_type = T.DateType()
        else:
            data_type = T.StringType()
        fields.append(T.StructField(column, data_type, nullable=True))
    schema = T.StructType(fields)

    raw = (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(args.input)
    )
    validate_columns(raw.columns)
    input_partitions = raw.rdd.getNumPartitions()
    raw = raw.cache()
    input_rows = raw.count()

    completed_condition = (F.col("cancelled") == 0) & (F.col("diverted") == 0)
    completed_missing_arrival = raw.filter(
        completed_condition & F.col("arr_delay").isNull()
    ).count()
    if completed_missing_arrival:
        raise ValueError(
            f"{completed_missing_arrival} completed flights have no arr_delay."
        )

    dep_whole = F.round(F.col("dep_time")).cast("int")
    dep_hour = F.floor(dep_whole / F.lit(100)).cast("int")
    dep_minute = F.pmod(dep_whole, F.lit(100))
    valid_dep_time = (
        dep_hour.between(0, 23) & dep_minute.between(0, 59)
    ) | (dep_whole == 2400)

    completed = (
        raw.filter(completed_condition)
        .withColumn(
            "is_delayed",
            F.when(F.col("arr_delay") >= 15, F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "departure_hour",
            F.when(dep_whole == 2400, F.lit(0))
            .when(valid_dep_time, dep_hour)
            .otherwise(F.lit(None).cast("int")),
        )
        .cache()
    )
    completed_rows = completed.count()
    delayed_rows = completed.agg(F.sum("is_delayed").alias("n")).first()["n"]

    def delay_rate_table(group_columns: list[str]):
        return (
            completed.groupBy(*group_columns)
            .agg(
                F.count(F.lit(1)).alias("completed_flights"),
                F.sum("is_delayed").alias("delayed_flights"),
            )
            .withColumn(
                "delay_rate_pct",
                F.col("delayed_flights") / F.col("completed_flights") * 100,
            )
        )

    airline = delay_rate_table(["op_unique_carrier"]).orderBy(
        F.desc("delay_rate_pct"), F.desc("completed_flights")
    )
    origin = delay_rate_table(["origin"]).orderBy(
        F.desc("delay_rate_pct"), F.desc("completed_flights")
    )
    monthly = delay_rate_table(["month"]).orderBy("month")
    weekday = delay_rate_table(["day_of_week"]).orderBy("day_of_week")
    departure = (
        delay_rate_table(["departure_hour"])
        .filter(F.col("departure_hour").isNotNull())
        .orderBy("departure_hour")
    )
    routes = (
        completed.groupBy("origin", "dest")
        .agg(
            F.count(F.lit(1)).alias("completed_flights"),
            F.sum("is_delayed").alias("delayed_flights"),
            F.avg("arr_delay").alias("average_arrival_delay_minutes"),
        )
        .withColumn(
            "delay_rate_pct",
            F.col("delayed_flights") / F.col("completed_flights") * 100,
        )
        .select(
            "origin",
            "dest",
            "completed_flights",
            "delayed_flights",
            "delay_rate_pct",
            "average_arrival_delay_minutes",
        )
        .filter(F.col("completed_flights") >= args.minimum_route_flights)
        .orderBy(
            F.desc("average_arrival_delay_minutes"), F.desc("completed_flights")
        )
    )

    total_cause_minutes = completed.agg(
        sum(
            (
                F.sum(F.greatest(F.coalesce(F.col(column), F.lit(0)), F.lit(0)))
                for column in CAUSE_COLUMNS
            ),
            F.lit(0),
        ).alias("total")
    ).first()["total"]
    cause_rows = []
    for column, label in CAUSE_COLUMNS.items():
        values = F.greatest(F.coalesce(F.col(column), F.lit(0)), F.lit(0))
        values_summary = completed.agg(
            F.sum(values).alias("minutes"),
            F.sum(F.when(values > 0, 1).otherwise(0)).alias("affected"),
        ).first()
        minutes = float(values_summary["minutes"] or 0)
        affected = int(values_summary["affected"] or 0)
        cause_rows.append(
            (
                column,
                label,
                minutes,
                affected,
                minutes / total_cause_minutes * 100 if total_cause_minutes else 0.0,
                minutes / affected if affected else 0.0,
            )
        )
    causes = spark.createDataFrame(
        cause_rows,
        [
            "cause_column",
            "cause",
            "total_delay_minutes",
            "affected_flights",
            "share_of_cause_minutes_pct",
            "average_minutes_affected_flight",
        ],
    ).orderBy(F.desc("total_delay_minutes"))

    quality_values = raw.agg(
        F.min("fl_date").alias("minimum_date"),
        F.max("fl_date").alias("maximum_date"),
        F.sum(F.when(F.col("cancelled") == 1, 1).otherwise(0)).alias(
            "cancelled_flights"
        ),
        F.sum(F.when(F.col("diverted") == 1, 1).otherwise(0)).alias(
            "diverted_flights"
        ),
        F.sum(F.when(F.col("arr_delay").isNull(), 1).otherwise(0)).alias(
            "missing_arr_delay"
        ),
        F.countDistinct("op_unique_carrier").alias("operating_carriers"),
        F.countDistinct("origin").alias("origin_airports"),
        F.countDistinct("dest").alias("destination_airports"),
        F.min("arr_delay").alias("minimum_arr_delay"),
        F.max("arr_delay").alias("maximum_arr_delay"),
    ).first()
    quality = spark.createDataFrame(
        [
            (
                input_rows,
                len(raw.columns),
                str(quality_values["minimum_date"]),
                str(quality_values["maximum_date"]),
                quality_values["cancelled_flights"],
                quality_values["diverted_flights"],
                completed_rows,
                delayed_rows,
                delayed_rows / completed_rows * 100,
                quality_values["missing_arr_delay"],
                completed_missing_arrival,
                quality_values["operating_carriers"],
                quality_values["origin_airports"],
                quality_values["destination_airports"],
                quality_values["minimum_arr_delay"],
                quality_values["maximum_arr_delay"],
            )
        ],
        [
            "input_rows",
            "input_columns",
            "minimum_date",
            "maximum_date",
            "cancelled_flights",
            "diverted_flights",
            "completed_flights",
            "delayed_completed_flights",
            "overall_delay_rate_pct",
            "missing_arr_delay",
            "completed_missing_arr_delay",
            "operating_carriers",
            "origin_airports",
            "destination_airports",
            "minimum_arr_delay",
            "maximum_arr_delay",
        ],
    )

    tables = {
        "data_quality_summary": quality,
        "airline_delay_rates": airline,
        "origin_delay_rates": origin,
        "monthly_delay_rates": monthly,
        "day_of_week_delay_rates": weekday,
        "departure_hour_delay_rates": departure,
        "recorded_delay_causes": causes,
        "route_delay_statistics": routes,
    }
    for name, table in tables.items():
        (
            table.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(f"{args.output.rstrip('/')}/{name}")
        )

    elapsed = time.perf_counter() - started
    benchmark = spark.createDataFrame(
        [
            (
                "pyspark",
                "success",
                args.input,
                input_rows,
                completed_rows,
                elapsed,
                input_partitions,
                spark.version,
                platform.python_version(),
                args.minimum_route_flights,
                " ".join(sys.argv),
            )
        ],
        [
            "engine",
            "status",
            "input",
            "input_rows",
            "analysed_rows",
            "elapsed_seconds",
            "input_partitions",
            "spark_version",
            "python_version",
            "minimum_route_flights",
            "command",
        ],
    )
    benchmark.coalesce(1).write.mode("overwrite").json(
        f"{args.output.rstrip('/')}/benchmark"
    )
    benchmark.show(truncate=False)

    completed.unpersist()
    raw.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

