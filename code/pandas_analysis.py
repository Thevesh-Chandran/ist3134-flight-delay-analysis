"""Single-machine Pandas implementation of the flight-delay analysis."""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from pathlib import Path

import pandas as pd

from analysis_common import (
    CAUSE_COLUMNS,
    NUMERIC_COLUMNS,
    PeakMemorySampler,
    ensure_output_directory,
    local_file_size,
    validate_columns,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument(
        "--minimum-route-flights",
        type=int,
        default=100,
        help="Minimum completed flights included in route ranking",
    )
    return parser.parse_args()


def delay_rate_table(
    completed: pd.DataFrame, group_columns: list[str]
) -> pd.DataFrame:
    table = (
        completed.groupby(group_columns, dropna=False, observed=True)
        .agg(
            completed_flights=("is_delayed", "size"),
            delayed_flights=("is_delayed", "sum"),
        )
        .reset_index()
    )
    table["delayed_flights"] = table["delayed_flights"].astype("int64")
    table["delay_rate_pct"] = (
        table["delayed_flights"] / table["completed_flights"] * 100
    )
    return table


def derive_departure_hour(dep_time: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(dep_time, errors="coerce")
    whole = numeric.round().astype("Int64")
    hour = (whole // 100).astype("Int64")
    minute = (whole % 100).astype("Int64")
    valid = ((hour.between(0, 23)) & (minute.between(0, 59))) | (whole == 2400)
    result = hour.copy()
    result.loc[~valid.fillna(False)] = pd.NA
    result.loc[(whole == 2400).fillna(False)] = 0
    return result


def build_cause_table(completed: pd.DataFrame) -> pd.DataFrame:
    total_all_causes = completed[list(CAUSE_COLUMNS)].clip(lower=0).sum().sum()
    rows = []
    for column, label in CAUSE_COLUMNS.items():
        values = completed[column].fillna(0).clip(lower=0)
        affected = values > 0
        total_minutes = float(values.sum())
        affected_flights = int(affected.sum())
        rows.append(
            {
                "cause_column": column,
                "cause": label,
                "total_delay_minutes": total_minutes,
                "affected_flights": affected_flights,
                "share_of_cause_minutes_pct": (
                    total_minutes / total_all_causes * 100
                    if total_all_causes
                    else 0.0
                ),
                "average_minutes_affected_flight": (
                    total_minutes / affected_flights if affected_flights else 0.0
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(
        "total_delay_minutes", ascending=False, ignore_index=True
    )


def analyse(
    input_path: str, output_path: str, minimum_route_flights: int
) -> dict[str, pd.DataFrame]:
    frame = pd.read_csv(input_path, low_memory=False)
    validate_columns(frame.columns)

    for column in NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["fl_date"] = pd.to_datetime(frame["fl_date"], errors="coerce")

    cancelled = frame["cancelled"].eq(1)
    diverted = frame["diverted"].eq(1)
    completed_mask = frame["cancelled"].eq(0) & frame["diverted"].eq(0)
    completed_missing_arrival = int(
        (completed_mask & frame["arr_delay"].isna()).sum()
    )
    if completed_missing_arrival:
        raise ValueError(
            f"{completed_missing_arrival} completed flights have no arr_delay."
        )

    completed = frame.loc[completed_mask].copy()
    completed["is_delayed"] = completed["arr_delay"].ge(15).astype("int8")
    completed["departure_hour"] = derive_departure_hour(completed["dep_time"])

    airline = delay_rate_table(completed, ["op_unique_carrier"]).sort_values(
        ["delay_rate_pct", "completed_flights"],
        ascending=[False, False],
        ignore_index=True,
    )
    origin = delay_rate_table(completed, ["origin"]).sort_values(
        ["delay_rate_pct", "completed_flights"],
        ascending=[False, False],
        ignore_index=True,
    )
    monthly = delay_rate_table(completed, ["month"]).sort_values(
        "month", ignore_index=True
    )
    weekday = delay_rate_table(completed, ["day_of_week"]).sort_values(
        "day_of_week", ignore_index=True
    )
    departure_hour = delay_rate_table(
        completed.dropna(subset=["departure_hour"]), ["departure_hour"]
    ).sort_values("departure_hour", ignore_index=True)

    routes = delay_rate_table(completed, ["origin", "dest"])
    average_delay = (
        completed.groupby(["origin", "dest"], observed=True)["arr_delay"]
        .mean()
        .rename("average_arrival_delay_minutes")
        .reset_index()
    )
    routes = routes.merge(average_delay, on=["origin", "dest"], validate="one_to_one")
    routes = routes.loc[routes["completed_flights"] >= minimum_route_flights]
    routes = routes.sort_values(
        ["average_arrival_delay_minutes", "completed_flights"],
        ascending=[False, False],
        ignore_index=True,
    )

    total_rows = len(frame)
    completed_rows = len(completed)
    delayed_rows = int(completed["is_delayed"].sum())
    data_quality = pd.DataFrame(
        [
            {
                "input_rows": total_rows,
                "input_columns": len(frame.columns),
                "input_bytes": local_file_size(input_path),
                "minimum_date": frame["fl_date"].min().date().isoformat(),
                "maximum_date": frame["fl_date"].max().date().isoformat(),
                "cancelled_flights": int(cancelled.sum()),
                "diverted_flights": int(diverted.sum()),
                "completed_flights": completed_rows,
                "delayed_completed_flights": delayed_rows,
                "overall_delay_rate_pct": delayed_rows / completed_rows * 100,
                "missing_arr_delay": int(frame["arr_delay"].isna().sum()),
                "completed_missing_arr_delay": completed_missing_arrival,
                "operating_carriers": int(frame["op_unique_carrier"].nunique()),
                "origin_airports": int(frame["origin"].nunique()),
                "destination_airports": int(frame["dest"].nunique()),
                "minimum_arr_delay": float(frame["arr_delay"].min()),
                "maximum_arr_delay": float(frame["arr_delay"].max()),
            }
        ]
    )

    return {
        "data_quality_summary": data_quality,
        "airline_delay_rates": airline,
        "origin_delay_rates": origin,
        "monthly_delay_rates": monthly,
        "day_of_week_delay_rates": weekday,
        "departure_hour_delay_rates": departure_hour,
        "recorded_delay_causes": build_cause_table(completed),
        "route_delay_statistics": routes,
    }


def main() -> None:
    args = parse_args()
    if args.minimum_route_flights < 1:
        raise ValueError("--minimum-route-flights must be at least 1.")

    output = ensure_output_directory(args.output)
    started = time.perf_counter()
    with PeakMemorySampler() as memory:
        tables = analyse(args.input, args.output, args.minimum_route_flights)
        for name, table in tables.items():
            table.to_csv(output / f"{name}.csv", index=False)
    elapsed = time.perf_counter() - started

    quality = tables["data_quality_summary"].iloc[0]
    benchmark = {
        "engine": "pandas",
        "status": "success",
        "input": args.input,
        "input_rows": int(quality["input_rows"]),
        "input_bytes": local_file_size(args.input),
        "analysed_rows": int(quality["completed_flights"]),
        "elapsed_seconds": elapsed,
        "peak_process_memory_bytes": memory.peak_bytes,
        "python_version": platform.python_version(),
        "pandas_version": pd.__version__,
        "minimum_route_flights": args.minimum_route_flights,
        "command": " ".join(sys.argv),
    }
    (output / "benchmark.json").write_text(
        json.dumps(benchmark, indent=2), encoding="utf-8"
    )
    print(json.dumps(benchmark, indent=2))


if __name__ == "__main__":
    main()
