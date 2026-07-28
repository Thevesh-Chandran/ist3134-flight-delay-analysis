"""Compare equivalent Pandas and PySpark CSV outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from analysis_common import OUTPUT_TABLES


KEY_COLUMNS = {
    "data_quality_summary": [],
    "airline_delay_rates": ["op_unique_carrier"],
    "origin_delay_rates": ["origin"],
    "monthly_delay_rates": ["month"],
    "day_of_week_delay_rates": ["day_of_week"],
    "departure_hour_delay_rates": ["departure_hour"],
    "recorded_delay_causes": ["cause_column"],
    "route_delay_statistics": ["origin", "dest"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pandas-output", required=True)
    parser.add_argument("--spark-output", required=True)
    parser.add_argument("--relative-tolerance", type=float, default=1e-9)
    parser.add_argument("--absolute-tolerance", type=float, default=1e-8)
    return parser.parse_args()


def read_pandas_table(root: Path, name: str) -> pd.DataFrame:
    return pd.read_csv(root / f"{name}.csv")


def read_spark_table(root: Path, name: str) -> pd.DataFrame:
    parts = sorted((root / name).glob("part-*.csv"))
    if not parts:
        raise FileNotFoundError(f"No Spark part CSV found for {name}.")
    return pd.concat((pd.read_csv(part) for part in parts), ignore_index=True)


def normalise(table: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if keys:
        table = table.sort_values(keys, kind="stable")
    return table.reset_index(drop=True).sort_index(axis=1)


def main() -> None:
    args = parse_args()
    pandas_root = Path(args.pandas_output)
    spark_root = Path(args.spark_output)
    results = []

    for name in OUTPUT_TABLES:
        left = normalise(read_pandas_table(pandas_root, name), KEY_COLUMNS[name])
        right = normalise(read_spark_table(spark_root, name), KEY_COLUMNS[name])
        common_columns = sorted(set(left.columns) & set(right.columns))
        left = left[common_columns]
        right = right[common_columns]
        try:
            assert_frame_equal(
                left,
                right,
                check_dtype=False,
                check_exact=False,
                rtol=args.relative_tolerance,
                atol=args.absolute_tolerance,
            )
            status = "match"
            detail = ""
        except AssertionError as error:
            status = "mismatch"
            detail = str(error)
        results.append(
            {
                "table": name,
                "pandas_rows": len(left),
                "spark_rows": len(right),
                "status": status,
                "detail": detail,
            }
        )

    print(json.dumps(results, indent=2))
    mismatches = [result for result in results if result["status"] != "match"]
    if mismatches:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

