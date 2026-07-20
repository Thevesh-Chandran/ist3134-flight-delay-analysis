from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from analysis_common import EXPECTED_COLUMNS
from pandas_analysis import analyse, derive_departure_hour


class PandasAnalysisTests(unittest.TestCase):
    def test_departure_hour_handles_midnight_and_invalid_values(self) -> None:
        values = pd.Series([5, 59, 100, 2359, 2400, 2360, None])
        actual = derive_departure_hour(values).tolist()
        self.assertEqual(actual[:5], [0, 0, 1, 23, 0])
        self.assertTrue(pd.isna(actual[5]))
        self.assertTrue(pd.isna(actual[6]))

    def test_delay_definition_and_completed_filter(self) -> None:
        rows = []
        for arr_delay, cancelled, diverted, dep_time in [
            (14, 0, 0, 930),
            (15, 0, 0, 2400),
            (30, 1, 0, None),
            (40, 0, 1, None),
        ]:
            row = {column: 0 for column in EXPECTED_COLUMNS}
            row.update(
                {
                    "year": 2024,
                    "month": 1,
                    "day_of_month": 1,
                    "day_of_week": 1,
                    "fl_date": "2024-01-01",
                    "op_unique_carrier": "ZZ",
                    "origin": "AAA",
                    "dest": "BBB",
                    "dep_time": dep_time,
                    "arr_delay": arr_delay,
                    "cancelled": cancelled,
                    "diverted": diverted,
                }
            )
            rows.append(row)

        with tempfile.TemporaryDirectory() as directory:
            tmp_path = Path(directory)
            input_csv = tmp_path / "fixture.csv"
            pd.DataFrame(rows, columns=EXPECTED_COLUMNS).to_csv(
                input_csv, index=False
            )
            tables = analyse(str(input_csv), str(tmp_path), minimum_route_flights=1)

        quality = tables["data_quality_summary"].iloc[0]
        airline = tables["airline_delay_rates"].iloc[0]
        self.assertEqual(quality["completed_flights"], 2)
        self.assertEqual(quality["delayed_completed_flights"], 1)
        self.assertEqual(quality["overall_delay_rate_pct"], 50)
        self.assertEqual(airline["completed_flights"], 2)
        self.assertEqual(airline["delay_rate_pct"], 50)


if __name__ == "__main__":
    unittest.main()
