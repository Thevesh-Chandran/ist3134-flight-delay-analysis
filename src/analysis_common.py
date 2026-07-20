"""Shared schema, definitions, and output names for both implementations."""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Iterable

import psutil


EXPECTED_COLUMNS = [
    "year",
    "month",
    "day_of_month",
    "day_of_week",
    "fl_date",
    "op_unique_carrier",
    "op_carrier_fl_num",
    "origin",
    "origin_city_name",
    "origin_state_nm",
    "dest",
    "dest_city_name",
    "dest_state_nm",
    "crs_dep_time",
    "dep_time",
    "dep_delay",
    "taxi_out",
    "wheels_off",
    "wheels_on",
    "taxi_in",
    "crs_arr_time",
    "arr_time",
    "arr_delay",
    "cancelled",
    "cancellation_code",
    "diverted",
    "crs_elapsed_time",
    "actual_elapsed_time",
    "air_time",
    "distance",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]

REQUIRED_ANALYTICAL_COLUMNS = [
    "month",
    "day_of_week",
    "op_unique_carrier",
    "origin",
    "dest",
    "dep_time",
    "arr_delay",
    "cancelled",
    "diverted",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]

NUMERIC_COLUMNS = [
    "year",
    "month",
    "day_of_month",
    "day_of_week",
    "op_carrier_fl_num",
    "crs_dep_time",
    "dep_time",
    "dep_delay",
    "taxi_out",
    "wheels_off",
    "wheels_on",
    "taxi_in",
    "crs_arr_time",
    "arr_time",
    "arr_delay",
    "cancelled",
    "diverted",
    "crs_elapsed_time",
    "actual_elapsed_time",
    "air_time",
    "distance",
    "carrier_delay",
    "weather_delay",
    "nas_delay",
    "security_delay",
    "late_aircraft_delay",
]

CAUSE_COLUMNS = {
    "carrier_delay": "Carrier",
    "weather_delay": "Weather",
    "nas_delay": "National Aviation System",
    "security_delay": "Security",
    "late_aircraft_delay": "Late aircraft",
}

OUTPUT_TABLES = [
    "data_quality_summary",
    "airline_delay_rates",
    "origin_delay_rates",
    "monthly_delay_rates",
    "day_of_week_delay_rates",
    "departure_hour_delay_rates",
    "recorded_delay_causes",
    "route_delay_statistics",
]


def validate_columns(actual_columns: Iterable[str]) -> None:
    """Reject missing, duplicate, or unexpected schema changes."""
    actual = list(actual_columns)
    missing = [column for column in EXPECTED_COLUMNS if column not in actual]
    unexpected = [column for column in actual if column not in EXPECTED_COLUMNS]
    duplicates = sorted({column for column in actual if actual.count(column) > 1})
    if missing or unexpected or duplicates:
        raise ValueError(
            "Incompatible input schema. "
            f"Missing={missing}; unexpected={unexpected}; duplicates={duplicates}"
        )
    if actual != EXPECTED_COLUMNS:
        raise ValueError("Input columns exist but are not in the confirmed order.")


def local_file_size(path: str) -> int | None:
    """Return a local input size; S3 and other URI sizes are recorded elsewhere."""
    if "://" in path:
        return None
    try:
        return os.path.getsize(path)
    except OSError:
        return None


class PeakMemorySampler:
    """Sample this process's RSS while a Pandas run is active."""

    def __init__(self, interval_seconds: float = 0.05) -> None:
        self.interval_seconds = interval_seconds
        self.peak_bytes = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._process = psutil.Process()

    def _sample(self) -> None:
        while not self._stop.is_set():
            rss = self._process.memory_info().rss
            self.peak_bytes = max(self.peak_bytes, rss)
            time.sleep(self.interval_seconds)

    def __enter__(self) -> "PeakMemorySampler":
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1)
        self.peak_bytes = max(self.peak_bytes, self._process.memory_info().rss)


def ensure_output_directory(path: str) -> Path:
    output = Path(path)
    output.mkdir(parents=True, exist_ok=True)
    return output

