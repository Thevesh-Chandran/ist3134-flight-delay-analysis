# Flight Data 2024: Verified Dataset Profile

Profiled locally from the downloaded files before analysis code was written.

## Files inspected

| File | Size | Rows | Columns |
|---|---:|---:|---:|
| `flight_data_2024.csv` | 1,309,010,752 bytes | 7,079,081 | 35 |
| `flight_data_2024_sample.csv` | 1,850,581 bytes | 10,000 | 35 |
| `flight_data_2024_data_dictionary.csv` | 1,055 bytes | 35 entries | 4 |

The full file and sample have identical headers. The data dictionary lists the
same 35 columns in the same order.

## Verified analytical population

```text
7,079,081 total records
- 96,315 cancelled records
- 17,499 diverted records
= 6,965,267 completed records
```

There are no records marked as both cancelled and diverted.

Using `arr_delay >= 15`, the full dataset contains 1,449,972 delayed completed
flights. The verified overall delay rate is 20.8171776%.

## Confirmed analytical fields

| Purpose | Physical column |
|---|---|
| Airline | `op_unique_carrier` |
| Origin airport | `origin` |
| Destination airport | `dest` |
| Flight date | `fl_date` |
| Month | `month` |
| Day of week | `day_of_week` |
| Departure time | `dep_time` |
| Arrival delay | `arr_delay` |
| Cancellation | `cancelled` |
| Diversion | `diverted` |
| Carrier-attributed delay | `carrier_delay` |
| Weather-attributed delay | `weather_delay` |
| NAS-attributed delay | `nas_delay` |
| Security-attributed delay | `security_delay` |
| Late-aircraft-attributed delay | `late_aircraft_delay` |

## Quality checks

| Check | Full-data result |
|---|---:|
| Missing `arr_delay` | 113,814 |
| Completed flights missing `arr_delay` | 0 |
| Missing `op_carrier_fl_num` | 1 |
| Missing `crs_elapsed_time` | 1 |
| Invalid month values | 0 |
| Invalid day-of-week values | 0 |
| Invalid dates | 0 |
| Invalid non-empty departure times | 0 |
| Negative distances | 0 |
| Minimum arrival delay | -126 minutes |
| Maximum arrival delay | 3,803 minutes |
| Operating carriers | 15 |
| Origin airports | 348 |
| Destination airports | 348 |
| Date range | 2024-01-01 to 2024-12-31 |

All 113,814 missing arrival-delay values are explained by cancellation or
diversion. Negative arrival delays are valid early arrivals and are preserved.
Extreme positive delays are retained and disclosed rather than silently
deleted.

## Sample verification

The 10,000-row sample contains:

- 122 cancelled flights;
- 42 diverted flights;
- 9,836 completed flights;
- 2,119 delayed completed flights; and
- a 21.5433103% overall delay rate.

The sample covers the full calendar year and is used for rapid logic testing.
It should not be used as the source of final report findings.

