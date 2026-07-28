# Big Data Analysis of United States Domestic Flight Delays Using PySpark

IST3134 group assignment analysing more than seven million United States
domestic flight records from 2024.

## Dataset

[Flight Data 2024 on Kaggle](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024)

- 7,079,081 flight records
- 35 columns
- 1,309,010,752-byte CSV
- United States domestic flights from January to December 2024

## Implementations

- **Main big-data solution:** PySpark DataFrames and Spark SQL on Amazon EMR
- **Comparison solution:** Pandas on a single Amazon EC2 instance
- **Cloud storage:** Amazon S3

A completed flight has `cancelled = 0` and `diverted = 0`. A completed flight
is classified as delayed when `arr_delay >= 15`.

```text
Delay rate (%) = delayed completed flights / completed flights * 100
```

## Repository structure

```text
.
|-- code/
|   |-- analysis_common.py
|   |-- compare_outputs.py
|   |-- create_result_charts.py
|   |-- pandas_analysis.py
|   |-- pyspark_analysis.py
|   `-- test_pandas_analysis.py
|-- graphs/
|   |-- 01_airline_delay_rates.png
|   |-- 02_origin_airport_delay_rates.png
|   |-- 03_temporal_delay_patterns.png
|   |-- 04_recorded_delay_causes.png
|   |-- 05_longest_average_route_delays.png
|   `-- 06_runtime_comparison.png
|-- requirements-pandas.txt
|-- requirements-pyspark.txt
`-- README.md
```

## Run the Pandas implementation

```powershell
python -m pip install -r requirements-pandas.txt

python code/pandas_analysis.py `
  --input "<path-to-flight_data_2024.csv>" `
  --output "_local/results/pandas" `
  --minimum-route-flights 100
```

## Run the PySpark implementation

Use Python 3.11 or 3.12 with Java and Spark installed.

```powershell
python -m pip install -r requirements-pyspark.txt

spark-submit code/pyspark_analysis.py `
  --input "<path-or-s3-uri-to-flight_data_2024.csv>" `
  --output "<output-path-or-s3-prefix>" `
  --minimum-route-flights 100
```

## Validate equivalent outputs

```powershell
python code/compare_outputs.py `
  --pandas-output "<pandas-output-directory>" `
  --spark-output "<pyspark-output-directory>"
```

## Run the automated tests

```powershell
python -m unittest discover -s code -p "test_*.py" -v
```

## Verified full-data results

Both implementations processed the same full CSV and produced matching values
for all eight analytical output tables.

| Engine | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| PySpark on EMR | 96.31 s | 98.07 s | 99.24 s | 98.07 s |
| Pandas on EC2 | 42.59 s | 41.06 s | 41.38 s | 41.38 s |

The six final analytical charts are available in the [graphs](graphs) folder.
