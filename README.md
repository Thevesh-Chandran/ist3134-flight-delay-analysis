# Big Data Analysis of United States Domestic Flight Delays Using PySpark

IST3134 group assignment analysing more than seven million United States
domestic flight records from 2024.

## Research questions

1. Which airlines have the highest flight-delay rates?
2. Which origin airports have the highest delay rates?
3. During which months, days and departure hours are delays most common?
4. What are the major recorded causes of flight delays?
5. Which domestic routes have the longest average delays?
6. How does PySpark compare with Pandas when processing millions of records?

## Analytical definition

A completed flight has `cancelled = 0` and `diverted = 0`. A completed flight
is delayed when `arr_delay >= 15`.

```text
Delay rate (%) = delayed completed flights / completed flights * 100
```

## Dataset

- Source: [Flight Data 2024 on Kaggle](https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024)
- Full file: 7,079,081 records, 35 columns, 1,309,010,752 bytes
- Sample file: 10,000 records with the same schema

The dataset is not stored in GitHub. Download it from Kaggle and follow
[data/README.md](data/README.md).

## Project structure

```text
.
|-- aws/                   AWS deployment notes and scripts
|-- data/                  Local data locations (CSV files ignored by Git)
|-- docs/                  Project documentation
|-- outputs/               Report-guide documents
|-- reports/
|   |-- figures/           Final report charts
|   `-- tables/            Final report tables
|-- results/               Generated analytical outputs (ignored by Git)
|-- src/
|   |-- analysis_common.py Shared schema and definitions
|   |-- create_result_charts.py Report-ready figure generation
|   |-- pandas_analysis.py Single-machine implementation
|   |-- pyspark_analysis.py Distributed Spark implementation
|   `-- compare_outputs.py Cross-engine validation
`-- tests/                 Automated tests
```

## Local sample run

Install the Pandas dependencies:

```powershell
python -m pip install -r requirements-pandas.txt
```

Run the Pandas implementation:

```powershell
python src/pandas_analysis.py `
  --input "C:\Users\theve\Downloads\flight_data_2024_sample.csv" `
  --output "results\sample\pandas" `
  --minimum-route-flights 1
```

For PySpark, use Python 3.11 or 3.12 with Java installed:

```powershell
python -m pip install -r requirements-pyspark.txt
spark-submit src/pyspark_analysis.py `
  --input "C:\Users\theve\Downloads\flight_data_2024_sample.csv" `
  --output "results/sample/pyspark" `
  --minimum-route-flights 1
```

The threshold of 1 is used only to exercise the route logic on the small
sample. Use the report's threshold of 100 for every full-dataset run.

Validate the two output sets:

```powershell
python src/compare_outputs.py `
  --pandas-output "results\sample\pandas" `
  --spark-output "results\sample\pyspark"
```

Run the automated logic tests:

```powershell
python -m unittest discover -s tests -v
```

## Generated outputs

Both implementations produce equivalent results for:

- dataset and data-quality summary;
- airline and origin-airport delay rates;
- monthly, day-of-week and departure-hour delay rates;
- recorded delay-cause statistics;
- route statistics; and
- performance metadata.

The PySpark job writes each table as a Spark CSV directory. The Pandas job
writes one CSV file per table.

## Verified AWS execution

The full CSV was stored in Amazon S3. PySpark ran on Amazon EMR using one
`m5.xlarge` primary node and two `m5.xlarge` core nodes. Pandas ran on one
`r5.large` EC2 instance. Both implementations processed the same full file and
all eight analytical output tables matched.

| Engine | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| PySpark on EMR | 96.31 s | 98.07 s | 99.24 s | 98.07 s |
| Pandas on EC2 | 42.59 s | 41.06 s | 41.38 s | 41.38 s |

Pandas was faster for this 1.31 GB dataset, but used approximately 8.26 GB of
peak process memory and failed on the smaller `m5.large` configuration. The
complete execution evidence is recorded in
[docs/aws_execution_record.md](docs/aws_execution_record.md).

## Report figures and writing guide

After placing the downloaded AWS output folders under `results/aws/outputs`,
generate the six report figures with:

```powershell
python src/create_result_charts.py
```

The figures are saved under [reports/figures/results](reports/figures/results).
Copy-ready results, validation and comparison guidance is available in
[docs/results_and_comparison_guide.md](docs/results_and_comparison_guide.md).

AWS account identifiers, credentials, the full dataset and raw AWS logs must
never be committed.
