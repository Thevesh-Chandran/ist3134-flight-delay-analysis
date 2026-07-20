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

## AWS target architecture

The full CSV will be stored in Amazon S3. PySpark will run on Amazon EMR and
write aggregate outputs back to S3. The comparison implementation will run
with Pandas on one Amazon EC2 instance using the same S3 input and analytical
definitions. AWS account identifiers and credentials must never be committed.
