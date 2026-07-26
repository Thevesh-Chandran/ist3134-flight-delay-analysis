# AWS Execution Record

## Amazon S3

- Region: `us-east-1`
- Bucket: `ist3134-flight-delay-thevesh-2026`
- Full dataset object: `data/raw/flight_data_2024.csv`
- Full dataset size: 1,309,010,752 bytes
- Total initial project storage: 1,310,873,188 bytes (approximately 1.311 GB)
- Estimated S3 Standard storage cost: approximately USD 0.030 per month

## Amazon EMR Environment

- Cluster name: `ist3134-flight-delay-thevesh`
- EMR release: `emr-7.13.0`
- Hadoop version: 3.4.2
- Spark version: 3.5.6 (`3.5.6-amzn-2` reported by the job)
- Python version: 3.11.15
- Primary nodes: 1 x `m5.xlarge`
- Core nodes: 2 x `m5.xlarge`
- Task nodes: 0
- Purchasing option: On-Demand
- Spark input partitions for the full CSV: 10

## Cluster Timeline

- Created: 2026-07-26 10:11:16.691 UTC
- Ready: 2026-07-26 10:15:34.856 UTC
- Terminated: 2026-07-26 10:39:50.704 UTC
- Total cluster lifetime: 1,714.013 seconds (28 minutes 34.013 seconds)
- Ready-to-termination duration: 1,455.848 seconds
- Estimated EC2 and EMR compute cost: approximately USD 0.343
- Estimated overall cluster cost including small EBS and public IPv4 charges: approximately USD 0.35

## PySpark Sample Run

- Input rows: 10,000
- Completed and analysed rows: 9,836
- Input partitions: 1
- Internal application runtime: 27.7529 seconds
- EMR step runtime: 58.221 seconds
- Status: Success

## PySpark Full Runs

| Run | Internal runtime (seconds) | EMR step runtime (seconds) | Input rows | Analysed rows |
|---|---:|---:|---:|---:|
| 1 | 96.3095 | 118.142 | 7,079,081 | 6,965,267 |
| 2 | 98.0664 | 120.117 | 7,079,081 | 6,965,267 |
| 3 | 99.2402 | 120.083 | 7,079,081 | 6,965,267 |
| Median | 98.0664 | 120.083 | — | — |

## Verified Full-Data Quality Results

- Total records: 7,079,081
- Columns: 35
- Date range: 2024-01-01 to 2024-12-31
- Cancelled flights: 96,315
- Diverted flights: 17,499
- Completed flights: 6,965,267
- Delayed completed flights: 1,449,972
- Overall delay rate: 20.8171775755%
- Missing arrival delays: 113,814
- Completed flights missing arrival delay: 0
- Operating carriers: 15
- Origin airports: 348
- Destination airports: 348
- Minimum arrival delay: -126 minutes
- Maximum arrival delay: 3,803 minutes

Sensitive screenshots and identifiers are stored outside the Git repository.

## Pandas EC2 Launch Attempt

- Requested configuration: Amazon Linux 2023, `m5.xlarge`, 30 GiB gp3,
  `LabInstanceProfile`
- Result: Launch failed before an instance was created
- AWS response: `ec2:RunInstances` was explicitly denied by the AWS Academy
  Learner Lab identity policy
- Cost impact: No EC2 instance was created, so there was no instance runtime
  charge for this attempt
- Correction: The comparison was retried on a permitted `m5.large`. After that
  run exposed the memory limit, the final successful runs used an `r5.large`.

### Full Pandas Attempt on m5.large

- Instance: `m5.large`, 2 vCPU, 7.6 GiB usable memory, no swap
- Dataset: full 1,309,010,752-byte CSV
- Result: failed because the operating system killed the process with signal 9
- Shell exit status: 137
- Time before termination: 36.07 seconds
- Maximum resident set size: 7,715,168 KiB (approximately 7.36 GiB)
- Interpretation: the in-memory Pandas implementation exceeded the available
  memory of the largest initially permitted single-machine configuration

### Successful Full Pandas Runs on r5.large

- Instance: `r5.large`, 2 vCPU, 15 GiB usable memory, no swap
- Python version: 3.9.25
- Pandas version: 2.3.3
- Input rows per run: 7,079,081
- Analysed rows per run: 6,965,267

| Run | Label | Internal runtime (seconds) | Wall-clock time (seconds) | Peak process memory (bytes) |
|---|---|---:|---:|---:|
| 1 | Cold / first | 42.5927 | 43.46 | 8,279,236,608 |
| 2 | Warm | 41.0614 | 41.38 | 8,260,907,008 |
| 3 | Warm | 41.3813 | 41.70 | 8,261,115,904 |
| Median | — | 41.3813 | 41.70 | 8,261,115,904 |

### Full PySpark–Pandas Output Agreement

- Comparison exit status: 0
- Matching tables: 8 of 8
- Data quality rows: 1 in each implementation
- Airline rows: 15 in each implementation
- Origin rows: 348 in each implementation
- Month rows: 12 in each implementation
- Day-of-week rows: 7 in each implementation
- Departure-hour rows: 24 in each implementation
- Recorded-cause rows: 5 in each implementation
- Qualifying-route rows: 5,483 in each implementation

### EC2 Termination and Estimated Cost

- Final instance state: Terminated
- `r5.large` start time: 2026-07-26 13:12:26 UTC
- Termination evidence captured: 2026-07-26 13:30:08 UTC
- Maximum evidenced `r5.large` runtime: 17 minutes 42 seconds
- Estimated `r5.large` compute cost: approximately USD 0.037
- Estimated total Pandas EC2 experiment cost, including the earlier
  `m5.large` period and small EBS/public IPv4 charges: approximately
  USD 0.08–0.10
