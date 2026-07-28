# Results and Comparison Writing Guide

Use this guide when writing Chapters 6, 7 and 8. The numbers below come from the
full AWS runs. Do not replace them with the earlier local or 10,000-row sample
results.

## Chapter 6: Testing and Validation

### 6.1 Testing Approach

Explain that the programs were first checked with the 10,000-row sample before
the complete CSV was processed on AWS. The full PySpark analysis ran on Amazon
EMR, while Pandas ran on one Amazon EC2 instance. Both programs used the same
definitions, filters and output columns.

### 6.2 Data-Quality Validation

Report the following reconciliation:

| Measure | Verified result |
|---|---:|
| Total records | 7,079,081 |
| Cancelled flights | 96,315 |
| Diverted flights | 17,499 |
| Completed flights analysed | 6,965,267 |
| Delayed completed flights | 1,449,972 |
| Overall delay rate | 20.8172% |
| Missing arrival-delay values | 113,814 |
| Completed flights missing arrival delay | 0 |

The missing arrival-delay values belong to cancelled or diverted records. Valid
negative arrival delays were kept because they represent early arrivals.
Extreme delays were also retained instead of being silently removed.

### 6.3 Output Agreement

State that the comparison program returned exit status 0 and all eight full-data
tables matched between PySpark and Pandas:

| Output | Rows in each implementation |
|---|---:|
| Data-quality summary | 1 |
| Airline delay rates | 15 |
| Origin-airport delay rates | 348 |
| Monthly delay rates | 12 |
| Day-of-week delay rates | 7 |
| Departure-hour delay rates | 24 |
| Recorded delay causes | 5 |
| Route statistics | 5,483 |

This agreement supports the correctness of the cleaning rules, delay
classification and aggregations. It does not prove that the dataset itself is
free from reporting errors.

## Chapter 7: Results and Analysis

### 7.1 Overall Result

The complete dataset contained 7,079,081 records. After cancelled and diverted
flights were excluded, 6,965,267 completed flights remained. Of these,
1,449,972 were delayed by at least 15 minutes, giving an overall delay rate of
20.8172%.

### 7.2 Airline Delay Rates

F9 had the highest observed delay rate at 28.74%, followed by AA at 26.13% and
B6 at 25.51%. YX had the lowest rate at 14.07%. The gap between the highest and
lowest carrier was 14.67 percentage points.

Interpret the ranking carefully. It describes what appeared in the 2024 data,
but it does not account for differences in routes, airports, schedules or
operating conditions. Include completed-flight volume beside each rate.

**Suggested caption:** Figure 7.1. Arrival delay rate by operating carrier for
completed US domestic flights in 2024.

### 7.3 Origin-Airport Delay Rates

For the main chart, use airports with at least 10,000 completed flights. This
retains 101 airports and 91.8% of all completed flights. Within that group, MIA
had the highest delay rate at 27.83%, followed by DFW at 27.10% and CLT at
27.04%.

The minimum-volume rule prevents very small airports from dominating the
ranking. It must be stated in the figure subtitle, caption or nearby text.

**Suggested caption:** Figure 7.2. Origin airports with the highest arrival
delay rates among airports with at least 10,000 completed flights in 2024.

### 7.4 Temporal Delay Patterns

July had the highest monthly delay rate at 29.62%, while October had the lowest
at 13.20%. By day of week, Friday was highest at 23.33% and Tuesday was lowest
at 17.92%.

The departure-hour result should be discussed together with flight volume.
Overnight hours showed high rates but had far fewer completed flights than the
main daytime periods. Therefore, do not describe the overnight pattern as the
largest operational problem without mentioning its small denominator.

**Suggested caption:** Figure 7.3. Monthly, day-of-week and departure-hour
arrival delay patterns for completed US domestic flights in 2024.

### 7.5 Recorded Delay Causes

Late-aircraft delay represented 40.44% of all attributed cause minutes, followed
by carrier delay at 34.51%. National Aviation System delay contributed 18.90%,
weather 5.97% and security 0.17%.

These fields are recorded operational attributions. They show how delay minutes
were classified in the dataset, but they do not prove that one category caused
another observed pattern.

**Suggested caption:** Figure 7.4. Distribution of recorded arrival-delay cause
minutes and affected completed flights in 2024.

### 7.6 Routes With the Longest Average Delays

The main ranking includes only routes with at least 100 completed flights.
RDM–DFW had the highest average signed arrival delay at 87.43 minutes across 128
flights. EGE–MIA followed at 75.30 minutes across 128 flights, and RNO–JFK
averaged 73.79 minutes across 117 flights.

Even after the threshold, these routes have much lower volumes than major
routes. Report the average delay, delay rate and number of flights together.

**Suggested caption:** Figure 7.5. Domestic routes with the longest average
arrival delays among routes with at least 100 completed flights in 2024.

### 7.7 Combined Interpretation

The results show that delays were not evenly distributed across airlines,
airports, time periods or routes. High values also occurred at different flight
volumes, which means rankings based only on percentages or averages can be
misleading. The recorded-cause results add operational context, but the analysis
remains descriptive and should not be presented as proof of causation.

## Chapter 8: PySpark and Pandas Comparison

### 8.1 Experimental Setup

PySpark processed the full CSV from Amazon S3 on Amazon EMR using one
`m5.xlarge` primary node and two `m5.xlarge` core nodes. The cluster used EMR
7.13.0, Spark 3.5.6 and 10 input partitions. Pandas 2.3.3 ran on one `r5.large`
EC2 instance with 2 vCPUs, about 15 GiB usable memory and no swap.

Both implementations processed the same 1,309,010,752-byte CSV, applied the
same rules and produced the same analytical tables. Internal processing time
does not include cluster provisioning or file transfer.

### 8.2 Runtime Results

| Engine | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| PySpark on EMR | 96.31 s | 98.07 s | 99.24 s | 98.07 s |
| Pandas on EC2 | 42.59 s | 41.06 s | 41.38 s | 41.38 s |

Pandas used about 42% of the PySpark median processing time for this experiment.
The result should be reported honestly: PySpark was not faster on the current
1.31 GB dataset.

**Suggested caption:** Figure 8.1. Internal processing runtime for three
full-data PySpark and Pandas runs.

### 8.3 Memory and Scalability

Pandas used approximately 8.26 GB of peak process memory. The first full-data
attempt on an `m5.large` with 7.6 GiB usable memory was killed by the operating
system with exit status 137. The same program succeeded after moving to an
`r5.large`.

This failure is important evidence. Pandas was faster when the dataset fitted in
memory, but the single-machine approach had a clear memory limit. Spark adds
cluster and scheduling overhead, but its partitioned processing model can scale
across more nodes for much larger datasets.

### 8.4 Accuracy, Cost and Overall Evaluation

All eight analytical outputs matched between the two implementations. Estimated
EMR cluster cost was approximately USD 0.35. The successful and failed Pandas
EC2 experiments together were estimated at approximately USD 0.08–0.10. S3
storage for the initial project files was approximately USD 0.03 per month.

Conclude that Pandas was the more efficient choice for this specific dataset
when enough single-machine memory was available. PySpark remains the stronger
choice for future datasets that exceed one machine's memory, require distributed
processing or need Spark's recovery and scale-out features.

## Files to Use

- `reports/figures/results/01_airline_delay_rates.png`
- `reports/figures/results/02_origin_airport_delay_rates.png`
- `reports/figures/results/03_temporal_delay_patterns.png`
- `reports/figures/results/04_recorded_delay_causes.png`
- `reports/figures/results/05_longest_average_route_delays.png`
- `reports/figures/results/06_runtime_comparison.png`
- `docs/aws_execution_record.md`

Keep the complete AWS output archive and screenshots as private evidence. Do not
commit AWS account identifiers, credentials or the 1.31 GB dataset to GitHub.
