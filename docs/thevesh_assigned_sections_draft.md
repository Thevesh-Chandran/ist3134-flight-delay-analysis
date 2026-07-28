# Thevesh’s Assigned Report Sections — Draft

This draft uses the verified full-data AWS outputs. Figure and table numbers can
be changed after all group sections are combined into the final report.

## 4.1 PySpark Implementation

The main big-data implementation was developed in Python using PySpark. The
complete CSV was stored in Amazon S3 and read by a Spark job running on Amazon
EMR. An explicit schema was supplied when the file was loaded instead of asking
Spark to infer every data type. This made the input more predictable and
reduced the risk of fields such as `arr_delay`, `cancelled` and `diverted` being
read as unsuitable types.

The program checked the complete 35-column header against the confirmed dataset
schema. It stopped with an error if a required field was missing, duplicated,
unexpected or in the wrong order. The CSV reader also used `FAILFAST` mode so a
malformed record would not be silently accepted. These checks were useful
because the sample file and complete file had to follow exactly the same
structure.

The analytical population was created by keeping records where `cancelled = 0`
and `diverted = 0`. The program then checked that no completed flight had a
missing arrival delay. Two derived fields were added. `is_delayed` was set to 1
when `arr_delay >= 15` and 0 otherwise. `departure_hour` was calculated from
`dep_time`, with 2400 treated as midnight and invalid times changed to null.
Negative arrival-delay values were retained because they represent early
arrivals.

The main filtering logic was:

```python
completed = (
    raw.filter((F.col("cancelled") == 0) & (F.col("diverted") == 0))
    .withColumn(
        "is_delayed",
        F.when(F.col("arr_delay") >= 15, 1).otherwise(0),
    )
    .cache()
)
```

Spark transformations are evaluated lazily. For example, the filter,
`withColumn` and `groupBy` statements define the processing plan, while actions
such as `count()` and writing output cause Spark to execute it. The raw and
completed DataFrames were cached because the same records were reused for the
airline, airport, temporal, cause and route analyses. The cache was materialised
with `count()` before the separate aggregations were run.

The analysis used group-and-aggregate operations, which follow the same general
pattern as MapReduce. The grouping field acts like the key, while counts, sums
and averages combine the records belonging to each key. For example, flights
were grouped by operating carrier to calculate completed flights, delayed
flights and delay rate. The same function was reused for origin airports,
months, days of week and departure hours. Routes were grouped by both `origin`
and `dest`, and only routes with at least 100 completed flights were retained
for the main ranking.

The measured AWS program used PySpark DataFrame operations. The airline
aggregation can also be expressed with Spark SQL after registering the cleaned
DataFrame as a temporary view:

```sql
SELECT
    op_unique_carrier,
    COUNT(*) AS completed_flights,
    SUM(is_delayed) AS delayed_flights,
    SUM(is_delayed) * 100.0 / COUNT(*) AS delay_rate_pct
FROM completed_flights
GROUP BY op_unique_carrier
ORDER BY delay_rate_pct DESC, completed_flights DESC;
```

This query is included to show the SQL form of the same group-and-aggregate
algorithm. It was not added to the measured runtime after the AWS experiment,
so the benchmark continues to represent the DataFrame implementation stored in
GitHub.

Each result was written back to a separate S3 prefix as a CSV output directory.
The job also wrote benchmark metadata containing the input rows, analysed rows,
elapsed time, Spark version, Python version and input partition count. The
complete source code is available in the project’s GitHub repository rather
than being reproduced in full in the report.

## 4.2 Pandas Implementation

Pandas was used as the single-machine comparison. The same full CSV stored in
S3 was downloaded to the EC2 instance and loaded using `pandas.read_csv`. It was
not processed in chunks. The whole DataFrame was loaded into memory because the
purpose of the comparison was to test a conventional in-memory approach against
distributed Spark.

The Pandas program used the same confirmed schema, completed-flight filter,
15-minute delay definition, departure-hour treatment and route threshold as the
PySpark program. Numeric fields were converted using `pandas.to_numeric`, while
the flight date was converted with `pandas.to_datetime`. The program also
stopped if a completed flight had no arrival-delay value.

Pandas `groupby` operations produced the same airline, airport, month, weekday,
departure-hour and route outputs. The program calculated group size as the
number of completed flights and summed `is_delayed` to obtain the delayed-flight
count. Delay rate was then calculated from the two values. The recorded-cause
table separately summed the five cause-minute columns and counted flights with
a positive value for each cause.

The output was written as one CSV file per analytical table. A JSON benchmark
file recorded the input size, number of rows, elapsed time, peak process memory
and software versions. The full in-memory run failed on an `m5.large` instance
because available memory was insufficient. It later completed successfully on
an `r5.large` instance with approximately 15 GiB of usable memory. This failure
was kept as part of the comparison rather than being removed from the report.

## 4.3 Testing and Validation

Testing was carried out in stages. The 10,000-row sample was used first because
it allowed the logic to be checked without repeatedly processing the full file.
The sample and full-data headers contained the same 35 fields in the same order,
and the required analysis fields resolved correctly.

The Pandas sample run read 10,000 rows and produced an analytical population of
9,836 completed flights. It identified 2,119 delayed completed flights and a
sample delay rate of 21.5433%. The sample was used only for testing and was not
used for the final findings.

Two manual calculations were also performed from the sample output. F9 had 84
delayed flights out of 288 completed flights:

```text
84 / 288 × 100 = 29.1667%
```

ATL had 96 delayed departures out of 487 completed flights:

```text
96 / 487 × 100 = 19.7125%
```

Both values matched the program output. Automated unit tests also checked the
15-minute boundary, exclusion of cancelled and diverted records, treatment of
2400 as midnight and rejection of invalid departure times.

After the sample checks, PySpark processed the complete file successfully on
EMR. The full Pandas and PySpark outputs were then compared table by table using
the project’s comparison script. All eight tables matched, including group
names, row counts and numerical values within the comparison tolerance.

Table 4.1 summarises the tests carried out on the sample and full dataset,
including the one failed memory test that was retained as evidence.

**Table 4.1: Summary of implementation and validation tests**

| Test | Expected result | Actual result | Status |
|---|---|---|---|
| Full and sample headers | Same 35 columns in the same order | Headers matched | Pass |
| Required-field validation | All required analytical fields resolve | All fields resolved | Pass |
| Pandas sample execution | Program completes without error | Exit status 0 | Pass |
| Sample row count | 10,000 input rows | 10,000 rows | Pass |
| Sample analytical population | 9,836 completed flights | 9,836 flights | Pass |
| Domain checks | Valid months, weekdays, dates, departure times and distances | No invalid values found | Pass |
| Delay-rate bounds | Every rate is between 0% and 100% | All rates within range | Pass |
| Manual airline check | F9 rate equals 84 / 288 × 100 | 29.1667% | Pass |
| Manual airport check | ATL rate equals 96 / 487 × 100 | 19.7125% | Pass |
| Sample output agreement | Eight Pandas and PySpark tables agree | All eight matched | Pass |
| Full PySpark execution | EMR step completes and writes outputs | Three full runs completed | Pass |
| Full output agreement | Eight full-data tables agree | All eight matched; comparison exit status 0 | Pass |
| Full Pandas run on `m5.large` | Complete if memory is sufficient | Killed with exit status 137 | Fail — documented |
| Full Pandas run on `r5.large` | Complete with output and benchmark files | Three successful runs | Pass |

The failed `m5.large` test was not a calculation error. It showed that the
in-memory Pandas method exceeded the available memory on that instance. This
became useful evidence for the scalability comparison in Chapter 6.

## 4.4 Implementation Evidence

The EMR cluster used the identifier `j-2M3N5DILDCV6Q`. The first full-data step
was `s-09486673NIIHTI0COSUC`, followed by
`s-08088801PSKG5HYAKUMY` and `s-0621580HOAVXWHI3WUN` for the second and third
runs. The step names were `flight-delay-full-run-1`,
`flight-delay-full-run-2` and `flight-delay-full-run-3`.

Table 4.2 records the AWS resources, software versions and output locations used
for the final executions. These details make the implementation easier to
repeat under a similar environment.

**Table 4.2: Recorded AWS and software configuration**

| Item | Recorded configuration |
|---|---|
| AWS region | `us-east-1` |
| EMR release | 7.13.0 |
| Hadoop version | 3.4.2 |
| Spark version | 3.5.6 (`3.5.6-amzn-2`) |
| EMR Python version | 3.11.15 |
| EMR nodes | 1 × `m5.xlarge` primary, 2 × `m5.xlarge` core |
| Spark input partitions | 10 |
| Pandas EC2 instance | 1 × `r5.large`, 2 vCPU, approximately 15 GiB usable memory |
| Pandas Python version | 3.9.25 |
| Pandas version | 2.3.3 |
| PySpark full output | `s3://ist3134-flight-delay-thevesh-2026/outputs/pyspark/full/` |
| Pandas full output | `s3://ist3134-flight-delay-thevesh-2026/outputs/pandas/full/` |
| GitHub repository | `https://github.com/Thevesh-Chandran/ist3134-flight-delay-analysis` |

The application included error handling for missing command-line arguments,
route thresholds below one, incompatible schemas, malformed CSV records and
completed flights with missing arrival delay. S3 permission or path failures
were reported by the AWS job rather than hidden. Spark outputs were written in
overwrite mode so a rerun would not mix new part files with earlier output.

The main implementation evidence is shown in Appendix D. It includes the S3
folder structure, full-data object, EMR configuration, successful sample and
full steps, three benchmark results, Pandas memory failure, successful
`r5.large` runs, output agreement and termination evidence. Account identifiers
should be cropped or covered before the screenshots are placed in the final
report.

# Chapter 5: Results and Discussion

## 5.1 Overall Flight-Delay Summary

The full dataset contained 7,079,081 scheduled domestic flight records. A total
of 96,315 records were cancelled and 17,499 were diverted. These records were
excluded from the arrival-delay denominator because they did not represent
ordinary completed arrivals. This left 6,965,267 completed flights for the main
analysis.

Table 5.1 shows how the raw dataset was reduced to the completed-flight
population used for the analysis. It also presents the number and percentage of
completed flights classified as delayed.

**Table 5.1: Overall flight-record and delay summary**

| Measure | Count |
|---|---:|
| Total records | 7,079,081 |
| Cancelled flights | 96,315 |
| Diverted flights | 17,499 |
| Completed flights analysed | 6,965,267 |
| Delayed completed flights | 1,449,972 |
| Completed flights delayed | 20.8172% |

Among the completed flights, 1,449,972 arrived at least 15 minutes late. The
overall delay rate was therefore:

```text
1,449,972 / 6,965,267 × 100 = 20.8172%
```

This means that roughly one in five completed flights met the project’s delay
definition. The percentage gives an overall view but does not show how delay
patterns differed by airline, airport, time or route. Those differences are
examined in the following sections.

The full reconciliation also supported the cleaning method. There were 113,814
missing arrival-delay values, but none belonged to a completed flight. Negative
arrival delays were preserved as early arrivals, while the largest positive
delay of 3,803 minutes was retained as an extreme but valid recorded value.

## 5.2 Airline and Airport Results

### Airline Results

Delay rates varied noticeably across the 15 operating carriers. F9 had the
highest rate at 28.74%, followed by AA at 26.13% and B6 at 25.51%. At the lower
end, YX recorded 14.07%, HA 15.45% and 9E 15.83%. The difference between F9 and
YX was 14.67 percentage points.

Table 5.2 lists selected carriers from the highest and lower ends of the
ranking, together with the flight counts behind each rate.

**Table 5.2: Selected operating carriers ranked by delay rate**

| Carrier | Completed flights | Delayed flights | Delay rate |
|---|---:|---:|---:|
| F9 | 203,482 | 58,481 | 28.74% |
| AA | 966,116 | 252,485 | 26.13% |
| B6 | 235,683 | 60,121 | 25.51% |
| NK | 255,633 | 61,163 | 23.93% |
| AS | 240,323 | 53,044 | 22.07% |
| 9E | 195,072 | 30,882 | 15.83% |
| HA | 77,633 | 11,998 | 15.45% |
| YX | 295,306 | 41,554 | 14.07% |

Figure 5.1 shows the delay rates for all 15 operating carriers. The chart makes
the difference between the highest- and lowest-ranked carriers easier to see,
while Table 5.2 provides the exact counts for selected carriers.

**Insert Figure 5.1 here**

**Figure 5.1: Arrival delay rate by operating carrier.**

Flight volume helps explain why the percentages should not be read alone. For
example, F9 had the highest rate but 203,482 completed flights, whereas AA had a
slightly lower rate across 966,116 flights. WN had the largest completed-flight
volume at approximately 1.40 million and a delay rate of 20.60%, which was close
to the overall rate.

The ranking shows association rather than airline quality by itself. Carriers
serve different networks and may use airports, routes and schedules with
different operating conditions. A lower rate does not prove that every part of
one airline’s operation performed better.

### Airport Results

Ranking every airport without a volume rule placed very small airports at the
top because one or two delayed flights could produce a rate of 100%. For the
main report, origin airports were therefore required to have at least 10,000
completed flights. This threshold retained 101 airports and 91.8% of all
completed flights.

Table 5.3 presents the ten highest delay rates after applying the
10,000-completed-flight threshold.

**Table 5.3: Origin airports with the highest delay rates among airports with
at least 10,000 completed flights**

| Origin airport | Completed flights | Delayed flights | Delay rate |
|---|---:|---:|---:|
| MIA | 107,722 | 29,981 | 27.83% |
| DFW | 306,872 | 83,172 | 27.10% |
| CLT | 214,096 | 57,894 | 27.04% |
| FLL | 90,121 | 24,093 | 26.73% |
| SJU | 34,804 | 8,972 | 25.78% |
| BWI | 97,251 | 23,898 | 24.57% |
| MCO | 156,258 | 38,146 | 24.41% |
| IAH | 113,248 | 27,628 | 24.40% |
| ORD | 275,505 | 65,275 | 23.69% |
| AVL | 11,654 | 2,758 | 23.67% |

Figure 5.2 displays the same airport ranking visually. It shows that the top
four qualifying airports had relatively similar delay rates, although their
flight volumes differed considerably.

**Insert Figure 5.2 here**

**Figure 5.2: Origin airports with the highest delay rates among airports with
at least 10,000 completed flights.**

MIA had the highest delay rate within the selected high-volume group at 27.83%.
DFW and CLT followed closely. DFW handled almost three times as many completed
flights as MIA, so its 27.10% rate represents a larger number of delayed flights.
This again shows why both rate and volume are needed.

Airport rankings are also descriptive. The dataset does not contain all factors
that may explain the pattern, such as runway capacity, weather conditions,
connecting traffic or temporary disruption. These results identify where high
rates were recorded, not the cause of those rates.

## 5.3 Temporal Delay Patterns

### Monthly Patterns

July had the highest monthly delay rate at 29.62%. May was second at 26.41% and
June followed at 24.94%. October had the lowest rate at 13.20%, followed by
November at 14.67% and September at 15.51%. The result shows a clear change
across the year, with a high period around May to August and lower rates during
September to November.

The dataset alone cannot prove why July was highest. Seasonal demand, weather
and congestion may be relevant, but those explanations require external data.
The result should therefore be described as a temporal pattern rather than a
causal finding.

### Day-of-Week Patterns

Friday had the highest day-of-week delay rate at 23.33%, followed by Sunday at
22.55% and Thursday at 21.88%. Tuesday recorded the lowest rate at 17.92%, while
Wednesday was 18.67%. Saturday’s rate was 20.41%, slightly below the overall
rate, so the data did not show a simple difference where every weekend day was
worse than every weekday.

### Departure-Hour Patterns

The highest rates were recorded at 01:00 and 02:00, at 66.52% and 66.12%
respectively. However, those hours contained only 11,816 and 3,873 completed
flights. The lowest rate was at 04:00, at 3.83%, followed by 05:00 at 4.13%.
Rates then generally increased through the day and reached 50.83% at 23:00.

The hourly pattern must be interpreted with flight volume. The early overnight
periods had much smaller denominators than the main daytime hours, which often
contained more than 400,000 flights. A high overnight rate is therefore not
equivalent to the largest number of affected passengers or flights.

Figure 5.3 brings the three time-based analyses together. The monthly panel
shows the mid-year increase, the weekday panel shows the relatively smaller
variation across the week, and the hourly panel shows how delay rates generally
rose later in the day. The hourly values should still be read alongside flight
volume because overnight hours contained fewer flights.

**Insert Figure 5.3 here**

**Figure 5.3: Delay rates by month, day of week and departure hour.**

## 5.4 Recorded Delay Causes

The five delay-cause columns contained approximately 103.80 million attributed
minutes in total. Late-aircraft delay contributed the largest share, with
41,974,007 minutes or 40.44%. Carrier delay was second with 35,823,265 minutes
or 34.51%. Together, these two categories represented 74.95% of all recorded
cause minutes.

Table 5.4 compares the five recorded causes using total minutes, affected
flights, percentage share and average minutes. Figure 5.4 then shows the two
main measures visually.

**Table 5.4: Recorded delay-cause statistics**

| Recorded cause | Total minutes | Affected flights | Share of cause minutes | Average minutes per affected flight |
|---|---:|---:|---:|---:|
| Late aircraft | 41,974,007 | 743,215 | 40.44% | 56.48 |
| Carrier | 35,823,265 | 788,965 | 34.51% | 45.41 |
| National Aviation System | 19,621,994 | 725,825 | 18.90% | 27.03 |
| Weather | 6,195,873 | 88,905 | 5.97% | 69.69 |
| Security | 179,928 | 7,406 | 0.17% | 24.29 |

Figure 5.4 shows that late-aircraft and carrier delay dominated the share of
recorded cause minutes. It also highlights that the cause with the most minutes
was not the same as the cause affecting the most flights.

**Insert Figure 5.4 here**

**Figure 5.4: Share of attributed cause minutes and number of affected
completed flights.**

Carrier delay affected the largest number of flights, while late-aircraft delay
produced the largest total number of minutes. Weather affected fewer flights,
but its average among affected flights was the highest at 69.69 minutes. These
different measures should not be combined into one simple ranking.

The columns represent the recorded operational attribution of delay minutes.
They are useful for describing how delays were classified, but they do not
prove causality. For example, the table cannot show whether an earlier weather
event later contributed to a late-aircraft entry without additional operational
data.

## 5.5 Routes With the Longest Average Delays

The route analysis grouped completed flights by origin and destination and
calculated flight count, delay rate and average signed arrival delay. Only
routes with at least 100 completed flights were included in the main ranking.
This removed extremely small routes whose averages could be dominated by a few
records.

Table 5.5 reports the top ten qualifying routes and keeps average delay, delay
rate and flight volume together so that the ranking is not judged from one
measure alone.

**Table 5.5: Routes with the longest average arrival delays, with at least 100
completed flights**

| Origin | Destination | Completed flights | Average arrival delay | Delay rate |
|---|---|---:|---:|---:|
| RDM | DFW | 128 | 87.43 minutes | 49.22% |
| EGE | MIA | 128 | 75.30 minutes | 54.69% |
| RNO | JFK | 117 | 73.79 minutes | 37.61% |
| DFW | RDM | 129 | 62.96 minutes | 42.64% |
| IDA | LAS | 109 | 59.61 minutes | 36.70% |
| SNA | MIA | 189 | 57.31 minutes | 26.98% |
| JAC | DFW | 608 | 56.27 minutes | 31.41% |
| EGE | JFK | 102 | 56.23 minutes | 41.18% |
| ANC | ATL | 105 | 56.11 minutes | 60.00% |
| MSO | LAS | 116 | 55.47 minutes | 31.90% |

Figure 5.5 visualises the average delay for the same ten routes. The figure
makes the unusually high RDM–DFW value clear, while Table 5.5 supplies the
volume and delay-rate context needed to interpret it.

**Insert Figure 5.5 here**

**Figure 5.5: Domestic routes with the longest average arrival delays among
routes with at least 100 completed flights.**

RDM–DFW had the highest average arrival delay at 87.43 minutes. The reverse
route, DFW–RDM, also appeared in the top four at 62.96 minutes. EGE–MIA had the
second-highest average and a delay rate of 54.69%. ANC–ATL had the highest delay
rate in the displayed top ten at 60.00%, although its average delay was lower
than the first eight routes.

The threshold improves the stability of the result but does not make all routes
equally comparable. Several routes in the table have close to 100 flights,
while JAC–DFW has 608. Average delay, rate and volume should therefore be
presented together.

# Chapter 6: PySpark and Pandas Comparison

## 6.1 Performance Results

Both programs processed the same 1,309,010,752-byte CSV containing 7,079,081
records. They applied the same completed-flight filter, delay definition,
derived departure hour and 100-flight route threshold. Internal runtime began
inside the analysis program and excluded EMR cluster creation, EC2 launch and
S3 file transfer.

PySpark ran on EMR 7.13.0 with Spark 3.5.6 and Python 3.11.15. The cluster had
one `m5.xlarge` primary node and two `m5.xlarge` core nodes. Pandas 2.3.3 and
Python 3.9.25 ran on a single `r5.large` EC2 instance with 2 vCPUs,
approximately 15 GiB usable memory and no swap.

Table 6.1 reports all three internal runtime measurements and their medians.
Figure 6.1 shows the same run-by-run comparison visually.

**Table 6.1: Full-data PySpark and Pandas performance results**

| Engine | Run 1 | Run 2 | Run 3 | Median | Peak memory |
|---|---:|---:|---:|---:|---:|
| PySpark on EMR | 96.31 s | 98.07 s | 99.24 s | 98.07 s | Not measured as one process; distributed across the cluster |
| Pandas on EC2 | 42.59 s | 41.06 s | 41.38 s | 41.38 s | Approximately 8.26 GB |

Figure 6.1 shows that Pandas was faster in each of the three measured runs on
this dataset. The chart only compares internal processing time; it does not
include cluster creation, instance launch or S3 transfer time.

**Insert Figure 6.1 here**

**Figure 6.1: Internal processing runtime for three full-data PySpark and
Pandas runs.**

Pandas completed the current workload faster. Its median runtime was 41.38
seconds compared with 98.07 seconds for PySpark. Pandas used about 42% of the
PySpark time, or was approximately 2.37 times faster based on the two medians.
This result is reasonable for a 1.31 GB file that fits into the memory of one
suitable machine because Spark has scheduling, serialisation and distributed
coordination overhead.

The first Pandas full-data attempt used an `m5.large` with 7.6 GiB usable
memory. The operating system killed the process after 36.07 seconds with exit
status 137, and maximum resident memory had reached about 7.36 GiB. The
successful `r5.large` runs therefore do not remove the observed memory
limitation; they show that the same program needed a larger-memory instance.

## 6.2 Accuracy and Scalability

Accuracy was checked by comparing every analytical output. All eight tables
matched between PySpark and Pandas. The comparison included one quality-summary
row, 15 airline rows, 348 origin-airport rows, 12 month rows, seven weekday
rows, 24 departure-hour rows, five cause rows and 5,483 qualifying route rows.
The comparison script returned exit status 0. No meaningful count or rate
difference was found.

The two implementations reached the same answer through different processing
models. Pandas loaded the full file into one machine’s memory and then used
direct `groupby` operations. This approach was simple to write, debug and run.
It worked well once the instance had enough memory.

Spark divided the input into 10 partitions and distributed work across the EMR
cluster. Its main advantage was not speed on this particular file. Its advantage
is that partitions can be processed across additional workers when the input is
too large for one machine. Spark and YARN can also retry failed tasks, while a
single Pandas process normally fails as one unit if it runs out of memory.

Spark’s distributed design introduces overhead. A cluster must be created,
tasks must be scheduled and data may need to move between executors during
grouping. For a dataset of only 1.31 GB, this overhead was greater than the
benefit of parallel execution. The Pandas memory failure nevertheless showed
that faster single-machine execution depends on having enough RAM.

## 6.3 Cost and Overall Evaluation

The EMR cluster existed for approximately 28 minutes and 34 seconds. Its
estimated combined EMR and EC2 cost was about USD 0.35, including a small
allowance for EBS and public IPv4 use. The Pandas EC2 work was estimated at
approximately USD 0.08–0.10, including the failed `m5.large` period, the
successful `r5.large` period and small supporting charges. Initial S3 storage
was approximately USD 0.03 per month.

Table 6.2 summarises the estimated component costs and the change observed in
the AWS Academy Learner Lab credit. Before the project’s main AWS processing
work, the dashboard showed USD 21.00 used. When it was checked again on 28 July
2026, it showed USD 21.80 used. The observed increase was therefore USD 0.80.

**Table 6.2: Estimated AWS service costs and observed Learner Lab credit usage**

| Cost item | Basis | Approximate cost |
|---|---|---:|
| S3 storage | Approximately 1.31 GB of raw data plus small scripts and outputs | USD 0.03 per month |
| EMR and supporting EC2 resources | One `m5.xlarge` primary and two `m5.xlarge` core nodes for approximately 28 minutes 34 seconds | USD 0.35 |
| Pandas EC2 testing | Failed `m5.large` attempt and successful `r5.large` testing | USD 0.08–0.10 |
| Calculated service estimate | Sum of the recorded project components above | USD 0.46–0.48 |
| Learner Lab usage before main project processing | Dashboard credit display before the recorded AWS executions | USD 21.00 used |
| Learner Lab usage after project processing | Dashboard credit display checked on 28 July 2026 | USD 21.80 used |
| Observed Learner Lab increase | USD 21.80 minus USD 21.00 | **USD 0.80** |

The calculated service estimate and observed credit usage should not be
expected to match exactly. The Learner Lab display can include cluster startup
and idle time, EBS volumes, public IPv4 addresses, logs, earlier classroom
resources and charges that appear after a delay. It is also a credit summary
rather than a detailed AWS invoice. For this reason, USD 0.80 is reported as an
approximate observed total, while the individual rows remain configuration-
based estimates.

For small datasets, Pandas is the more practical choice because it has less
setup and lower overhead. For the current 1.31 GB dataset, Pandas was also the
better choice for runtime and estimated cost once an `r5.large` with enough
memory was used. However, the failed `m5.large` test shows that the margin was
not large.

For much larger datasets, PySpark is more suitable because processing and
memory can be spread across multiple nodes. It is also more appropriate for
repeated production processing when fault tolerance, managed cluster resources
and scale-out capacity are more important than the extra setup. The comparison
therefore does not support a general claim that one tool is always faster or
better. The preferred tool depends on data size, available memory, processing
frequency and operational requirements.

# Chapter 7: Conclusion and Reflection

## 7.2 Thevesh’s Individual Reflection

My main responsibility in this assignment was the technical implementation and
the explanation of the results. I prepared the shared analysis rules, developed
the PySpark and Pandas programs, tested the sample data, set up the S3 folder
structure, ran the EMR jobs and completed the EC2 comparison. I also checked
that both implementations produced the same outputs and prepared the charts and
benchmark evidence for the report.

One of the main challenges was getting the Pandas comparison to complete on the
full dataset. The first run on an `m5.large` was killed because the program used
almost all the available memory. At first this looked like a failed experiment,
but it became an important part of the comparison. I checked the exit status and
memory output, recorded the failure, and repeated the test on an `r5.large`.
The larger-memory instance completed all three runs successfully.

I also had to be careful when preparing the PySpark job for EMR. The analysis
used a shared Python module, so the additional library file had to be packaged
and passed to `spark-submit` correctly. The job then had to read from the exact
S3 object and write each output to a separate prefix. Testing the sample first
helped me find command and environment problems before paying for full-data
cluster time.

The most useful thing I learned was that big-data tools should not be judged
only by which one finishes first. Pandas was faster for this dataset, but it
failed when the available memory was slightly below what the program needed.
Spark was slower because of its distributed overhead, yet it did not depend on
the memory of one process in the same way. This made the idea of scalability
more practical to me instead of only something discussed in lectures.

The result validation was another important lesson. Producing a chart was not
enough to show that the calculation was correct. I compared eight output tables
from the two engines, checked manual airline and airport calculations, and
kept the failed run as evidence. This gave me more confidence when explaining
the overall 20.8172% delay rate and the airline, airport, time, cause and route
patterns.

For teamwork, the assignment was divided so that I could focus on coding, AWS
execution and result-related writing while Angel worked on the other report
sections. We still needed to use the same definitions and report structure.
Sharing the verified counts and figures helped prevent the introduction and
results sections from describing different versions of the analysis.

If I repeated the project, I would automate more of the AWS setup and evidence
collection. I would also convert the raw CSV to partitioned Parquet before
benchmarking. That would reduce repeated parsing and allow a more useful test of
Spark’s strengths. A future version could compare several dataset sizes instead
of only one full file, which would make it easier to identify the point where
distributed processing becomes more efficient than a single machine.

# Appendices

## Appendix B: Important Execution Commands

Only the main execution and validation commands are included here. The complete
PySpark and Pandas source code, testing scripts and detailed execution
instructions are available in the project’s GitHub repository.

### B.1 PySpark Full-Data Run on EMR

```bash
export AWS_REGION="us-east-1"
export BUCKET="ist3134-flight-delay-thevesh-2026"
export CLUSTER_ID="<EMR_CLUSTER_ID>"

aws emr add-steps \
  --cluster-id "$CLUSTER_ID" \
  --region "$AWS_REGION" \
  --steps "Type=Spark,Name=flight-delay-full-run-1,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--master,yarn,--py-files,s3://$BUCKET/scripts/flight_analysis_lib.zip,s3://$BUCKET/scripts/pyspark_analysis.py,--input,s3://$BUCKET/data/raw/flight_data_2024.csv,--output,s3://$BUCKET/outputs/pyspark/full/run-1,--minimum-route-flights,100]"
```

### B.2 Pandas Full-Data Run With Time and Memory Measurement

```bash
mkdir -p results/full/pandas/run-1
set -o pipefail

/usr/bin/time -v python src/pandas_analysis.py \
  --input ~/flight-data/flight_data_2024.csv \
  --output results/full/pandas/run-1 \
  --minimum-route-flights 100 \
  2>&1 | tee results/full/pandas/run-1-terminal.txt

echo "EXIT_STATUS=${PIPESTATUS[0]}"
```

### B.3 Compare Pandas and PySpark Outputs

```bash
python src/compare_outputs.py \
  --pandas-output results/full/pandas/run-1 \
  --spark-output results/full/pyspark/run-1
```

### B.4 GitHub Repository

The full source code is available at:
[IST3134 Flight Delay Analysis](https://github.com/Thevesh-Chandran/ist3134-flight-delay-analysis).

## Appendix C: Full Benchmark and Test Results

### C.1 PySpark Benchmarks

Table C.1 provides the detailed internal and EMR step runtimes behind the
PySpark median reported in Table 6.1.

**Table C.1: Detailed PySpark benchmark results**

| Run | Internal runtime | EMR step runtime | Input rows | Analysed rows | Status |
|---|---:|---:|---:|---:|---|
| Sample run | 27.7529 s | 58.221 s | 10,000 | 9,836 | Success |
| Full run 1 | 96.3095 s | 118.142 s | 7,079,081 | 6,965,267 | Success |
| Full run 2 | 98.0664 s | 120.117 s | 7,079,081 | 6,965,267 | Success |
| Full run 3 | 99.2402 s | 120.083 s | 7,079,081 | 6,965,267 | Success |
| Full median | 98.0664 s | 120.083 s | — | — | — |

### C.2 Pandas Benchmarks

Table C.2 records the Pandas sample test, the failed `m5.large` attempt and the
three successful `r5.large` runs. These results support the runtime and memory
discussion in Sections 6.1 and 6.2.

**Table C.2: Detailed Pandas benchmark results**

| Run | Internal runtime | Wall-clock runtime | Peak process memory | Status |
|---|---:|---:|---:|---|
| Sample run | 0.1609 s | 0.50 s | 96,358,400 bytes | Success |
| `m5.large` full attempt | Not completed | 36.07 s before termination | 7,715,168 KiB maximum RSS | Failed, exit 137 |
| `r5.large` full run 1 | 42.5927 s | 43.46 s | 8,279,236,608 bytes | Success |
| `r5.large` full run 2 | 41.0614 s | 41.38 s | 8,260,907,008 bytes | Success |
| `r5.large` full run 3 | 41.3813 s | 41.70 s | 8,261,115,904 bytes | Success |
| Full median | 41.3813 s | 41.70 s | 8,261,115,904 bytes | — |

### C.3 Full Output Agreement

Table C.3 shows the row-count agreement for every output table generated by the
two implementations. All eight comparisons returned a match.

**Table C.3: Full-data output agreement between Pandas and PySpark**

| Output table | Pandas rows | PySpark rows | Result |
|---|---:|---:|---|
| Data-quality summary | 1 | 1 | Match |
| Airline delay rates | 15 | 15 | Match |
| Origin-airport delay rates | 348 | 348 | Match |
| Monthly delay rates | 12 | 12 | Match |
| Day-of-week delay rates | 7 | 7 | Match |
| Departure-hour delay rates | 24 | 24 | Match |
| Recorded delay causes | 5 | 5 | Match |
| Route delay statistics | 5,483 | 5,483 | Match |

Comparison exit status: 0. Matching tables: 8 of 8.

### C.4 Full-Data Quality Checks

Table C.4 lists the main data-quality checks performed on the full dataset. The
results confirm that no completed flight was missing its arrival-delay value.

**Table C.4: Full-data quality-check results**

| Check | Result |
|---|---:|
| Input records | 7,079,081 |
| Input columns | 35 |
| Date range | 1 January–31 December 2024 |
| Invalid month values | 0 |
| Invalid day-of-week values | 0 |
| Invalid dates | 0 |
| Invalid non-empty departure times | 0 |
| Negative distances | 0 |
| Missing arrival delays | 113,814 |
| Completed flights missing arrival delay | 0 |
| Minimum arrival delay | −126 minutes |
| Maximum arrival delay | 3,803 minutes |

## Appendix D: AWS Screenshots

Use the following screenshots from the private evidence folder. Crop or cover
the AWS account number, user name, public IP address and any other unnecessary
identifier before inserting them.

Table D.1 lists the AWS screenshots that support the implementation and testing
discussion. Each screenshot should be inserted under its stated figure number
with the suggested caption.

**Table D.1: AWS implementation screenshots and suggested captions**

| Figure | Suggested caption | Evidence file |
|---|---|---|
| Figure D.1 | Amazon S3 project folder structure | `Phase 1 - S3/01-bucket-folders.png` |
| Figure D.2 | Full 1.31 GB flight dataset stored in the S3 raw-data prefix | `Phase 1 - S3/03-full-dataset.png` |
| Figure D.3 | PySpark script and supporting library uploaded to S3 | `Phase 1 - S3/05-spark-scripts.png` |
| Figure D.4 | EMR 7.13.0 cluster in the waiting state with one primary and two core nodes | `Phase 2 - EMR Cluster/01-emr-cluster-waiting.png` |
| Figure D.5 | Successful completion of the PySpark sample step | `Phase 3 - PySpark Sample Run/02-sample-step-completed.png` |
| Figure D.6 | PySpark full-run benchmark and verified quality summary | `Phase 6 - PySpark Full Runs/03-full-run-1-benchmark-and-quality.png` |
| Figure D.7 | Internal runtimes for the three successful PySpark full-data runs | `Phase 6 - PySpark Full Runs/07-three-pyspark-benchmarks.png` |
| Figure D.8 | Pandas full-data process terminated because the `m5.large` ran out of memory | `Phase 7 - Pandas Full Runs/02-m5-large-out-of-memory-failure.png` |
| Figure D.9 | Successful Pandas full-data execution on the `r5.large` instance | `Phase 7 - Pandas Full Runs/04-r5-large-full-run-1-success.png` |
| Figure D.10 | All eight full-data output tables matched between Pandas and PySpark | `Phase 8 - Full Output Agreement/02-eight-full-tables-match.png` |
okay| Figure D.11 | EMR cluster terminated after processing | `Phase 6 - PySpark Full Runs/08-emr-cluster-terminated.png` |
| Figure D.12 | Pandas EC2 instance terminated after testing | `Phase 7 - Pandas Full Runs/08-pandas-ec2-terminated.png` |
