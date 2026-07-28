"""Create report-ready charts from the verified AWS analytical outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter, PercentFormatter


BLUE = "#2F6B9A"
BLUE_LIGHT = "#BFD5E5"
ORANGE = "#D9822B"
INK = "#24313D"
MUTED = "#66737F"
GRID = "#DCE3E8"
PALE = "#EEF3F6"

SOURCE_NOTE = (
    "Source: Flight Data 2024; authors' AWS PySpark and Pandas analytical outputs."
)

TABLES = [
    "airline_delay_rates",
    "origin_delay_rates",
    "monthly_delay_rates",
    "day_of_week_delay_rates",
    "departure_hour_delay_rates",
    "recorded_delay_causes",
    "route_delay_statistics",
    "data_quality_summary",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("results/aws/outputs"),
        help="Extracted AWS outputs directory",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports/figures/results"),
        help="Destination for report-ready PNG figures",
    )
    parser.add_argument(
        "--minimum-airport-flights",
        type=int,
        default=10_000,
        help="Completed-flight threshold for the main airport ranking",
    )
    parser.add_argument("--top-n", type=int, default=10)
    return parser.parse_args()


def read_tables(input_root: Path) -> dict[str, pd.DataFrame]:
    pandas_root = input_root / "pandas" / "full" / "run-1"
    missing = [
        str(pandas_root / f"{name}.csv")
        for name in TABLES
        if not (pandas_root / f"{name}.csv").exists()
    ]
    if missing:
        raise FileNotFoundError("Missing verified result tables:\n" + "\n".join(missing))
    return {
        name: pd.read_csv(pandas_root / f"{name}.csv") for name in TABLES
    }


def read_benchmarks(input_root: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    pandas_rows = []
    spark_rows = []
    for run in (1, 2, 3):
        pandas_path = (
            input_root / "pandas" / "full" / f"run-{run}" / "benchmark.json"
        )
        spark_candidates = list(
            (
                input_root / "pyspark" / "full" / f"run-{run}" / "benchmark"
            ).glob("part-*.json")
        )
        if not pandas_path.exists() or len(spark_candidates) != 1:
            raise FileNotFoundError(f"Missing benchmark record for run {run}.")
        pandas_record = json.loads(pandas_path.read_text(encoding="utf-8"))
        spark_record = json.loads(spark_candidates[0].read_text(encoding="utf-8"))
        pandas_rows.append(
            {
                "run": run,
                "engine": "Pandas",
                "elapsed_seconds": float(pandas_record["elapsed_seconds"]),
                "peak_memory_bytes": int(pandas_record["peak_process_memory_bytes"]),
            }
        )
        spark_rows.append(
            {
                "run": run,
                "engine": "PySpark",
                "elapsed_seconds": float(spark_record["elapsed_seconds"]),
                "input_partitions": int(spark_record["input_partitions"]),
            }
        )
    return pd.DataFrame(pandas_rows), pd.DataFrame(spark_rows)


def apply_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.titlesize": 14,
            "axes.titleweight": "bold",
            "axes.labelcolor": INK,
            "axes.edgecolor": MUTED,
            "axes.linewidth": 0.8,
            "xtick.color": INK,
            "ytick.color": INK,
            "text.color": INK,
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "savefig.facecolor": "white",
        }
    )


def clean_axis(ax: plt.Axes, grid_axis: str = "x") -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def add_source(fig: plt.Figure, extra: str = "") -> None:
    note = SOURCE_NOTE if not extra else f"{SOURCE_NOTE} {extra}"
    fig.text(0.01, 0.012, note, ha="left", va="bottom", fontsize=8, color=MUTED)


def save(fig: plt.Figure, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=300, bbox_inches="tight")
    plt.close(fig)


def chart_airlines(df: pd.DataFrame, path: Path) -> None:
    ranked = df.sort_values("delay_rate_pct", ascending=True).copy()
    colors = [ORANGE if code == ranked.iloc[-1]["op_unique_carrier"] else BLUE for code in ranked["op_unique_carrier"]]
    fig, ax = plt.subplots(figsize=(10, 7.5))
    bars = ax.barh(ranked["op_unique_carrier"], ranked["delay_rate_pct"], color=colors)
    ax.set_title("Arrival delay rate by operating carrier", loc="left", pad=24)
    ax.text(
        0,
        1.015,
        "Completed US domestic flights in 2024; delayed means arrival delay ≥15 minutes",
        transform=ax.transAxes,
        color=MUTED,
    )
    ax.set_xlabel("Delayed completed flights (%)")
    ax.set_ylabel("Operating carrier code")
    ax.set_xlim(0, max(ranked["delay_rate_pct"]) * 1.3)
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
    clean_axis(ax)
    for bar, (_, row) in zip(bars, ranked.iterrows()):
        ax.text(
            bar.get_width() + 0.35,
            bar.get_y() + bar.get_height() / 2,
            f"{row.delay_rate_pct:.1f}%  ·  {row.completed_flights / 1000:,.0f}k flights",
            va="center",
            fontsize=9,
        )
    add_source(fig)
    fig.subplots_adjust(bottom=0.1, top=0.89)
    save(fig, path)


def chart_airports(
    df: pd.DataFrame, path: Path, minimum_flights: int, top_n: int
) -> None:
    eligible = df[df["completed_flights"] >= minimum_flights].copy()
    ranked = eligible.nlargest(top_n, "delay_rate_pct").sort_values(
        "delay_rate_pct", ascending=True
    )
    colors = [
        ORANGE if airport == ranked.iloc[-1]["origin"] else BLUE
        for airport in ranked["origin"]
    ]
    fig, ax = plt.subplots(figsize=(10, 7.5))
    bars = ax.barh(ranked["origin"], ranked["delay_rate_pct"], color=colors)
    ax.set_title("Origin airports with the highest arrival delay rates", loc="left", pad=24)
    ax.text(
        0,
        1.015,
        f"Top {top_n} among airports with at least {minimum_flights:,} completed flights in 2024",
        transform=ax.transAxes,
        color=MUTED,
    )
    ax.set_xlabel("Delayed completed flights (%)")
    ax.set_ylabel("Origin airport")
    ax.set_xlim(0, max(ranked["delay_rate_pct"]) * 1.35)
    ax.xaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
    clean_axis(ax)
    for bar, (_, row) in zip(bars, ranked.iterrows()):
        ax.text(
            bar.get_width() + 0.3,
            bar.get_y() + bar.get_height() / 2,
            f"{row.delay_rate_pct:.1f}%  ·  {row.completed_flights / 1000:,.1f}k flights",
            va="center",
            fontsize=9,
        )
    coverage = eligible["completed_flights"].sum() / df["completed_flights"].sum() * 100
    add_source(
        fig,
        f"The airport threshold retains {len(eligible)} airports and {coverage:.1f}% of completed flights.",
    )
    fig.subplots_adjust(bottom=0.1, top=0.89)
    save(fig, path)


def chart_temporal(
    monthly: pd.DataFrame,
    weekday: pd.DataFrame,
    hourly: pd.DataFrame,
    path: Path,
) -> None:
    month_names = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    fig, axes = plt.subplots(3, 1, figsize=(12, 13))
    fig.suptitle(
        "Temporal patterns in arrival delay rates",
        x=0.06,
        y=0.995,
        ha="left",
        fontsize=17,
        fontweight="bold",
    )
    fig.text(
        0.06,
        0.963,
        "Completed US domestic flights in 2024; delay rate uses the ≥15-minute arrival threshold",
        color=MUTED,
    )

    month_colors = [
        ORANGE if value == monthly["delay_rate_pct"].max() else BLUE
        for value in monthly["delay_rate_pct"]
    ]
    bars = axes[0].bar(month_names, monthly["delay_rate_pct"], color=month_colors)
    axes[0].set_title("A. Monthly delay rate", loc="left")
    axes[0].set_ylabel("Delayed flights (%)")
    axes[0].set_ylim(0, monthly["delay_rate_pct"].max() * 1.22)
    axes[0].yaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
    clean_axis(axes[0], "y")
    axes[0].bar_label(bars, labels=[f"{x:.1f}%" for x in monthly["delay_rate_pct"]], padding=3, fontsize=8)

    day_colors = [
        ORANGE if value == weekday["delay_rate_pct"].max() else BLUE
        for value in weekday["delay_rate_pct"]
    ]
    bars = axes[1].bar(day_names, weekday["delay_rate_pct"], color=day_colors)
    axes[1].set_title("B. Day-of-week delay rate", loc="left")
    axes[1].set_ylabel("Delayed flights (%)")
    axes[1].set_ylim(0, weekday["delay_rate_pct"].max() * 1.25)
    axes[1].yaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
    clean_axis(axes[1], "y")
    axes[1].bar_label(bars, labels=[f"{x:.1f}%" for x in weekday["delay_rate_pct"]], padding=3, fontsize=8)

    ax = axes[2]
    volume_axis = ax.twinx()
    volume_axis.bar(
        hourly["departure_hour"],
        hourly["completed_flights"],
        color=PALE,
        width=0.82,
        label="Completed flights",
        zorder=1,
    )
    ax.plot(
        hourly["departure_hour"],
        hourly["delay_rate_pct"],
        color=BLUE,
        marker="o",
        markersize=4,
        linewidth=2,
        label="Delay rate",
        zorder=3,
    )
    ax.set_title("C. Delay rate and flight volume by actual departure hour", loc="left")
    ax.set_xlabel("Departure hour (24-hour clock)")
    ax.set_ylabel("Delayed flights (%)", color=BLUE)
    volume_axis.set_ylabel("Completed flights", color=MUTED)
    ax.set_xlim(-0.5, 23.5)
    ax.set_xticks(range(24))
    ax.set_ylim(0, max(hourly["delay_rate_pct"]) * 1.15)
    ax.yaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
    volume_axis.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x / 1000:,.0f}k"))
    clean_axis(ax, "y")
    volume_axis.spines["top"].set_visible(False)
    volume_axis.spines["right"].set_color(MUTED)
    ax.legend(loc="upper left", frameon=False)
    volume_axis.legend(loc="upper right", frameon=False)
    add_source(
        fig,
        "Day-of-week values use the BTS convention: 1=Monday through 7=Sunday.",
    )
    fig.subplots_adjust(left=0.08, right=0.9, bottom=0.06, top=0.91, hspace=0.52)
    save(fig, path)


def chart_causes(df: pd.DataFrame, path: Path) -> None:
    ordered = df.sort_values("total_delay_minutes", ascending=True)
    fig, axes = plt.subplots(1, 2, figsize=(14, 6.5))
    fig.suptitle("Recorded causes of arrival delay", x=0.06, ha="left", fontsize=17, fontweight="bold")
    fig.text(
        0.06,
        0.925,
        "Operational attribution fields for completed flights in 2024; categories are not causal estimates",
        color=MUTED,
    )

    bars = axes[0].barh(ordered["cause"], ordered["share_of_cause_minutes_pct"], color=BLUE)
    axes[0].set_title("A. Share of attributed delay minutes", loc="left")
    axes[0].set_xlabel("Share of attributed minutes (%)")
    axes[0].set_xlim(0, ordered["share_of_cause_minutes_pct"].max() * 1.3)
    axes[0].xaxis.set_major_formatter(PercentFormatter(xmax=100, decimals=0))
    clean_axis(axes[0])
    for bar, value in zip(bars, ordered["share_of_cause_minutes_pct"]):
        axes[0].text(bar.get_width() + 0.5, bar.get_y() + bar.get_height() / 2, f"{value:.1f}%", va="center")

    bars = axes[1].barh(ordered["cause"], ordered["affected_flights"], color=ORANGE)
    axes[1].set_title("B. Flights affected by each recorded cause", loc="left")
    axes[1].set_xlabel("Affected completed flights")
    axes[1].set_xlim(0, ordered["affected_flights"].max() * 1.3)
    axes[1].xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x / 1000:,.0f}k"))
    clean_axis(axes[1])
    for bar, value in zip(bars, ordered["affected_flights"]):
        axes[1].text(bar.get_width() + 10_000, bar.get_y() + bar.get_height() / 2, f"{value / 1000:,.0f}k", va="center")

    add_source(fig)
    fig.subplots_adjust(left=0.12, right=0.97, bottom=0.12, top=0.85, wspace=0.5)
    save(fig, path)


def chart_routes(df: pd.DataFrame, path: Path, top_n: int) -> None:
    ranked = df.nlargest(top_n, "average_arrival_delay_minutes").copy()
    ranked["route"] = ranked["origin"] + "–" + ranked["dest"]
    ranked = ranked.sort_values("average_arrival_delay_minutes", ascending=True)
    colors = [
        ORANGE if value == ranked["average_arrival_delay_minutes"].max() else BLUE
        for value in ranked["average_arrival_delay_minutes"]
    ]
    fig, ax = plt.subplots(figsize=(11, 8))
    bars = ax.barh(ranked["route"], ranked["average_arrival_delay_minutes"], color=colors)
    ax.set_title("Domestic routes with the longest average arrival delays", loc="left", pad=24)
    ax.text(
        0,
        1.015,
        f"Top {top_n} routes among those with at least 100 completed flights in 2024",
        transform=ax.transAxes,
        color=MUTED,
    )
    ax.set_xlabel("Average arrival delay (minutes)")
    ax.set_ylabel("Origin–destination route")
    ax.set_xlim(0, ranked["average_arrival_delay_minutes"].max() * 1.18)
    clean_axis(ax)
    for bar, (_, row) in zip(bars, ranked.iterrows()):
        ax.text(
            bar.get_width() + 1,
            bar.get_y() + bar.get_height() / 2,
            f"{row.average_arrival_delay_minutes:.1f} min",
            va="center",
            fontsize=8.5,
        )
    add_source(fig)
    fig.subplots_adjust(bottom=0.1, top=0.89)
    save(fig, path)


def chart_runtime(
    pandas_bench: pd.DataFrame, spark_bench: pd.DataFrame, path: Path
) -> None:
    x = np.arange(3)
    width = 0.34
    fig, ax = plt.subplots(figsize=(10.5, 6.5))
    pandas_bars = ax.bar(
        x - width / 2,
        pandas_bench["elapsed_seconds"],
        width,
        label="Pandas on one r5.large",
        color=ORANGE,
    )
    spark_bars = ax.bar(
        x + width / 2,
        spark_bench["elapsed_seconds"],
        width,
        label="PySpark on EMR",
        color=BLUE,
    )
    ax.set_title("Internal processing runtime by engine and run", loc="left", pad=24)
    ax.text(
        0,
        1.015,
        "Same 1.31 GB CSV and analytical definitions; excludes cluster provisioning and file transfer",
        transform=ax.transAxes,
        color=MUTED,
    )
    ax.set_xlabel("Full-data run")
    ax.set_ylabel("Elapsed time (seconds)")
    ax.set_xticks(x, ["Run 1", "Run 2", "Run 3"])
    ax.set_ylim(0, max(spark_bench["elapsed_seconds"]) * 1.25)
    clean_axis(ax, "y")
    ax.legend(frameon=False, loc="upper left")
    ax.bar_label(pandas_bars, labels=[f"{x:.1f}s" for x in pandas_bench["elapsed_seconds"]], padding=3)
    ax.bar_label(spark_bars, labels=[f"{x:.1f}s" for x in spark_bench["elapsed_seconds"]], padding=3)
    pandas_median = pandas_bench["elapsed_seconds"].median()
    spark_median = spark_bench["elapsed_seconds"].median()
    ax.text(
        0.99,
        0.94,
        f"Median: Pandas {pandas_median:.1f}s · PySpark {spark_median:.1f}s",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=10,
        color=INK,
    )
    add_source(
        fig,
        "Pandas required about 8.26 GB peak process memory and failed on an 8 GiB m5.large; Spark used 10 input partitions.",
    )
    fig.subplots_adjust(bottom=0.12, top=0.88)
    save(fig, path)


def main() -> None:
    args = parse_args()
    if args.minimum_airport_flights < 1 or args.top_n < 1:
        raise ValueError("Thresholds must be positive.")
    apply_style()
    tables = read_tables(args.input_root)
    pandas_bench, spark_bench = read_benchmarks(args.input_root)

    chart_airlines(
        tables["airline_delay_rates"], args.output_dir / "01_airline_delay_rates.png"
    )
    chart_airports(
        tables["origin_delay_rates"],
        args.output_dir / "02_origin_airport_delay_rates.png",
        args.minimum_airport_flights,
        args.top_n,
    )
    chart_temporal(
        tables["monthly_delay_rates"],
        tables["day_of_week_delay_rates"],
        tables["departure_hour_delay_rates"],
        args.output_dir / "03_temporal_delay_patterns.png",
    )
    chart_causes(
        tables["recorded_delay_causes"],
        args.output_dir / "04_recorded_delay_causes.png",
    )
    chart_routes(
        tables["route_delay_statistics"],
        args.output_dir / "05_longest_average_route_delays.png",
        args.top_n,
    )
    chart_runtime(
        pandas_bench,
        spark_bench,
        args.output_dir / "06_runtime_comparison.png",
    )

    print(f"Created 6 report figures in {args.output_dir.resolve()}")
    print(
        f"Airport threshold: {args.minimum_airport_flights:,} completed flights; "
        f"top {args.top_n} shown."
    )
    print(
        f"Median internal runtime: Pandas {pandas_bench.elapsed_seconds.median():.4f}s; "
        f"PySpark {spark_bench.elapsed_seconds.median():.4f}s."
    )


if __name__ == "__main__":
    main()
