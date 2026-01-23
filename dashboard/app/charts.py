"""Chart generation functions using Altair."""

from typing import List, Optional

import altair as alt
import pandas as pd

from data_loader import METRICS

# Brand color matching netzbremse.de
BRAND_COLOR = "#e91e63"


def create_metric_line_chart(
    df: pd.DataFrame,
    metric_key: str,
    height: int = 300,
) -> alt.Chart:
    """Create a line chart for a single metric over time with hourly trend view."""
    metric_info = METRICS[metric_key]

    # Create unique named selection for pan/zoom
    zoom = alt.selection_interval(name=f"zoom_{metric_key}", encodings=["x"])

    # Line chart with hourly time formatting
    line = (
        alt.Chart(df)
        .mark_line(color=BRAND_COLOR, strokeWidth=2)
        .encode(
            x=alt.X(
                "timestamp:T",
                title="Time",
                axis=alt.Axis(
                    format="%H:%M",
                    labelAngle=-45,
                    tickCount="hour",
                ),
                scale=alt.Scale(domain=zoom),
            ),
            y=alt.Y(
                f"{metric_key}:Q",
                title=f"{metric_info['name']} ({metric_info['unit']})",
                scale=alt.Scale(zero=True),
            ),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Time", format="%Y-%m-%d %H:%M"),
                alt.Tooltip(f"{metric_key}:Q", title=metric_info["name"], format=".2f"),
            ],
        )
        .properties(height=height)
    )

    # Add points for better visibility
    points = (
        alt.Chart(df)
        .mark_circle(color=BRAND_COLOR, size=40)
        .encode(
            x="timestamp:T",
            y=f"{metric_key}:Q",
        )
    )

    return (line + points).add_params(zoom)


def create_speed_chart(
    df: pd.DataFrame,
    speed_metrics: List[str],
    height: int = 400,
) -> Optional[alt.Chart]:
    """Create a chart for speed metrics (Mbps)."""
    if not speed_metrics:
        return None

    # Melt data for speed metrics
    cols = ["timestamp"] + speed_metrics
    speed_df = df[cols].melt(
        id_vars=["timestamp"],
        var_name="metric",
        value_name="value",
    )
    # Map metric keys to display names
    speed_df["metric"] = speed_df["metric"].map(
        lambda x: METRICS[x]["name"] if x in METRICS else x
    )

    zoom_speed = alt.selection_interval(name="zoom_speed", encodings=["x"])

    speed_chart = (
        alt.Chart(speed_df)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X(
                "timestamp:T",
                title="Time",
                axis=alt.Axis(format="%H:%M", labelAngle=-45, tickCount="hour"),
                scale=alt.Scale(domain=zoom_speed),
            ),
            y=alt.Y("value:Q", title="Speed (Mbps)", scale=alt.Scale(zero=True)),
            color=alt.Color("metric:N", title="Metric"),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Time", format="%Y-%m-%d %H:%M"),
                alt.Tooltip("metric:N", title="Metric"),
                alt.Tooltip("value:Q", title="Value", format=".2f"),
            ],
        )
        .properties(height=height)
        .add_params(zoom_speed)
    )

    return speed_chart


def create_latency_chart(
    df: pd.DataFrame,
    latency_metrics: List[str],
    height: int = 400,
) -> Optional[alt.Chart]:
    """Create a chart for latency metrics (ms)."""
    if not latency_metrics:
        return None

    cols = ["timestamp"] + latency_metrics
    latency_df = df[cols].melt(
        id_vars=["timestamp"],
        var_name="metric",
        value_name="value",
    )
    latency_df["metric"] = latency_df["metric"].map(
        lambda x: METRICS[x]["name"] if x in METRICS else x
    )

    zoom_latency = alt.selection_interval(name="zoom_latency", encodings=["x"])

    latency_chart = (
        alt.Chart(latency_df)
        .mark_line(strokeWidth=2)
        .encode(
            x=alt.X(
                "timestamp:T",
                title="Time",
                axis=alt.Axis(format="%H:%M", labelAngle=-45, tickCount="hour"),
                scale=alt.Scale(domain=zoom_latency),
            ),
            y=alt.Y("value:Q", title="Latency (ms)", scale=alt.Scale(zero=True)),
            color=alt.Color("metric:N", title="Metric"),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Time", format="%Y-%m-%d %H:%M"),
                alt.Tooltip("metric:N", title="Metric"),
                alt.Tooltip("value:Q", title="Value", format=".2f"),
            ],
        )
        .properties(height=height)
        .add_params(zoom_latency)
    )

    return latency_chart


def create_combined_chart(
    df: pd.DataFrame,
    selected_metrics: List[str],
    height: int = 400,
) -> Optional[alt.Chart]:
    """
    Create a combined chart with multiple metrics.

    Groups metrics by unit type (Mbps vs ms) into separate panels.
    """
    if not selected_metrics:
        return None

    # Group metrics by unit type
    speed_metrics = [m for m in selected_metrics if METRICS[m]["unit"] == "Mbps"]
    latency_metrics = [m for m in selected_metrics if METRICS[m]["unit"] == "ms"]

    charts = []

    if speed_metrics:
        speed_chart = create_speed_chart(df, speed_metrics, height // 2)
        if speed_chart:
            charts.append(speed_chart.properties(title="Speed Metrics"))

    if latency_metrics:
        latency_chart = create_latency_chart(df, latency_metrics, height // 2)
        if latency_chart:
            charts.append(latency_chart.properties(title="Latency Metrics"))

    if len(charts) == 2:
        return alt.vconcat(*charts)
    elif len(charts) == 1:
        return charts[0]
    else:
        return None
