"""Reusable UI components for the dashboard."""

import pandas as pd
import streamlit as st

# Netzbremse logo URL
LOGO_URL = "https://netzbremse.de/img/hourglass_hu15528090548520832702.png"


def render_header():
    """Render the dashboard header with logo."""
    col1, col2 = st.columns([1, 5])
    with col1:
        st.image(LOGO_URL, width=80)
    with col2:
        st.title("Netzbremse Speedtest Dashboard")


def render_latest_summary(df: pd.DataFrame):
    """
    Render summary cards for the latest measurement.

    Shows the average of the last complete test run (typically 5 data points).
    """
    if df.empty:
        st.warning("No data available yet.")
        return

    # Average the last 5 measurements (one complete test run) for accurate values
    latest = df.iloc[-5:].mean(numeric_only=True)
    latest_timestamp = df.iloc[-1]["timestamp"]

    st.subheader("Latest Measurement")
    st.caption(
        f"Recorded at: {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')} (last of the set)"
    )

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Download",
            value=f"{latest.get('download', 0):.2f} Mbps",
        )
    with col2:
        st.metric(
            label="Upload",
            value=f"{latest.get('upload', 0):.2f} Mbps",
        )
    with col3:
        st.metric(
            label="Latency",
            value=f"{latest.get('latency', 0):.2f} ms",
        )
    with col4:
        st.metric(
            label="Jitter",
            value=f"{latest.get('jitter', 0):.2f} ms",
        )

    st.caption(
        "Values are averaged over the last complete test run, which typically consists of 5 individual measurements."
    )


def render_recent_table(df: pd.DataFrame, count: int = 5):
    """Render a table of the most recent measurements."""
    if df.empty:
        return

    st.subheader(f"Last {min(count, len(df))} Measurements")

    display_df = df.head(count).copy()
    display_df["timestamp"] = display_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M")

    # Select and rename columns for display
    columns_to_show = ["timestamp", "download", "upload", "latency", "jitter"]
    display_df = display_df[columns_to_show]

    column_rename = {
        "timestamp": "Time",
        "download": "Download (Mbps)",
        "upload": "Upload (Mbps)",
        "latency": "Latency (ms)",
        "jitter": "Jitter (ms)",
    }
    display_df = display_df.rename(columns=column_rename)

    # Format numeric columns
    for col in ["Download (Mbps)", "Upload (Mbps)", "Latency (ms)", "Jitter (ms)"]:
        display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}")

    st.dataframe(display_df, width="stretch", hide_index=True)
