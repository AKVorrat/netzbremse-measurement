"""Reusable UI components for the dashboard."""

import pandas as pd
import streamlit as st


def render_header():
    """Render the dashboard header."""
    st.markdown("# Netzbremse Speedtest Dashboard")


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
    latest_timestamp = df["timestamp"].max()  # Most recent measurement timestamp

    st.subheader("Latest Measurement")
    # Format timestamp with timezone name from the timestamp itself
    tz_name = latest_timestamp.strftime("%Z") if latest_timestamp.tzinfo else ""
    st.caption(
        f"Recorded at: {latest_timestamp.strftime('%Y-%m-%d %H:%M:%S')} {tz_name}"
        f" (last of the set)"
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
        "Values are averaged over the last complete test run, which typically"
        " consists of 5 individual measurements."
    )

    # Show last 5 measurements in an accordion
    with st.expander("View individual measurements from this test run"):
        last_5_df = df.iloc[-5:].copy()
        last_5_df["timestamp"] = last_5_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        # Select all available columns
        all_columns = [
            "timestamp",
            "sessionID",
            "endpoint",
            "download",
            "upload",
            "latency",
            "jitter",
            "downLoadedLatency",
            "downLoadedJitter",
            "upLoadedLatency",
            "upLoadedJitter",
        ]
        available_cols = [col for col in all_columns if col in last_5_df.columns]
        display_df = last_5_df[available_cols].copy()

        column_rename = {
            "timestamp": "Time",
            "sessionID": "Session ID",
            "endpoint": "Endpoint",
            "download": "Download (Mbps)",
            "upload": "Upload (Mbps)",
            "latency": "Latency (ms)",
            "jitter": "Jitter (ms)",
            "downLoadedLatency": "Loaded Latency Down (ms)",
            "downLoadedJitter": "Loaded Jitter Down (ms)",
            "upLoadedLatency": "Loaded Latency Up (ms)",
            "upLoadedJitter": "Loaded Jitter Up (ms)",
        }
        display_df = display_df.rename(columns=column_rename)

        # Format numeric columns
        numeric_cols = [
            col
            for col in display_df.columns
            if col not in ["Time", "Session ID", "Endpoint"]
        ]
        for col in numeric_cols:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}")

        st.dataframe(display_df, width="stretch", hide_index=True)
