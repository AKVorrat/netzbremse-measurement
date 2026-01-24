"""Netzbremse Speedtest Dashboard - Main Application."""

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import streamlit as st
from charts import create_metric_line_chart
from components import (
    render_header,
    render_latest_summary,
)
from data_loader import (
    DATA_DIR,
    METRICS,
    REFRESH_INTERVAL_SECONDS,
    aggregate_to_intervals,
    get_latest_measurements,
    load_all_data,
)

# Configure app logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Number of recent measurements to show
RECENT_COUNT = 5

# Default metrics to display
DEFAULT_METRICS = ["download", "upload", "latency", "jitter"]

# Page configuration
st.set_page_config(
    page_title="Netzbremse Dashboard",
    page_icon=":hourglass:",
    layout="wide",
    initial_sidebar_state="expanded",
)

logger.info("Dashboard page render started")

# Custom CSS for brand color buttons and reduced spacing
st.markdown(
    """
    <style>
    .block-container {
        padding-top: 1rem;
    }
    .stTitle {
        margin-top: -1.5rem;
    }
    .stButton > button {
        background-color: #e91e63;
        color: white;
        border: none;
    }
    .stButton > button:hover {
        background-color: #c2185b;
        color: white;
        border: none;
    }
    .stDownloadButton > button {
        background-color: #e91e63;
        color: white;
        border: none;
    }
    .stDownloadButton > button:hover {
        background-color: #c2185b;
        color: white;
        border: none;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# Sidebar - Settings
st.sidebar.title("Settings")

# Fixed timezone for all data
DISPLAY_TIMEZONE = ZoneInfo("Europe/Berlin")

# Main content
render_header()

# Load data
with st.spinner("Loading data..."):
    df_utc = load_all_data()


def convert_timezone(df_input):
    """Convert timestamp column to Europe/Berlin timezone."""
    if df_input.empty:
        return df_input
    df_out = df_input.copy()
    # Ensure timestamp is timezone-aware (UTC), then convert to Berlin time
    if df_out["timestamp"].dt.tz is None:
        df_out["timestamp"] = df_out["timestamp"].dt.tz_localize("UTC")
    df_out["timestamp"] = df_out["timestamp"].dt.tz_convert(DISPLAY_TIMEZONE)
    return df_out


# Convert to Europe/Berlin timezone for all data
df = convert_timezone(df_utc)

# Check for stale data (no new measurements in last 2 hours)
if not df.empty:
    latest_measurement = df["timestamp"].max()
    now = datetime.now(DISPLAY_TIMEZONE)
    time_since_last = now - latest_measurement
    if time_since_last > timedelta(hours=2):
        total_hours = time_since_last.total_seconds() / 3600
        if total_hours >= 48:
            time_ago_str = f"{round(total_hours / 24):.0f} days"
        else:
            time_ago_str = f"{round(total_hours, 1):.0f} hours"
        logger.warning(
            "Stale data detected: last measurement was %s ago (at %s)",
            time_ago_str,
            latest_measurement.strftime("%Y-%m-%d %H:%M"),
        )
        st.warning(
            f"⚠️ No new data in the last ~{time_ago_str}. "
            f"Last measurement: {latest_measurement.strftime('%Y-%m-%d %H:%M')}"
        )

# Date range selector - First option in sidebar
if not df.empty:
    st.sidebar.markdown("---")
    st.sidebar.subheader("Date Range Filter")
    min_date = df["timestamp"].min().date()
    max_date = df["timestamp"].max().date()

    # Versioned key pattern: changing the key forces a fresh widget instance
    date_filter_key = f"date_range_filter_v{st.session_state.get('_date_filter_v', 0)}"

    date_range = st.sidebar.date_input(
        "Select date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        help="Filter data by date range",
        key=date_filter_key,
    )

    # Apply date filter if range is selected
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        original_count = len(df)
        df = df[
            (df["timestamp"].dt.date >= start_date)
            & (df["timestamp"].dt.date <= end_date)
        ]
        logger.debug(
            "Date filter applied: %s to %s (%d -> %d records)",
            start_date,
            end_date,
            original_count,
            len(df),
        )
        st.sidebar.caption(f"Filtered to {len(df)} measurements")

    def reset_date_filter():
        """Increment version to force new widget, clean up old key."""
        logger.info("Date range reset by user")
        old_version = st.session_state.get("_date_filter_v", 0)
        st.session_state["_date_filter_v"] = old_version + 1
        # Clean up old key to avoid accumulation
        old_key = f"date_range_filter_v{old_version}"
        if old_key in st.session_state:
            del st.session_state[old_key]

    st.sidebar.button("Reset Date Range", on_click=reset_date_filter)

    # Chart settings
    st.sidebar.markdown("---")
    st.sidebar.subheader("Chart Settings")
    aggregate_data = st.sidebar.checkbox(
        "Group by measurement run",
        value=True,
        help="Each run produces 5 data points. Enable to average them into single"
        " points. Makes cleaner charts.",
    )

# Refresh info
st.sidebar.markdown("---")
st.sidebar.subheader("Auto-Refresh")
refresh_note = (
    f"{REFRESH_INTERVAL_SECONDS // 60} minutes"
    if REFRESH_INTERVAL_SECONDS >= 60
    else f"{REFRESH_INTERVAL_SECONDS} seconds"
)

st.sidebar.caption(
    f"The dashboard refreshes data from the linked directory automatically every"
    f" {refresh_note}. You can also manually refresh it any time."
)

if st.sidebar.button("Manual Refresh", width="stretch"):
    logger.info("Manual refresh triggered by user")
    st.cache_data.clear()
    st.rerun()

# Show data count in sidebar after loading
if not df.empty:
    from_date = df["timestamp"].min().strftime("%Y-%m-%d %H:%M")
    to_date = df["timestamp"].max().strftime("%Y-%m-%d %H:%M")

    # CSV Download button at bottom of sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("Export Data")
    import io

    # Prepare export data with clear timezone in column name
    export_df = df.copy()
    export_df = export_df.rename(columns={"timestamp": "timestamp_Europe_Berlin"})
    # Format timestamp as ISO 8601 with timezone offset for clarity
    export_df["timestamp_Europe_Berlin"] = export_df[
        "timestamp_Europe_Berlin"
    ].dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    csv_buffer = io.StringIO()
    export_df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()

    st.sidebar.download_button(
        label="📥 Download as CSV",
        data=csv_data,
        file_name="speedtest_data.csv",
        mime="text/csv",
        help="Download data as CSV (timestamps in Europe/Berlin timezone)",
        width="stretch",
    )
    st.sidebar.caption(f"Loaded {len(df)} measurements\n({from_date} to {to_date})")

if df.empty:
    logger.warning("No speedtest data found in DATA_DIR=%s", DATA_DIR)
    st.warning(f"No speedtest data found in `{DATA_DIR}`")
    st.info(
        "Make sure speedtest files are being saved to the configured directory. "
        "Check that the `DATA_DIR` environment variable is set correctly."
    )
    st.stop()

# Get latest measurements for summary (raw data, most recent first)
latest_df = get_latest_measurements(df, RECENT_COUNT)

# Prepare chart data (optionally aggregated)
chart_df = aggregate_to_intervals(df, interval_minutes=10) if aggregate_data else df

# Summary section
st.markdown("---")
render_latest_summary(latest_df)

# Charts section
st.markdown("---")
st.subheader("Performance Over Time")

# Individual charts in tabs
tabs = st.tabs([METRICS[m]["name"] for m in DEFAULT_METRICS])

for tab, metric in zip(tabs, DEFAULT_METRICS):
    with tab:
        chart = create_metric_line_chart(chart_df, metric)
        st.altair_chart(chart, width="stretch")

# Combined view - Split into separate charts
st.markdown("---")
st.subheader("Combined View")

# Separate speed and latency charts
speed_metrics = [m for m in DEFAULT_METRICS if METRICS[m]["unit"] == "Mbps"]
latency_metrics = [m for m in DEFAULT_METRICS if METRICS[m]["unit"] == "ms"]

if speed_metrics:
    st.markdown("### Speed Metrics")
    from charts import create_speed_chart

    speed_chart = create_speed_chart(chart_df, speed_metrics)
    if speed_chart:
        st.altair_chart(speed_chart, width="stretch")

if latency_metrics:
    st.markdown("### Latency Metrics")
    from charts import create_latency_chart

    latency_chart = create_latency_chart(chart_df, latency_metrics)
    if latency_chart:
        st.altair_chart(latency_chart, width="stretch")

st.markdown("---")
st.caption("All timestamps are displayed in Europe/Berlin timezone (CET/CEST).")
