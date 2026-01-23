"""Data loading and caching logic for speedtest results."""

import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

# Environment variables
DATA_DIR = os.environ.get("DATA_DIR", "/data")
REFRESH_INTERVAL_SECONDS = int(os.environ.get("REFRESH_INTERVAL_SECONDS", "60"))

# Metric definitions with display names, units, and conversion functions
METRICS = {
    "download": {
        "name": "Download",
        "unit": "Mbps",
        "convert": lambda x: x / 1_000_000,
    },
    "upload": {
        "name": "Upload",
        "unit": "Mbps",
        "convert": lambda x: x / 1_000_000,
    },
    "latency": {
        "name": "Latency",
        "unit": "ms",
        "convert": lambda x: x,
    },
    "jitter": {
        "name": "Jitter",
        "unit": "ms",
        "convert": lambda x: x,
    },
    "downLoadedLatency": {
        "name": "Loaded Latency (Down)",
        "unit": "ms",
        "convert": lambda x: x,
    },
    "downLoadedJitter": {
        "name": "Loaded Jitter (Down)",
        "unit": "ms",
        "convert": lambda x: x,
    },
    "upLoadedLatency": {
        "name": "Loaded Latency (Up)",
        "unit": "ms",
        "convert": lambda x: x,
    },
    "upLoadedJitter": {
        "name": "Loaded Jitter (Up)",
        "unit": "ms",
        "convert": lambda x: x,
    },
}


def parse_timestamp_from_filename(filename: str) -> Optional[datetime]:
    """
    Extract timestamp from filename like 'speedtest-2024-01-15T10-30-00-000Z.json'.

    The timestamp format is ISO 8601 with colons/dots replaced by hyphens.
    """
    pattern = r"speedtest-(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z)\.json"
    match = re.match(pattern, filename)
    if not match:
        return None

    timestamp_str = match.group(1)
    # Convert back to standard ISO format: replace hyphens with colons in time part
    # 2024-01-15T10-30-00-000Z -> 2024-01-15T10:30:00.000Z
    parts = timestamp_str.split("T")
    if len(parts) != 2:
        return None

    date_part = parts[0]
    time_part = parts[1]

    # Time format: HH-mm-ss-SSSZ -> HH:mm:ss.SSSZ
    time_match = re.match(r"(\d{2})-(\d{2})-(\d{2})-(\d{3})Z", time_part)
    if not time_match:
        return None

    hour, minute, second, ms = time_match.groups()
    iso_str = f"{date_part}T{hour}:{minute}:{second}.{ms}Z"

    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_single_file(filepath: Path) -> Optional[dict]:
    """
    Load and validate a single JSON file.

    Returns None if file is corrupt or missing required fields.
    """
    try:
        with open(filepath, "r") as f:
            data = json.load(f)

        # Filter out failed measurements
        if not data.get("success", False):
            return None

        if "result" not in data:
            return None

        # Extract timestamp from filename
        timestamp = parse_timestamp_from_filename(filepath.name)
        if timestamp is None:
            return None

        # Build record with converted metrics
        record = {
            "timestamp": timestamp,
            "sessionID": data.get("sessionID"),
            "endpoint": data.get("endpoint"),
        }

        for key, config in METRICS.items():
            if key in data["result"]:
                record[key] = config["convert"](data["result"][key])

        return record
    except (json.JSONDecodeError, KeyError, TypeError, OSError):
        return None


@st.cache_data(ttl=max(REFRESH_INTERVAL_SECONDS - 5, 5))
def load_all_data() -> pd.DataFrame:
    """
    Load all speedtest JSON files from DATA_DIR.

    Returns a DataFrame sorted by timestamp (oldest first).
    Cached with TTL slightly less than refresh interval.
    """
    data_path = Path(DATA_DIR)
    if not data_path.exists():
        return pd.DataFrame()

    records = []
    for filepath in data_path.glob("speedtest-*.json"):
        record = load_single_file(filepath)
        if record:
            records.append(record)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = df.sort_values("timestamp", ascending=True)
    return df


def get_latest_measurements(df: pd.DataFrame, count: int = 5) -> pd.DataFrame:
    """Return the most recent N measurements (most recent first)."""
    if df.empty:
        return df
    return df.tail(count).iloc[::-1]


def aggregate_to_intervals(
    df: pd.DataFrame, interval_minutes: int = 10
) -> pd.DataFrame:
    """
    Aggregate measurements into time intervals.

    Each measurement run produces ~5 data points. This function groups them
    by the specified interval and calculates the mean for each metric.
    """
    if df.empty:
        return df

    df = df.copy()
    df["interval"] = df["timestamp"].dt.floor(f"{interval_minutes}min")

    metric_cols = [col for col in df.columns if col in METRICS]
    agg_dict = {col: "mean" for col in metric_cols}
    agg_df = df.groupby("interval").agg(agg_dict).reset_index()
    agg_df = agg_df.rename(columns={"interval": "timestamp"})

    return agg_df.sort_values("timestamp", ascending=True)
