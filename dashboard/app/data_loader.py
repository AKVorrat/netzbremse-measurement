"""Data loading and caching logic for speedtest results."""

import hashlib
import json
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

# Environment variables
DATA_DIR = os.environ.get("DATA_DIR", "/data")
REFRESH_INTERVAL_SECONDS = int(os.environ.get("REFRESH_INTERVAL_SECONDS", "60"))


def _get_cache_path() -> Path:
    """Get cache file path in system temp directory, unique per DATA_DIR."""
    # Hash DATA_DIR to create unique cache file per data source
    dir_hash = hashlib.md5(DATA_DIR.encode()).hexdigest()[:12]
    return Path(tempfile.gettempdir()) / f"speedtest_cache_{dir_hash}.parquet"


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
            "source_file": filepath.name,  # Track which file this came from
            "sessionID": data.get("sessionID"),
            "endpoint": data.get("endpoint"),
        }

        for key, config in METRICS.items():
            if key in data["result"]:
                record[key] = config["convert"](data["result"][key])

        return record
    except (json.JSONDecodeError, KeyError, TypeError, OSError):
        return None


def _load_json_files_parallel(filepaths: list[Path]) -> list[dict]:
    """Load multiple JSON files in parallel using thread pool."""
    if not filepaths:
        return []

    records = []
    with ThreadPoolExecutor(max_workers=min(32, len(filepaths))) as executor:
        future_to_path = {executor.submit(load_single_file, fp): fp for fp in filepaths}
        for future in as_completed(future_to_path):
            record = future.result()
            if record:
                records.append(record)
    return records


def _load_cache(cache_path: Path) -> Optional[pd.DataFrame]:
    """Load data from Parquet cache file if it exists."""
    if not cache_path.exists():
        return None
    try:
        return pd.read_parquet(cache_path)
    except Exception:
        # Cache corrupted, will rebuild
        return None


def _save_cache(df: pd.DataFrame, cache_path: Path) -> None:
    """Save DataFrame to Parquet cache file."""
    try:
        df.to_parquet(cache_path, index=False)
    except Exception:
        # Non-fatal: cache save failed, will work without it
        pass


@st.cache_data(ttl=max(REFRESH_INTERVAL_SECONDS - 5, 5))
def load_all_data() -> pd.DataFrame:
    """
    Load speedtest data with Parquet caching for fast startup.

    On first run, loads all JSON files and creates a cache.
    On subsequent runs, loads from cache and only parses new JSON files.
    Returns a DataFrame sorted by timestamp (oldest first).
    """
    data_path = Path(DATA_DIR)
    if not data_path.exists():
        return pd.DataFrame()

    cache_path = _get_cache_path()

    # Get all JSON files in directory
    all_json_files = {fp.name: fp for fp in data_path.glob("speedtest-*.json")}
    if not all_json_files:
        return pd.DataFrame()

    # Try to load existing cache
    cached_df = _load_cache(cache_path)

    if cached_df is not None and not cached_df.empty:
        # Find files not yet in cache
        cached_files = set(cached_df["source_file"].unique())
        new_files = [
            fp for name, fp in all_json_files.items() if name not in cached_files
        ]

        if not new_files:
            # Cache is up to date
            return cached_df.drop(columns=["source_file"]).sort_values(
                "timestamp", ascending=True
            )

        # Load only new files
        new_records = _load_json_files_parallel(new_files)

        if new_records:
            new_df = pd.DataFrame(new_records)
            df = pd.concat([cached_df, new_df], ignore_index=True)
        else:
            df = cached_df
    else:
        # No cache, load all files
        all_filepaths = list(all_json_files.values())
        records = _load_json_files_parallel(all_filepaths)

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)

    # Sort and save updated cache
    df = df.sort_values("timestamp", ascending=True)
    _save_cache(df, cache_path)

    # Return without source_file column (internal tracking only)
    return df.drop(columns=["source_file"])


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
