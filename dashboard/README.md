# Netzbremse Dashboard - Development Docs

## Requirements

- Python 3.12
- uv
- Docker

## Quick Start

**Using Docker (recommended):**
```bash
docker compose up --build
```

Dashboard available at `http://localhost:8501`

**Local development:**

Install dependencies:
```bash
uv sync
```

Set environment variables:
```bash
export DATA_DIR="../json-results"
export REFRESH_INTERVAL_SECONDS="60"
````
**Note:** DATA_DIR should contain the raw output of the measurement app, i.e. json files.

Run the app:
```bash
# make sure you are in /dashboard
uv run streamlit run app/app.py
```

## Structure

```
app/
├── app.py              # Main application
├── charts.py           # Chart generation (Altair)
├── components.py       # Reusable UI components
└── data_loader.py      # Data loading & caching
```

## Configuration

- `DATA_DIR`: Path to directory with speedtest JSON files (default: `/data`)
- `REFRESH_INTERVAL_SECONDS`: Data cache TTL (default: `3600`)

## Key Features

- Auto-refresh with live countdown
- Date range filtering
- Interactive charts (pan/zoom)
- CSV export
- Data aggregation options