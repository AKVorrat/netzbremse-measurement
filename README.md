# Netzbremse Headless Speedtest

Automated speedtest runner using Puppeteer to periodically test peering bottlenecks from your Deutsche Telekom internet connection.

To learn more about the campaign go to our [website](https://netzbremse.de/en/) and try the [speedtest](https://netzbremse.de/en/speed) in the browser.

By running this test you are supporting our claim with anonymized real world measurements in accordance with the privacy policy.

## Quick Start using Docker

Download the [`docker-compose.yml`](https://raw.githubusercontent.com/AKVorrat/netzbremse-measurement/refs/heads/main/docker-compose.yml) file.

Read our privacy policy on the [website](https://netzbremse.de/speed) (visible when starting the speedtest for the first time) and edit the `docker-compose.yml` file to accept the [Cloudflare terms](https://www.cloudflare.com/de-de/privacypolicy/).

```yml
environment: 
  NB_SPEEDTEST_ACCEPT_POLICY: true
```

Start the container to enable periodic speedtests running in the background.

```bash
docker compose up -d
```

View the results with:

```bash
docker compose logs -f
```

Anonymized results are automatically submitted to our data collection service.

Pre-built Docker images are provided for:

- **amd64**
- **arm64** (including Raspberry Pi 3 and later)

It is also possible to build your own image for different architectures by cloning this repository and running:

```bash
docker compose -f docker-compose.build.yml build
```

## Data Visualization

At the moment this tool does not have a built-in solution for visualizing the measurements over time. Thankfully there is a community-provided [Streamlit dashboard](https://github.com/lwndp/netzbremse-dashboard/) by [@lwndp](https://github.com/lwndp) that runs as a separate container alongside the speedtest container.

To get started, clone this repository or download [`docker-compose.dashboard.yml`](https://raw.githubusercontent.com/AKVorrat/netzbremse-measurement/refs/heads/main/docker-compose.dashboard.yml) in addition to [`docker-compose.yml`](https://raw.githubusercontent.com/AKVorrat/netzbremse-measurement/refs/heads/main/docker-compose.yml) and save both files in the same folder. 

Use the following command to start both the speedtest and dashboard containers.

```bash
docker compose -f docker-compose.yml -f docker-compose.dashboard.yml up -d
```

After a few seconds the dashboard should be reachable under ```http://[your-systems-ip-address]:8501```.

> **⚠️ WARNING:** The default configuration opens a port and exposes the dashboard publicly. Please make sure you know what you are doing. 

To learn more about how to set up the dashboard go to https://github.com/lwndp/netzbremse-dashboard/. 

## Run using Node.js (without Docker)

Clone this repository:

```bash
git clone https://github.com/AKVorrat/netzbremse-measurement.git
```

Install dependencies and start the script:

```bash
npm install
export NB_SPEEDTEST_ACCEPT_POLICY="true"
npm start
```

To run the script reliably in the background, create a Systemd service or use a process manager like PM2.

> **Note:** The script is developed and tested on Linux. The instructions can probably be adapted to run the script on other platforms.

### Troubleshooting

**Using system-installed Chromium:**

If the Chrome browser bundled with Puppeteer doesn't work for some reason, you can use a separate version of Chrome or Chromium installed through your system's native package manager. 

```bash
sudo apt install chromium
```

Configure the Chromium binary path using the `PUPPETEER_EXECUTABLE_PATH` environment variable. Set `PUPPETEER_SKIP_DOWNLOAD` to `true` to skip downloading the bundled Chromium version entirely.

Note: The path may be different on your system.

```bash
export PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
npm start
```

## Configuration

| Variable | Default | Required |
|----------|---------|----------|
| `NB_SPEEDTEST_ACCEPT_POLICY` | - | **Yes** (set to `"true"`) |
| `NB_SPEEDTEST_INTERVAL` | `3600` (1 hour) | No |
| `NB_SPEEDTEST_TIMEOUT` | `3600` (1 hour) | No |
| `NB_SPEEDTEST_RETRY_INTERVAL` | `900` (15 minutes) | No |
| `NB_SPEEDTEST_RETRY_COUNT` | `3` | No |
| `NB_SPEEDTEST_URL` | `https://netzbremse.de/speed` | No |
| `NB_SPEEDTEST_BROWSER_DATA_DIR` | `./tmp-browser-data` | No |
| `NB_SPEEDTEST_JSON_OUT_DIR` | `undefined` | No |

**Timeout Configuration:** The `NB_SPEEDTEST_TIMEOUT` variable sets the maximum duration (in seconds) for each speedtest operation. This prevents the script from hanging indefinitely during failures.

**Error Handling:** The script implements a retry mechanism with configurable parameters:
- `NB_SPEEDTEST_RETRY_COUNT`: Maximum number of consecutive failures before exiting (default: 3)
- `NB_SPEEDTEST_RETRY_INTERVAL`: Delay between retries after failures (default: 15 minutes)
- After reaching the maximum retry count, the script will exit with code 1

**Oneshot Mode:** Set `NB_SPEEDTEST_INTERVAL=0` to run the speedtest once and exit (exit code 0 on success).

**Important:** When running in Docker, use restart policies like `restart: unless-stopped` in docker-compose.yml, or use a service manager like systemd to automatically restart the process after it exits due to consecutive failures.

## Local Result Storage

To store speedtest results locally as JSON files, edit the `docker-compose.yml` to include the environment variable and the volume mapping:

```yml
NB_SPEEDTEST_JSON_OUT_DIR: './json-results'

volumes:
  - ${NB_SPEEDTEST_JSON_OUT_DIR}:/app/json-results
```

## Building the Image

```bash
docker compose -f docker-compose.build.yml build
```

## Warning

You should monitor your system or at least periodically check system metrics while running this script.

The script launches a headless Chromium instance in the background. In some cases, orphaned browser processes may not be cleaned up properly, or the disk may fill up with leftover Chromium profile data.

*The author speaks from personal experience with similar scripts in the past.*
