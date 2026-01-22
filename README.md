# Netzbremse Headless Speedtest

Automated speedtest runner using Puppeteer to periodically test peering bottlenecks form your Deutsche Telekom internet connection.

To learn more about the campaign go to our [website](https://netzbremse.de) and try the [speedtest](https://netzbremse.de/speed) in the browser.

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

To run the script reliably in the background create a Systemd service or use as process manager like PM2.

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
| `NB_SPEEDTEST_TIMEOUT` | `900` (15 minutes) | No |
| `NB_SPEEDTEST_URL` | `https://netzbremse.de/speed` | No |
| `NB_SPEEDTEST_BROWSER_DATA_DIR` | `./tmp-browser-data` | No |
| `NB_SPEEDTEST_JSON_OUT_DIR` | `undefined` | No |

**Timeout Configuration:** The `NB_SPEEDTEST_TIMEOUT` variable sets the maximum duration (in seconds) for each speedtest operation. This prevents the script from hanging indefinitely during failures.

## Local Result Storage
Edit the `docker-compose.yml` to include the environment variable and the volume mapping:

```yml
NB_SPEEDTEST_JSON_OUT_DIR: './json-results'

volumes:
  - ${NB_SPEEDTEST_JSON_OUT_DIR}:/app/json-results
```

## Dashboard

A web-based dashboard is available to visualize your speedtest results. The dashboard auto-refreshes and provides interactive charts for download/upload speeds and latency metrics. It runs in a separate Docker container (./dashboard).

built by [@lwndp](https://github.com/lwndp)

**Quick start:**

Run both containers with

```bash
docker compose up -d
```

or 

```bash
docker compose up -d --build
```

to build from source.

The dashboard will be available at `http://localhost:8501`

**Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `/data` | Container path where JSON files are read from |
| `REFRESH_INTERVAL_SECONDS` | `3600` | How often to reload data. Since the main app collects data every hour, it probably makes sense to leave the default value |

The dashboard container automatically mounts the same `json-results` directory as the speedtest container via the volume configuration in `docker-compose.yml`. You typically don't need to change `DATA_DIR` unless you're using a custom setup.

## Building the Image

```bash
docker compose -f docker-compose.build.yml build
```

## Warning

You should monitor your system or at least periodically check system metrics while running this script.

The script launches a headless Chromium instance in the background. In some cases, orphaned browser processes may not be cleaned up properly, or the disk may fill up with leftover Chromium profile data.

*The author speaks from personal experience with similar scripts in the past.*
