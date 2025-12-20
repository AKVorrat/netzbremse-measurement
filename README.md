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

> **Note:** We currently provide a prebuilt Docker image for x86_64 systems. To run on ARM devices (e.g., Raspberry Pi), you can clone this repository and build the image on the device.

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

## Configuration

| Variable | Default | Required |
|----------|---------|----------|
| `NB_SPEEDTEST_ACCEPT_POLICY` | - | **Yes** (set to `"true"`) |
| `NB_SPEEDTEST_INTERVAL` | `3600` (1 hour) | No |
| `NB_SPEEDTEST_URL` | `https://netzbremse.de/speed` | No |
| `NB_SPEEDTEST_BROWSER_DATA_DIR` | `./tmp-browser-data` | No |
| `NB_SPEEDTEST_JSON_OUT_DIR` | `undefined` | No |

## Local Result Storage
Edit the `docker-compose.yml` to include the environment variable and the volume mapping:

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
