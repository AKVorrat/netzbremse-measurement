# Grafana Dashboard

Visualize speedtest results using **Grafana Alloy** + **Loki** + **Grafana**.

This assumes you already have Grafana, Loki, and Alloy running.

## How it works

1. The speedtest container writes JSON result files into the configured output directory
2. Alloy tails those files, extracts fields, and ships them to Loki
3. The Grafana dashboard queries Loki and visualizes the data

## Setup

> [!TIP]
> `/var/log/netzbremse` can be replaced with any path on the Host
> Important is the mapping into alloy and updating the path in the alloy config

### 1. Configure the output directory

Make sure the speedtest container writes its JSON results to `/var/log/netzbremse/`. Set the environment variable and volume in your `docker-compose.yml`:

```yaml
environment:
  NB_SPEEDTEST_JSON_OUT_DIR: './json-results'
volumes:
  - /var/log/netzbremse:/app/json-results
```

### 2. Configure Alloy

Make sure Alloy has access to log directory. If Alloy runs in a container, mount the same directory:

```yaml
volumes:
  - /var/log/netzbremse:/var/log/netzbremse:ro
```

Add the following to your Alloy configuration and adjust the Loki endpoints to match your setup:

```alloy
// Get the logs from Netzbremse and extract
local.file_match "netzbremse" {
        path_targets = [{
                __address__ = "localhost",
                __path__    = "/var/log/netzbremse/*",
                job         = "netzbremse",
        }]
}

loki.process "netzbremse" {
        forward_to = [loki.write.loki.receiver]

        stage.regex {
                source     = "filename"
                expression = "speedtest-(?P<speedtest_date>\\d{4}-\\d{2}-\\d{2}T\\d{2}-\\d{2}-\\d{2})-\\d+Z\\.json"
        }

        stage.timestamp {
                source   = "speedtest_date"
                format   = "2006-01-02T15-04-05"
                location = "UTC"
        }

        stage.multiline {
                firstline     = "^{"
                max_wait_time = "3s"
        }

        stage.json {
                expressions = {
                        sessionID = "sessionID",
                        endpoint  = "endpoint",
                        success   = "success",
                        result    = "result",
                }
        }

        stage.json {
                source      = "result"
                expressions = {
                        download          = "download",
                        upload            = "upload",
                        latency           = "latency",
                        jitter            = "jitter",
                        downLoadedLatency = "downLoadedLatency",
                        downLoadedJitter  = "downLoadedJitter",
                        upLoadedLatency   = "upLoadedLatency",
                        upLoadedJitter    = "upLoadedJitter",
                }
        }

        stage.labels {
                values = {
                        endpoint = "endpoint",
                        success  = "success",
                }
        }

        stage.structured_metadata {
                values = {
                        sessionID = "sessionID",
                }
        }
}

loki.source.file "netzbremse" {
        targets    = local.file_match.netzbremse.targets
        forward_to = [loki.process.netzbremse.receiver]
}

// If not configured yet: add a Loki write target and reference it in loki.process.netzbremse above
loki.write "loki" {
        endpoint {
                url = "http://your-loki-host:3100/loki/api/v1/push"
        }
}
```

Reload Alloy after applying the config.

### 3. Import the Grafana dashboard

In Grafana, go to **Dashboards > Import** and either:
- Enter the dashboard ID: `25163`
- Or paste the URL: `https://grafana.com/grafana/dashboards/25163`

Select your Loki data source when prompted and hit **Import**.

## Screenshot

<img width="1632" height="847" alt="Screenshot from 2026-04-14 10-56-10" src="https://github.com/user-attachments/assets/839361f8-0435-45ce-bb5f-1f73f15b5b88" />
