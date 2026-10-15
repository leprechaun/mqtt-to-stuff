# mqtt-to-stuff

Personal home-automation pipeline: subscribes to an MQTT broker and archives
device telemetry into [Delta Lake](https://delta.io/) tables on S3-compatible
storage.

## Services

Three independent collectors, each a console script, each run as its own
container in a single Kubernetes pod (see `helm-charts/`):

| Script | Console script | What it does |
| --- | --- | --- |
| `mqtt_to_stuff/to_delta.py` | `hierarchy-to-deltalake` | Parses `devices/{zone}/{area}/{kind}/{thing}/...` topics (ESPHome plugs, presence sensors) into per-sensor Delta tables (`electricity`, `presence`, `habitat`, `iot_device_uptime`, `multi-presence`). |
| `mqtt_to_stuff/zigbee_to_delta.py` | `zigbee-to-deltalake` | Subscribes to `zigbee2mqtt/#`, matches devices by model ID (bulbs, buttons, sensors, etc.), writes one Delta table per device type plus a `zigbee-devices` mapping table. |
| `mqtt_to_stuff/raw_to_delta.py` | `raw-mqtt-to-delta` | Subscribes to everything (`#`) and archives the raw topic/payload as-is, for replay/debugging. Supports `--ignore` topic filters. |

Shared building blocks live in `mqtt_to_stuff/`: `devices.py`/`register.py`
(topic → sensor dispatch), `delta_client.py` (Delta Lake write + compaction),
`mqtt_helpers.py` (MQTT connect/subscribe helper).

## Requirements

- Python >= 3.11
- [uv](https://docs.astral.sh/uv/)

## Local development

```sh
uv sync --all-groups   # install deps + dev tools (pytest, ruff, marimo, altair)
uv run pytest          # run the test suite
uv run ruff check mqtt_to_stuff/ tests/   # lint
```

Running a collector locally:

```sh
uv run hierarchy-to-deltalake --host mqtt.local -t 'devices/#' -d /tmp/deltalake/
uv run zigbee-to-deltalake --host mqtt.local -d /tmp/deltalake/
uv run raw-mqtt-to-delta --host mqtt.local -d /tmp/deltalake/ --ignore 'zigbee2mqtt/#'
```

### Configuration

All three read these environment variables (CLI flags override them):

| Variable | Used by | Meaning |
| --- | --- | --- |
| `MQTT_HOST` | all | MQTT broker address |
| `MQTT_TOPIC` | `hierarchy-to-deltalake` | Topic filter to subscribe to (default `devices/#`) |
| `DELTA_PATH` | all | Base path/URI for Delta tables (default `/tmp/deltalake/`) |
| `INTERVAL` | all | Batch write interval in seconds |
| `AWS_ENDPOINT_URL_S3` | all | S3-compatible endpoint (e.g. MinIO) |
| `AWS_SESSION_TOKEN` | `zigbee-to-deltalake`, `raw-mqtt-to-delta` | STS session token, if used |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | all | Standard S3 credentials |

## Deployment

Built as a single Docker image (`Dockerfile`), deployed via the Helm chart in
`helm-charts/mqtt-to-stuff/` as one Deployment with all three collectors as
separate containers in the same pod. CI (`.github/workflows/pipeline.yml`)
runs tests, lint, and a chart lint before building and pushing the image.

## Notebooks

The root-level `*.nb.py` files are [marimo](https://marimo.io/) notebooks used
for ad hoc data exploration and one-off migrations against the Delta tables;
they're not part of the deployed pipeline.
