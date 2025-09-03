# Bombeiros Sertã Monitor (Go)

Watches active incidents (GeoJSON) from Fogos.pt and filters them by municipalities (Sertã and nearby by default). Sends optional push alerts via ntfy and exports Prometheus metrics.

## Features

- Polls Fogos.pt “active incidents” API (GeoJSON)
- Municipality filtering with accent/whitespace normalization and common synonyms
- Additional filters: districts, regions, subregions, parishes; include-by-nature; exclude-by-status
- Optional radius filter around a center coordinate
- De-duplication across restarts using a local state file
- ntfy notifications with tags, priority, quiet hours, and dry-run mode
- Optional aggregation: send a single summary when many new incidents appear in one cycle
- Rich notifications: nature, status, and a Click link to Google Maps (coords or municipality search)
- Conditional HTTP caching (ETag/Last-Modified) to avoid unnecessary downloads (304)
- Graceful shutdown on Ctrl+C/SIGTERM after finishing the current cycle
- Prometheus metrics for current counts and status dynamics

## Requirements

- Go 1.22+
- Windows (also works on Linux/macOS)

## Quick build

PowerShell:

```powershell
Set-Location E:\bombeiros-serta
go build -o bin/monitor.exe ./cmd/monitor
```

CMD:

```bat
cd /d E:\bombeiros-serta
go build -o bin\monitor.exe .\cmd\monitor
```

## Run

- One-off run (no polling):

PowerShell:

```powershell
$env:POLL_SECONDS = '0'
& .\bin\monitor.exe
```

CMD:

```bat
set POLL_SECONDS=0 && bin\monitor.exe
```

- Continuous run (e.g., 60s; default is 30s):

PowerShell:

```powershell
$env:NTFY_TOPIC = 'bombeiros-serta'
$env:POLL_SECONDS = '60'
& .\bin\monitor.exe
```

CMD:

```bat
set NTFY_TOPIC=bombeiros-serta && set POLL_SECONDS=60 && bin\monitor.exe
```

## Configuration (environment variables)

Core

- MUNICIPIOS: Comma- or semicolon-separated list of municipality names to monitor. Example:
  - PowerShell: `$env:MUNICIPIOS = 'Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova'`
  - CMD: `set MUNICIPIOS=Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova`
- POLL_SECONDS: Poll interval in seconds (0 runs once and exits)
- STATE_FILE: Path to the state file (default: `last_ids.json`)
- STATE_TTL_HOURS: Optional TTL to prune old IDs from state (e.g., `72`)

Fogos API

- FOGOS_URL: Active incidents endpoint (default: `https://api.fogos.pt/v2/incidents/active?geojson=true`)
- FOGOS_FALLBACK_URLS: Optional comma/semicolon list of fallback endpoints
- FOGOS_API_KEY: Optional API token if required by the endpoint

Filtering (admin units / attributes)

- DISTRICTS: Case-insensitive list of district names
- REGIOES: Case-insensitive list of regions (NUTS2/3 as provided by the API)
- SUBREGIOES: Case-insensitive list of subregions
- FREGUESIAS: Case-insensitive list of parish names
- INCLUDE_NATUREZA: Case-insensitive list of nature strings (substring match)
- EXCLUDE_STATUS_CODES: Comma-separated integer codes to exclude

Radius filter (optional)

- CENTER_LAT, CENTER_LON: Center point in decimal degrees
- RADIUS_KM: Radius in kilometers (enabled if > 0)

ntfy (notifications)

- NTFY_URL: Base ntfy URL (default: `https://ntfy.sh`)
- NTFY_TOPIC: Topic to publish to (default: `bombeiros-serta`)
- NTFY_PRIORITY: ntfy priority 1–5 (default: `5`)
- NTFY_TAGS: Comma list of tags/emojis (default: `fire,rotating_light`)
- NTFY_DRYRUN: If set, do not post to ntfy; log the message only
- NTFY_SUMMARY_THRESHOLD: If > 0, send an aggregated summary when the number of new incidents in a cycle meets/exceeds the threshold (e.g., `5`)
- QUIET_HOURS: Quiet window `start-end` in 24h format (e.g., `23-7`); lowers priority and adds a `zzz` tag
- NTFY_TEST: If set, sends a test notification on startup

Logging & Metrics

- DEBUG or LOG_LEVEL=debug: Enable debug logs
- METRICS_DISABLE: If set, disables metrics export
- SUMMARY_HOURLY / SUMMARY_DAILY: Set to `0` to disable hourly/daily summaries

Optional means-based enrichment

- MIN_MAN, MIN_TERRAIN, MIN_AERIAL, MIN_AQUATIC: Integer thresholds that add tags and bump priority when exceeded

## State file

The file `last_ids.json` stores, per normalized municipality, the IDs that were already notified to avoid duplicates across restarts. It also records timestamps such as first seen and concluded when available, and the last known status per incident.

## Metrics

Exports Prometheus metrics (when not disabled):

- bombeiros_active_incidents (gauge) with labels like district/concelho/regiao/natureza/status
- bombeiros_status_transitions_total (counter)
- bombeiros_time_to_conclusion_seconds (histogram)

An HTTP `/metrics` endpoint is exposed when metrics are enabled. Check the startup log to see where it is served.

## Notes & behavior

- Empty responses from the API (0 incidents) are treated as valid
- Google Maps “Click” link is included when coordinates are available, otherwise a municipality search link is used
- Municipality names are normalized (accents/spacing removed) and common synonyms are recognized
- Uses friendly HTTP headers and conditional GET (ETag/Last-Modified) to reduce bandwidth
- Gracefully handles Ctrl+C/SIGTERM: completes the current cycle before exiting

## Project layout

- `cmd/monitor/main.go` – Application entry point
- `last_ids.json` – State file (created/updated at runtime)
- `monitor.exe` – Binary (if you build to project root)

## Disclaimer

This is a lightweight, single-binary tool intended to run 24/7 (task scheduler/service) or on-demand. Always verify incident information with official sources.
