# Bombeiros Sertã Monitor (Go)

Monitors active incidents from Fogos.pt, filters by municipalities (Sertã and nearby ones by default), and sends ntfy push alerts with hourly/daily summaries and Prometheus metrics. On Windows it runs as a system tray app by default.

## What it does now

- Periodically polls the Fogos v2 API (current endpoint: `https://api-dev.fogos.pt/v2/incidents/active?all=1`). Accepts multiple response shapes (GeoJSON FeatureCollection or plain objects) and extracts coordinates when available.
- Municipality filtering with normalization (accents/spacing) and common synonyms.
- Additional filters by admin units and attributes:
  - district, region, sub‑region, parish
  - include/exclude by nature (name) and by `naturezaCode`
  - include/exclude by status (name) and exclude by numeric `statusCode`
- Optional radius filter around a center point (km).
- De‑dup across restarts using a state file; optional TTL retention and cleanup of finished items.
- Rich ntfy notifications: new incident, status transitions, means changes, “extra” field changes, and reactivations; includes actions “Open Map” and “Open Fogos” (fires only) and can attach the area file.
- Dynamic tags and priority based on means counts (man/terrain/aerial/aquatic) and aircraft; quiet hours lower priority and add `zzz`; dry‑run mode.
- Summaries:
  - Per‑cycle aggregation of new incidents when a configurable threshold is reached
  - Hourly summary (once per hour at minute 00)
  - Daily summary (once per day at 08:00)
- KML (VOST): optionally saves KML, computes area/perimeter, and includes a `file://` URL to open it.
- Prometheus metrics: current counts and status dynamics (counter/histogram) at `http://localhost:2112/metrics` (configurable port).
- Windows tray by default: hides the console, tray icon with “Quit”. Ctrl+C/SIGTERM works gracefully in console mode.

Note: Conditional HTTP caching via ETag/Last‑Modified was removed.

## Requirements

- Go 1.23+
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

- One‑off run (no polling). On Windows, disable tray so it exits after the run:

PowerShell:

```powershell
$env:USE_TRAY = '0'; $env:POLL_SECONDS = '0'
& .\bin\monitor.exe
```

CMD:

```bat
set USE_TRAY=0 && set POLL_SECONDS=0 && bin\monitor.exe
```

- Continuous run (e.g., every 60s; default is 30s):

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

On Windows, tray mode is enabled by default (`USE_TRAY=1`). Set `USE_TRAY=0` to run in a console window.

## Configuration (environment variables)

Core

- MUNICIPIOS or MUNICIPIO: comma/semicolon‑separated list. Examples:
  - PowerShell: `$env:MUNICIPIOS = 'Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova'`
  - CMD: `set MUNICIPIOS=Sertã,Oleiros,Castanheira de Pera,Proença-a-Nova`
- POLL_SECONDS: interval in seconds (0 runs once and exits)
- USE_TRAY: on Windows, 1=tray (default), 0=console
- STATE_FILE: path to the state file (default: `last_ids.json`)
- STATE_TTL_HOURS: optional TTL to prune old IDs (e.g., `72`)
- CLEAN_FINISHED: if not `0`, removes IDs no longer active (default: `1`)

Default municipalities (when `MUNICIPIOS` is not set):

- Sertã, Oleiros, Castanheira de Pera, Proença‑a‑Nova, Vila de Rei, Vila Velha de Ródão,
  Sardoal, Figueiró dos Vinhos, Pedrógão Grande, Pampilhosa da Serra, Ferreira do Zêzere,
  Fundão, Castelo Branco, Idanha‑a‑Nova, Penamacor, Belmonte, Covilhã

Fogos API

- Endpoint is fixed in code (`api-dev.fogos.pt/v2/incidents/active?all=1`)
- FOGOS_API_KEY: optional token (added as `Authorization: Bearer`)

Filters (admin units / attributes)

- DISTRICTS, REGIOES, SUBREGIOES, FREGUESIAS: case‑insensitive lists
- INCLUDE_NATUREZA: by name (substring allowed)
- INCLUDE_NATUREZA_CODE / EXCLUDE_NATUREZA_CODE: by code (e.g., `3101`)
- INCLUDE_STATUS / EXCLUDE_STATUS: by status name (substring allowed)
- EXCLUDE_STATUS_CODES: list of numeric codes

Radius filter (optional)

- CENTER_LAT, CENTER_LON: decimal degrees
- RADIUS_KM: radius in km (enabled if > 0)

ntfy (notifications)

- NTFY_URL: base (default: `https://ntfy.sh`)
- NTFY_TOPIC: topic (default: `bombeiros-serta`)
- NTFY_PRIORITY: 1–5 (default: `5`)
- NTFY_TAGS: CSV of tags/emojis (default: `fire,rotating_light`)
- NTFY_DRYRUN: if set, do not post; log only
- NTFY_SUMMARY_THRESHOLD: if > 0, send aggregated summary when new incidents in a cycle ≥ threshold
- QUIET_HOURS: window `start-end` (24h, e.g., `23-7`); lowers priority and adds `zzz`
- NTFY_TEST: if set, sends a test notification on startup
- NTFY_JSON: publish in JSON mode (otherwise header‑based)
- NTFY_MARKDOWN: enable markdown
- NTFY_ICON_URL, NTFY_EMAIL, NTFY_CACHE, NTFY_FIREBASE, NTFY_ACTIONS (default `1`), NTFY_ATTACH_AREA, NTFY_CLICK_GEO
- MIN_MAN, MIN_TERRAIN, MIN_AERIAL, MIN_AQUATIC: thresholds that add tags and bump priority
- NOTIFY_MEANS_CHANGES (default `1`), NOTIFY_EXTRA_CHANGES (default `1`)
- SUMMARY_HOURLY (default `1`), SUMMARY_DAILY (default `1`)

KML (optional)

- SAVE_KML_DIR: directory to save KML and compute area/perimeter (adds `file://` URL to notification)

Logging & Metrics

- DEBUG or LOG_LEVEL=debug: enable debug logging
- METRICS_DISABLE: if set, disables metrics
- METRICS_ADDR: addr/port for the metrics server (default: `:2112`), endpoint `/metrics`

## State file

Default is `last_ids.json`. It stores, per canonical municipality, active IDs and extra info per ID: `status`, timestamps `first`/`concluded`, `means`, `extra_text` and the hour/day marks `last_hourly`/`last_daily`. It’s updated automatically; no manual editing required.

## Metrics

Exports Prometheus metrics (when not disabled):

- bombeiros_active_incidents (gauge) with labels district/concelho/regiao/natureza/status
- bombeiros_status_transitions_total (counter)
- bombeiros_time_to_conclusion_seconds (histogram)

The HTTP `/metrics` endpoint is exposed when metrics are enabled. Check the startup output for the address.

## Notes & behavior

- Empty API responses (0 incidents) are valid.
- Google Maps “Click” link uses coordinates when present; otherwise falls back to a municipality search.
- Municipality names are normalized (accents/spaces removed) and common synonyms are recognized.
- Uses friendly HTTP headers. Conditional GET (ETag/Last‑Modified) is not used anymore.
- Graceful shutdown on Ctrl+C/SIGTERM: finishes the current cycle before exiting.

## Project layout

- `cmd/monitor/main.go` – Application entry point
- `last_ids.json` – State file (created/updated at runtime)
- `monitor.exe` – Binary (if you build to project root)

## Disclaimer

This is a lightweight, single‑binary tool intended to run 24/7 (task scheduler/service) or on‑demand. Always verify incident information with official sources.
