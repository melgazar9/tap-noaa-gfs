# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Singer tap (Meltano SDK) that extracts NOAA GFS weather forecast data from GRIB2 binary files. No authentication required — GFS is public data. This extends `singer_sdk.Stream` (not `RESTStream`) because it downloads and parses binary GRIB2 files rather than calling JSON REST endpoints.

**Purpose:** Weather-driven commodity trading — GFS forecast revisions drive natural gas and energy prices. Data integrity and completeness are critical for financial backtesting.

## Build & Development Commands

```bash
uv sync                                    # Install dependencies
uv run pytest                              # Run tests
uv run pytest tests/test_core.py -k test_name  # Run single test
uv run tap-noaa-gfs --config config.json --discover  # Discover streams
uv run tap-noaa-gfs --config config.json   # Run tap directly

# Meltano pipeline (primary validation method)
meltano install
meltano el tap-noaa-gfs target-jsonl

# Lint & format (via pre-commit)
uv run ruff check tap_noaa_gfs/
uv run ruff format tap_noaa_gfs/
```

**System dependency:** `eccodes` must be installed for cfgrib. macOS: `brew install eccodes`. Linux: `apt-get install libeccodes-dev`.

## Architecture

```
tap.py          → TapNoaaGfs config schema (14 settings), discover_streams()
client.py       → GFSBaseStream (throttle, backoff, download, parallel ThreadPoolExecutor)
                  ForecastRunsStream (probes run availability)
                  ForecastDataStream (main stream, partitioned by run_date × cycle)
grib_parser.py  → GribParser: GRIB2 → flat records via cfgrib/xarray + numpy vectorization
helpers.py      → URL builders (NOMADS, S3), date generation, variable filtering
streams.py      → Re-exports ForecastDataStream, ForecastRunsStream from client.py
```

### Data Flow

1. `ForecastDataStream.partitions` generates `(run_date, cycle)` pairs from config date range
2. `get_records(context)` calls `_download_and_yield_partition_records()` for each partition
3. Forecast hour files are downloaded **in parallel** via `ThreadPoolExecutor` (I/O-bound)
4. Each GRIB2 file is parsed by `GribParser` using cfgrib/xarray, filtered by bounding box and grid_step
5. 2D grids are flattened to records using **numpy vectorization** (not nested loops)
6. Records are yielded sequentially (Singer protocol requirement)

### Critical Design Decisions

- **NOMADS vs S3**: NOMADS supports server-side filtering (~40KB per file vs ~500MB unfiltered). Preferred for recent data (~10 day retention). S3 has data from 2021+.
- **Variable name mapping**: NOMADS uses names like `TMP`, `UGRD`. cfgrib uses ecCodes short names like `t2m`, `u10`. The `NOMADS_TO_CFGRIB_NAMES` dict in `grib_parser.py` bridges this. Any new variable added must have its cfgrib mapping verified with a test download.
- **APCP unavailable at fh=0**: Accumulated/averaged variables don't exist in the analysis file. `helpers._filter_variables_for_forecast_hour()` handles this.
- **Latitude direction**: Full GRIB2 files have descending lat (90→-90), but NOMADS-filtered subregions have ascending lat (24→50). The parser auto-detects direction.
- **S3 directory structure change**: Pre-June 2021 has no `atmos/` subdirectory. `helpers.build_s3_url()` handles this with `S3_ATMOS_SUBDIR_CUTOFF`.
- **Longitude convention**: GRIB2 uses 0–360; output converts to -180–180.

## Config Sync Rule

When changing tap configuration, always update these three files together:
1. `config_jsonschema` in `tap_noaa_gfs/tap.py`
2. `settings` block in `meltano.yml`
3. Environment variables in `.env.example`

## Data Source Details

| Source | Retention | Filtering | Resolution | Use Case |
|--------|-----------|-----------|------------|----------|
| NOMADS | ~10 days | Server-side (variable, level, region) | 0.25° | Recent data (preferred) |
| AWS S3 | 2021-01+ | None (full global files) | 0.25° | Historical backfill |

## Reference Taps

Study these sibling taps for Meltano SDK patterns:
- `../tap-fred/` — Thread-safe caching, sliding window throttle, point-in-time mode
- `../tap-massive/` — Error handling, field validation, state management
- `../tap-fmp/` — Time slicing for large date ranges

## Testing Approach

- **Smoke tests** (`tests/test_smoke.py`): 18 fast offline tests (no network), run as pre-commit hook. Cover tap init, schema validation, URL building, date range, variable filtering, cfgrib mapping coverage.
- **Pipeline validation**: Real `meltano el` runs are the primary validation. After code changes, run with varying configs and verify record counts match expected math (grid_points × variables × forecast_hours).
- **Pre-commit hooks**: ruff check/format, uv-lock, uv-sync, and smoke-test all run on commit.

## Energy Trading / ML Configuration

**Primary use case:** ML models predicting crude oil and natural gas prices.

### Recommended Variables (22)

| Category | Variables | Why |
|----------|-----------|-----|
| Temperature | TMP, TMAX, TMIN | HDD/CDD drive heating/cooling demand → natgas/power prices |
| Wind | UGRD, VGRD, GUST | Wind generation displaces gas-fired power; hub-height (80m/100m) for wind farms |
| Precipitation | APCP, PRATE | Hydro generation, flood risk to infrastructure |
| Moisture | RH, DPT, SPFH | Cooling load (humidity × temp), wet-bulb stress |
| Solar | DSWRF, USWRF | Solar generation displaces gas; net radiation for demand modeling |
| Cloud | TCDC | Cloud cover proxy for solar output |
| Pressure | PRMSL | Storm systems, shipping/pipeline disruption |
| Instability | CAPE, PWAT | Severe weather risk, demand spikes |
| Winter weather | CSNOW, CFRZR, WEASD | Freeze-offs shut in gas production; heating demand spikes |
| Upper air | HGT (500mb, 850mb) | Synoptic pattern identification (ridges/troughs) |

### Recommended Levels (9)

`2 m above ground`, `10 m above ground`, `80 m above ground`, `100 m above ground`, `surface`, `entire atmosphere (considered as a single layer)`, `mean sea level`, `500 mb`, `850 mb`

### Key Forecast Horizons for Trading

- **Day-ahead (fh 0-24):** Highest accuracy, spot price signals
- **Week-ahead (fh 24-168):** Storage/hedging decisions
- **2-week (fh 168-384):** Seasonal positioning, less accurate but moves markets on extreme signals

### Derived Features for ML

- **HDD/CDD:** `HDD = max(0, 65°F - T_avg)`, `CDD = max(0, T_avg - 65°F)` — population-weighted over demand regions
- **Forecast revisions:** Diff between consecutive model runs (same valid time, different run dates) — revision magnitude drives price moves
- **Wind power proxy:** Wind speed at 80m/100m cubed (power ∝ v³)
- **Ensemble spread:** If using GEFS ensemble members, spread indicates forecast uncertainty
