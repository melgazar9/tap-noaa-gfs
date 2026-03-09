# Prompt: Build `tap-noaa-gfs` — A Singer Tap for NOAA GFS Weather Forecast Model Data

## Goal
Build a production-grade Singer tap (Meltano SDK) that extracts historical weather forecast data from the NOAA Global Forecast System (GFS). GFS is the primary U.S. deterministic weather forecast model, producing 4 forecast runs per day (00z, 06z, 12z, 18z) with forecasts out to 384 hours. This data is the core signal for weather-driven commodity trading — specifically, forecast revisions drive natural gas and energy prices.

**The key challenge:** GFS data is stored as GRIB2 binary files, NOT JSON. This tap must download GRIB2 files, parse them using `cfgrib`/`xarray`, extract relevant variables (temperature, wind, precipitation) for relevant geographic areas, aggregate them into tabular records, and emit as Singer JSONL.

This is an extractor ONLY — no loader logic.

---

## Reference Codebases (MUST study these first)

Before writing any code, read and understand these existing taps:

1. **tap-fred** at `~/code/github/personal/tap-fred/` — Study:
   - `tap_fred/tap.py` — Config schema, thread-safe caching, wildcard discovery
   - `tap_fred/client.py` — Base stream hierarchy, `_make_request()` with backoff, `_throttle()`, `_safe_partition_extraction()`, `post_process()`, pagination
   - `tap_fred/helpers.py` — Utilities

2. **tap-massive** at `~/code/github/personal/tap-massive/` — Study:
   - `tap_massive/client.py` — `_check_missing_fields()`, error handling, state management
   - `tap_massive/tap.py` — Thread-safe caching pattern

3. **tap-fmp** at `~/code/github/personal/tap-fmp/` — Study:
   - `tap_fmp/client.py` — Time slicing pattern for large date ranges

**Match the Meltano SDK patterns** — same `singer-sdk` version, same base class style, same config/state/error handling. The difference is that this tap downloads files from HTTP/S3 instead of calling REST JSON endpoints.

---

## GFS Data Architecture

### What GFS Is
- Deterministic global weather forecast model run by NCEP (National Centers for Environmental Prediction)
- Runs 4 times daily: 00z, 06z, 12z, 18z UTC (called "cycles")
- Each run produces forecasts for hours 0 through 384 (16 days out)
- Forecast hours: 0,1,2,3,...120 (hourly), then 123,126,...384 (3-hourly)
- Resolution: 0.25° × 0.25° global grid (~1M grid points per variable per forecast hour)
- Variables: temperature, wind, precipitation, humidity, pressure, etc.

### Data Sources (in order of preference)

**Option A: AWS S3 Open Data (PREFERRED — easiest bulk access)**
```
s3://noaa-gfs-bdp-pds/
```
- Web index: https://noaa-gfs-bdp-pds.s3.amazonaws.com/index.html
- Structure: `gfs.YYYYMMDD/HH/atmos/gfs.tHHz.pgrb2.0p25.fFFF`
  - `YYYYMMDD` = model run date
  - `HH` = cycle hour (00, 06, 12, 18)
  - `FFF` = forecast hour (000, 001, ..., 384)
- Files are GRIB2 format
- **No authentication required** — public bucket
- **Date range available**: approximately last 30-60 days only (rolling window)
- To get older data, need NCEI archive

**Option B: NOAA NOMADS (real-time + filtered access)**
```
https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl
```
- Allows server-side filtering by variable, level, and geographic region
- Parameters:
  - `file=gfs.t00z.pgrb2.0p25.f000` — specific file
  - `dir=/gfs.20240101/00/atmos` — directory
  - `var_TMP=on` — include temperature
  - `var_UGRD=on` — include U-wind component
  - `var_VGRD=on` — include V-wind component
  - `var_RH=on` — include relative humidity
  - `var_APCP=on` — include accumulated precipitation
  - `lev_2_m_above_ground=on` — 2m temperature (surface)
  - `lev_10_m_above_ground=on` — 10m wind
  - `subregion=` — geographic bounding box (leftlon, rightlon, toplat, bottomlat)
- **This is strongly preferred** because you can download ONLY the variables and region you need, reducing file sizes from ~300MB to ~5MB per request
- **Date range**: approximately last 10 days only

**Option C: NCEI Archive (historical — for backfill)**
```
https://www.ncei.noaa.gov/data/global-forecast-system/access/historical/analysis/
```
- Has data going back years
- Full GRIB2 files (no server-side filtering)
- Slower access

**Option D: Google Cloud (alternative archive)**
```
gs://gcp-public-data-weather/gfs/
```

### For This Tap: Use NOMADS as Primary, AWS S3 as Fallback
- NOMADS allows filtering — much less data to download
- For historical backfill beyond NOMADS retention, fall back to NCEI archive

---

## Strategy: What to Extract

**We do NOT need the full global grid.** For natural gas / energy trading, we need:

### Variables to Extract
| Variable | GRIB2 Name | Level | Why |
|----------|-----------|-------|-----|
| Temperature | TMP | 2m above ground | HDD/CDD computation |
| U-Wind | UGRD | 10m above ground | Wind chill, wind generation |
| V-Wind | VGRD | 10m above ground | Wind chill, wind generation |
| Precipitation | APCP | surface | Rain/snow accumulation |
| Relative Humidity | RH | 2m above ground | Heat index, comfort |

### Geographic Regions (for population-weighted aggregation later)
Extract for the Continental US (CONUS) bounding box:
- Latitude: 24°N to 50°N
- Longitude: 125°W to 66°W (i.e., -125 to -66)

**Two output granularity options:**

**Option 1 (Simpler — RECOMMENDED for MVP): Grid-point level**
- Emit one record per (run_date, cycle, forecast_hour, latitude, longitude, variable)
- Let downstream (dbt/Python) handle population weighting
- More flexible but more data

**Option 2 (Pre-aggregated): Regional averages**
- Define regions (East, Midwest, South, Texas, West)
- Compute population-weighted average per region in the tap
- Less data but bakes in aggregation assumptions

**Start with Option 1.** It's simpler and more flexible.

BUT: even CONUS at 0.25° resolution is ~26,000 grid points × 5 variables × ~200 forecast hours × 4 cycles/day = very large. We need to be selective.

**Practical approach: Extract only key forecast hours, not all 384.**
- Hours 0, 6, 12, 18, 24, 48, 72, 96, 120, 168, 240, 336 (12 horizons)
- This gives you current conditions + 1/2/3/4/5/7/10/14 day forecasts
- Configurable via `forecast_hours` setting

---

## Directory Structure

```
tap-noaa-gfs/
├── tap_noaa_gfs/
│   ├── __init__.py
│   ├── __main__.py            # TapNOAAGFS.cli()
│   ├── tap.py                 # Main Tap class, config, stream registration
│   ├── client.py              # GFSStream base class, GRIB2 download/parse, throttling
│   ├── grib_parser.py         # GRIB2 parsing with cfgrib/xarray, variable extraction
│   ├── helpers.py             # Grid utilities, coordinate helpers
│   └── streams/
│       ├── __init__.py
│       ├── inventory_streams.py  # Available runs/files discovery
│       └── forecast_streams.py   # Actual forecast data extraction
├── tests/
│   ├── __init__.py
│   └── test_core.py
├── pyproject.toml
├── meltano.yml
└── README.md
```

---

## Implementation Requirements

### 1. Tap Class (`tap.py`)

```python
class TapNOAAGFS(Tap):
    name = "tap-noaa-gfs"
```

**Config Properties:**
- `source` (StringType, default "nomads") — Data source: "nomads", "aws_s3", "ncei"
- `nomads_url` (StringType, default "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl")
- `aws_s3_bucket` (StringType, default "noaa-gfs-bdp-pds")
- `start_date` (DateType, required) — First forecast run date to extract
- `end_date` (DateType, optional) — Last date. Default: today.
- `cycles` (ArrayType(StringType), default ["00", "06", "12", "18"]) — Which cycles. `["00", "12"]` for twice-daily.
- `forecast_hours` (ArrayType(IntegerType), default [0, 6, 12, 18, 24, 48, 72, 96, 120, 168, 240, 336]) — Which forecast hours to extract
- `variables` (ArrayType(StringType), default ["TMP", "UGRD", "VGRD", "APCP", "RH"]) — GRIB2 variable names
- `levels` (ArrayType(StringType), default ["2 m above ground", "10 m above ground", "surface"]) — Pressure levels
- `bounding_box` (ObjectType, default {"north": 50, "south": 24, "west": -125, "east": -66}) — Geographic filter (CONUS default)
- `output_format` (StringType, default "gridpoint") — "gridpoint" (one record per lat/lon/variable) or "regional" (pre-aggregated)
- `max_concurrent_downloads` (IntegerType, default 2) — Parallel GRIB2 downloads
- `max_requests_per_minute` (IntegerType, default 20) — Rate limit for NOMADS
- `temp_dir` (StringType, optional) — Where to store temporary GRIB2 files. Default: system temp.
- `strict_mode` (BooleanType, default False)

**Thread-Safe Caching:**
- `get_available_runs(start_date, end_date)` — Check which (date, cycle) combinations have data available. For NOMADS/AWS, probe directory listings. Return list of `{"run_date": "2024-01-15", "cycle": "00"}`.

### 2. GRIB2 Parser (`grib_parser.py`)

This is the unique component — no other tap has this.

```python
import xarray as xr
import cfgrib

class GRIBParser:
    """Parse GRIB2 files and extract variables as tabular records."""

    def parse_grib_file(
        self,
        file_path: str,
        variables: list[str],
        bounding_box: dict,
        run_date: str,
        cycle: str,
        forecast_hour: int,
    ) -> list[dict]:
        """
        Parse a GRIB2 file and return flat records.

        Returns list of dicts like:
        {
            "run_date": "2024-01-15",
            "cycle": "00",
            "forecast_hour": 24,
            "valid_time": "2024-01-16T00:00:00Z",
            "latitude": 40.0,
            "longitude": -74.0,
            "variable": "TMP",
            "level": "2 m above ground",
            "value": 275.15,
            "units": "K"
        }
        """
        datasets = cfgrib.open_datasets(file_path)

        records = []
        for ds in datasets:
            for var_name in ds.data_vars:
                if var_name not in variables:
                    continue

                da = ds[var_name]

                # Apply bounding box filter
                da = da.sel(
                    latitude=slice(bounding_box["north"], bounding_box["south"]),
                    longitude=slice(
                        bounding_box["west"] % 360,  # GRIB uses 0-360 longitude
                        bounding_box["east"] % 360
                    )
                )

                # Extract level info
                level = self._get_level_string(ds, var_name)

                # Convert to records
                for lat in da.latitude.values:
                    for lon in da.longitude.values:
                        val = float(da.sel(latitude=lat, longitude=lon).values)
                        records.append({
                            "run_date": run_date,
                            "cycle": cycle,
                            "forecast_hour": forecast_hour,
                            "valid_time": self._compute_valid_time(run_date, cycle, forecast_hour),
                            "latitude": round(float(lat), 4),
                            "longitude": round(float(lon) - 360 if float(lon) > 180 else float(lon), 4),
                            "variable": var_name,
                            "level": level,
                            "value": val,
                            "units": da.attrs.get("units", "unknown"),
                        })

        return records
```

**CRITICAL NOTES about GRIB2 parsing:**
1. `cfgrib.open_datasets()` returns MULTIPLE datasets (one per level type). You must iterate all.
2. GRIB2 uses 0-360 longitude, NOT -180 to 180. Convert: `lon_standard = lon - 360 if lon > 180 else lon`
3. Latitude in GRIB2 goes from 90 to -90 (north to south). `slice(north, south)` works because of this.
4. Temperature is in Kelvin. Do NOT convert here — let downstream handle it. Just pass `units: "K"`.
5. Accumulated precipitation (APCP) may be cumulative within a forecast run. Document this.
6. Some variables may not exist in every GRIB2 file. Handle `KeyError` gracefully.
7. Delete temp files after parsing to avoid disk bloat.

### 3. Base Stream Class (`client.py`)

**`GFSStream(Stream, ABC)`** — Note: This extends `Stream`, NOT `RESTStream`, because we're downloading files, not calling REST endpoints.

```python
from singer_sdk import Stream

class GFSStream(Stream, ABC):
    """Base class for GFS streams. Downloads GRIB2 files instead of REST calls."""
```

**File Download with Retry:**
```python
@backoff.on_exception(backoff.expo, (RequestException, IOError), max_tries=5, max_time=300)
def _download_grib_file(self, url: str, dest_path: str) -> str:
    """Download a GRIB2 file from NOMADS or S3."""
    self._throttle()
    response = requests.get(url, stream=True, timeout=(30, 300))
    response.raise_for_status()
    with open(dest_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return dest_path
```

**NOMADS URL Builder:**
```python
def _build_nomads_url(self, run_date: str, cycle: str, forecast_hour: int) -> str:
    """Build NOMADS filter URL for specific variables and region."""
    base = self.config["nomads_url"]
    params = {
        "file": f"gfs.t{cycle}z.pgrb2.0p25.f{forecast_hour:03d}",
        "dir": f"/gfs.{run_date.replace('-','')}/{cycle}/atmos",
    }
    # Add variable filters
    for var in self.config.get("variables", ["TMP"]):
        params[f"var_{var}"] = "on"
    # Add level filters
    for level in self.config.get("levels", []):
        level_key = f"lev_{level.replace(' ', '_')}"
        params[level_key] = "on"
    # Add geographic filter
    bb = self.config.get("bounding_box", {})
    if bb:
        params["subregion"] = ""
        params["leftlon"] = bb.get("west", -125)
        params["rightlon"] = bb.get("east", -66)
        params["toplat"] = bb.get("north", 50)
        params["bottomlat"] = bb.get("south", 24)
    return base + "?" + urlencode(params)
```

**S3 URL Builder (fallback):**
```python
def _build_s3_url(self, run_date: str, cycle: str, forecast_hour: int) -> str:
    bucket = self.config["aws_s3_bucket"]
    date_str = run_date.replace("-", "")
    return f"https://{bucket}.s3.amazonaws.com/gfs.{date_str}/{cycle}/atmos/gfs.t{cycle}z.pgrb2.0p25.f{forecast_hour:03d}"
```

### 4. Parallel Download Architecture

**Critical optimization**: Within a single `get_records()` call for a (run_date, cycle) partition, download multiple forecast hour files concurrently using `ThreadPoolExecutor`. The Singer protocol only requires records to be **yielded** sequentially — it doesn't care how you get the data internally.

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_records(self, context):
    """Download forecast hour files in parallel, yield records sequentially."""
    run_date = context["run_date"]
    cycle = context["cycle"]
    forecast_hours = self.config.get("forecast_hours", [0, 24, 48, 72, 120])

    # Build all URLs for this run
    download_tasks = [
        {"url": self._build_url(run_date, cycle, fh), "forecast_hour": fh}
        for fh in forecast_hours
    ]

    max_workers = self.config.get("max_concurrent_downloads", 10)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(self._download_and_parse, task["url"], run_date, cycle, task["forecast_hour"]): task
            for task in download_tasks
        }
        for future in as_completed(futures):
            task = futures[future]
            try:
                records = future.result()
                for record in records:
                    yield record  # Single-threaded yield — Singer is happy
            except Exception as e:
                logging.warning(f"Failed forecast hour {task['forecast_hour']}: {e}")
                if self.config.get("strict_mode"):
                    raise

def _download_and_parse(self, url, run_date, cycle, forecast_hour):
    """Download one GRIB2 file, parse it, return records, delete temp file."""
    dest = os.path.join(self._temp_dir, f"gfs_{run_date}_{cycle}_f{forecast_hour:03d}.grib2")
    try:
        self._download_grib_file(url, dest)
        parser = GRIBParser()
        return parser.parse_grib_file(dest, self.config["variables"], self.config["bounding_box"],
                                       run_date, cycle, forecast_hour)
    finally:
        if os.path.exists(dest):
            os.remove(dest)
```

**Why this works**: Each GFS run has ~385 forecast hour files. Downloading them one at a time is I/O-bound and slow. Downloading 10 concurrently saturates the network pipe. The CPU-bound GRIB2 parsing happens per file as it lands. Yielding records is sequential (Singer requirement). This gives you ~10x speedup on downloads without fighting the Singer framework.

### 5. Streams

**Stream 1: `ForecastRunsStream`** — Available forecast runs
- Discovers which (run_date, cycle) combinations exist
- For NOMADS: probe directory listings
- For S3: list bucket prefixes
- Schema: run_date, cycle, status ("available" / "missing")
- Useful for data quality monitoring

**Stream 2: `ForecastDataStream`** — THE MAIN STREAM
- **Partitioned by: (run_date, cycle)** — NOT by forecast_hour. Forecast hours are downloaded in parallel within the partition (see Section 4 above).
- For each partition:
  1. Build URLs for ALL configured forecast hours
  2. Download in parallel via ThreadPoolExecutor
  3. Parse each with GRIBParser as it completes
  4. Yield records sequentially
  5. Delete temp files
- Primary keys: `["run_date", "cycle", "forecast_hour", "latitude", "longitude", "variable"]`
- Replication key: `"run_date"` — for incremental sync (don't re-download old runs)
- Schema:
  ```python
  schema = th.PropertiesList(
      th.Property("run_date", th.StringType, required=True),
      th.Property("cycle", th.StringType, required=True),       # "00","06","12","18"
      th.Property("forecast_hour", th.IntegerType, required=True),
      th.Property("valid_time", th.DateTimeType, required=True), # run_date + cycle + forecast_hour
      th.Property("latitude", th.NumberType, required=True),
      th.Property("longitude", th.NumberType, required=True),
      th.Property("variable", th.StringType, required=True),     # "TMP","UGRD", etc.
      th.Property("level", th.StringType),                       # "2 m above ground"
      th.Property("value", th.NumberType),
      th.Property("units", th.StringType),                       # "K", "m s**-1", etc.
  ).to_dict()
  ```

**Partition generation (by run_date × cycle only — forecast hours are parallelized internally):**
```python
@property
def partitions(self):
    partitions = []
    for run_date in self._generate_dates(self.config["start_date"], self.config.get("end_date")):
        for cycle in self.config.get("cycles", ["00", "06", "12", "18"]):
            partitions.append({
                "run_date": run_date,
                "cycle": cycle,
            })
    return partitions
```

### 5. Key Considerations

**Data Volume:**
- Even with NOMADS filtering (CONUS only, 5 variables, selected levels), each GRIB2 file is ~5-20MB
- With 4 cycles/day × 12 forecast hours × 365 days = ~17,520 files per year
- At ~10MB average = ~175GB per year of raw downloads
- After parsing to tabular: much smaller but still substantial
- CONUS at 0.25° = ~26,000 grid points × 5 variables = 130,000 records per GRIB2 file
- Total: ~2.3 billion records per year at full resolution

**Practical Mitigation:**
1. Reduce grid resolution: Only extract every 4th grid point (1° resolution instead of 0.25°) → 16x reduction
   - Add config: `grid_step` (IntegerType, default 4) — extract every Nth grid point
2. Reduce forecast hours: Use [0, 24, 48, 72, 120] instead of all 384
3. Reduce cycles: Use ["00", "12"] instead of all 4
4. Start with 1-2 years for research, not full history

**NOMADS Availability:**
- NOMADS only keeps ~10 days of recent data
- For historical backfill, you MUST use NCEI archive or AWS S3
- AWS S3 keeps ~30-60 days rolling
- NCEI has years of history but is slower

**Missing Data:**
- Some forecast runs may be missing (model re-runs, maintenance)
- If a GRIB2 file 404s, log WARNING and skip — do NOT fail the entire extraction
- Track missing runs in a summary log

### 6. pyproject.toml

```toml
[project]
name = "tap-noaa-gfs"
version = "0.0.1"
description = "Singer tap for NOAA GFS weather forecast model data (GRIB2)"
requires-python = ">=3.10,<4.0"

dependencies = [
    "singer-sdk~=0.53.5",
    "requests~=2.32.3",
    "backoff>=2.2.1,<3.0.0",
    "cfgrib>=0.9.10",
    "xarray>=2024.1.0",
    "eccodes>=1.6.0",
    "numpy>=1.26.0",
]

[project.scripts]
tap-noaa-gfs = "tap_noaa_gfs.tap:TapNOAAGFS.cli"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

**Note:** `eccodes` is the ECMWF GRIB library that `cfgrib` depends on. On macOS: `brew install eccodes`. On Linux: `apt-get install libeccodes-dev`.

### 7. meltano.yml

```yaml
version: 1
send_anonymous_usage_stats: false
project_id: "tap-noaa-gfs"

plugins:
  extractors:
  - name: tap-noaa-gfs
    namespace: tap_noaa_gfs
    pip_url: -e .
    capabilities:
      - state
      - catalog
      - discover
      - about
      - stream-maps
    settings:
      - name: source
        kind: string
      - name: start_date
        kind: date_iso8601
      - name: end_date
        kind: date_iso8601
      - name: cycles
        kind: array
      - name: forecast_hours
        kind: array
      - name: variables
        kind: array
      - name: bounding_box
        kind: object
      - name: grid_step
        kind: integer
      - name: max_requests_per_minute
        kind: integer
      - name: strict_mode
        kind: boolean
    select:
      - forecast_runs.*
      - forecast_data.*
    config:
      source: "nomads"
      start_date: "2024-01-01"
      cycles: ["00", "12"]
      forecast_hours: [0, 6, 12, 18, 24, 48, 72, 96, 120, 168, 240, 336]
      variables: ["TMP", "UGRD", "VGRD", "APCP", "RH"]
      bounding_box:
        north: 50
        south: 24
        west: -125
        east: -66
      grid_step: 4
      max_requests_per_minute: 20

  loaders:
  - name: target-jsonl
    variant: andyh1203
    pip_url: target-jsonl
```

---

## Critical Rules

1. **ALL imports at top of file.** No inline imports.
2. **GRIB2 longitude is 0-360.** Convert to -180 to 180 for output.
3. **Temperature is in Kelvin.** Output as-is with units field. Don't silently convert.
4. **Delete temp GRIB2 files** after parsing. These are large.
5. **NOMADS has limited history** (~10 days). For backfill, code the NCEI/S3 fallback path.
6. **Some GRIB2 files will 404.** Log WARNING, skip, continue.
7. **Install eccodes system library** before cfgrib. Document this in README.
8. **Grid step config** to control output volume. Default 4 (1° resolution) is ~1,600 grid points for CONUS instead of ~26,000.
9. **Rate limit NOMADS** — be a good citizen. 20 req/min max.
10. **Use UV** for all Python execution.
11. **Match existing tap patterns** for config, state, error handling.
