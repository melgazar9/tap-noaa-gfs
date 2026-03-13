"""Helper utilities for tap-noaa-gfs."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

# Forecast hours: 0-120 hourly, then 123-384 every 3 hours (209 total)
ALL_FORECAST_HOURS: list[int] = list(range(121)) + list(range(123, 385, 3))

VALID_CYCLES: list[str] = ["00", "06", "12", "18"]
VALID_SOURCES: tuple[str, ...] = ("nomads", "aws_s3")

# S3 directory structure changed mid-2021: before this date, no "atmos/" subdirectory
S3_ATMOS_SUBDIR_CUTOFF = date(2021, 6, 1)

# Variables NOT available at forecast hour 0 (analysis).
# Accumulated/averaged fields only exist at f001+.
VARIABLES_UNAVAILABLE_AT_F000: set[str] = {
    "APCP",
    "ACPCP",
    "LHTFL",
    "SHTFL",
    "DLWRF",
    "DSWRF",
    "ULWRF",
    "USWRF",
    "TMAX",
    "TMIN",
    "WATR",
    "CPRAT",
    "PRATE",
    "SUNSD",
}


def _gfs_filename(cycle: str, forecast_hour: int) -> str:
    """Build the canonical GFS GRIB2 filename."""
    return f"gfs.t{cycle}z.pgrb2.0p25.f{forecast_hour:03d}"


def generate_date_range(start_date: str, end_date: str | None) -> list[str]:
    """Generate inclusive list of date strings from start_date to end_date.

    Args:
        start_date: ISO format date string (YYYY-MM-DD).
        end_date: ISO format date string. Defaults to today (UTC).

    Returns:
        List of date strings in YYYY-MM-DD format.
    """
    start = _parse_date(start_date)
    end = _parse_date(end_date) if end_date else datetime.now(tz=timezone.utc).date()
    dates: list[str] = []
    current = start
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def _parse_date(date_string: str) -> date:
    """Parse a date string in YYYY-MM-DD or ISO datetime format to a date object."""
    return datetime.fromisoformat(date_string.replace("Z", "+00:00")).date()


def compute_valid_time(run_date: str, cycle: str, forecast_hour: int) -> str:
    """Compute the valid forecast time from run parameters.

    valid_time = run_date + cycle_hour + forecast_hour

    Returns:
        ISO 8601 datetime string in UTC.
    """
    run_dt = datetime.strptime(f"{run_date}T{cycle}:00:00", "%Y-%m-%dT%H:%M:%S").replace(
        tzinfo=timezone.utc
    )
    valid_dt = run_dt + timedelta(hours=forecast_hour)
    return valid_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_nomads_url(  # noqa: PLR0913
    base_url: str,
    run_date: str,
    cycle: str,
    forecast_hour: int,
    variables: list[str],
    levels: list[str],
    bounding_box: dict | None,
) -> str:
    """Build a NOMADS filter URL for a specific GFS GRIB2 file.

    The NOMADS filter supports server-side subsetting by variable, level, and region,
    which reduces downloaded file sizes from ~500MB to ~5MB.

    Args:
        base_url: NOMADS filter base URL.
        run_date: Model run date (YYYY-MM-DD).
        cycle: Model cycle hour ("00", "06", "12", "18").
        forecast_hour: Forecast hour (0-384).
        variables: GRIB2 variable names (e.g. ["TMP", "UGRD"]).
        levels: Level names (e.g. ["2 m above ground"]).
        bounding_box: Geographic filter with north/south/east/west keys.

    Returns:
        Complete NOMADS filter URL string.
    """
    date_str = run_date.replace("-", "")
    file_name = _gfs_filename(cycle, forecast_hour)
    directory = f"/gfs.{date_str}/{cycle}/atmos"

    # Filter out variables unavailable at analysis hour
    active_variables = filter_variables_for_forecast_hour(variables, forecast_hour)

    params: list[tuple[str, str]] = [
        ("file", file_name),
        ("dir", directory),
    ]
    params.extend((f"var_{var}", "on") for var in active_variables)
    for level in levels:
        level_key = f"lev_{level.replace(' ', '_')}"
        params.append((level_key, "on"))
    if bounding_box:
        params.append(("subregion", ""))
        params.append(("leftlon", str(bounding_box["west"])))
        params.append(("rightlon", str(bounding_box["east"])))
        params.append(("toplat", str(bounding_box["north"])))
        params.append(("bottomlat", str(bounding_box["south"])))

    return f"{base_url}?{urlencode(params)}"


def build_s3_url(
    bucket: str,
    run_date: str,
    cycle: str,
    forecast_hour: int,
) -> str:
    """Build an AWS S3 HTTP URL for a GFS GRIB2 file.

    Handles the directory structure change that occurred mid-2021:
    - Before ~June 2021: gfs.YYYYMMDD/HH/gfs.tHHz.pgrb2.0p25.fFFF
    - After ~June 2021: gfs.YYYYMMDD/HH/atmos/gfs.tHHz.pgrb2.0p25.fFFF

    Args:
        bucket: S3 bucket name.
        run_date: Model run date (YYYY-MM-DD).
        cycle: Model cycle hour.
        forecast_hour: Forecast hour.

    Returns:
        S3 HTTPS URL string.
    """
    date_str = run_date.replace("-", "")
    file_name = _gfs_filename(cycle, forecast_hour)
    run_date_obj = _parse_date(run_date)

    if run_date_obj >= S3_ATMOS_SUBDIR_CUTOFF:
        path = f"gfs.{date_str}/{cycle}/atmos/{file_name}"
    else:
        path = f"gfs.{date_str}/{cycle}/{file_name}"

    return f"https://{bucket}.s3.amazonaws.com/{path}"


def filter_variables_for_forecast_hour(
    variables: list[str],
    forecast_hour: int,
) -> list[str]:
    """Remove variables that are unavailable at the given forecast hour.

    Accumulated/averaged variables (APCP, ACPCP, etc.) do not exist at
    forecast hour 0 (the analysis). Requesting them would cause a NOMADS error.
    """
    if forecast_hour == 0:
        filtered = [v for v in variables if v not in VARIABLES_UNAVAILABLE_AT_F000]
        removed = set(variables) - set(filtered)
        if removed:
            logger.debug(
                "Skipping variables %s at forecast hour 0 (not available in analysis)",
                removed,
            )
        return filtered
    return variables
