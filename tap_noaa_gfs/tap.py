"""TapNoaaGfs — Singer tap for NOAA GFS weather forecast model data.

Extracts GFS forecast data from NOMADS (server-side filtered) or AWS S3
(full GRIB2 files). Designed for weather-driven commodity trading, backtesting,
and energy price modeling.

Data is emitted as flat records: one row per (run_date, cycle, forecast_hour,
latitude, longitude, variable).
"""

from __future__ import annotations

import logging
import sys

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_noaa_gfs.grib_parser import NOMADS_TO_CFGRIB_NAMES
from tap_noaa_gfs.helpers import ALL_FORECAST_HOURS, VALID_CYCLES, VALID_SOURCES
from tap_noaa_gfs.streams import ForecastDataStream, ForecastRunsStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

logger = logging.getLogger(__name__)


class TapNoaaGfs(Tap):
    """Singer tap for NOAA GFS weather forecast data (GRIB2).

    Configuration controls which data source, date range, cycles, forecast hours,
    variables, levels, and geographic region to extract. See README for details.
    """

    name = "tap-noaa-gfs"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "source",
            th.StringType,
            default="nomads",
            description=(
                "Data source: 'nomads' (server-side filtered, ~10 day retention) "
                "or 'aws_s3' (full files, data from 2021+)."
            ),
        ),
        th.Property(
            "nomads_url",
            th.StringType,
            default="https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl",
            description="NOMADS GFS 0.25-degree filter endpoint URL.",
        ),
        th.Property(
            "aws_s3_bucket",
            th.StringType,
            default="noaa-gfs-bdp-pds",
            description="AWS S3 bucket for GFS data (public, no auth required).",
        ),
        th.Property(
            "start_date",
            th.DateType,
            required=True,
            description="First forecast run date to extract (YYYY-MM-DD).",
        ),
        th.Property(
            "end_date",
            th.DateType,
            description="Last forecast run date. Defaults to today (UTC).",
        ),
        th.Property(
            "cycles",
            th.ArrayType(th.StringType),
            default=["00", "06", "12", "18"],
            description=(
                "Which GFS cycles to extract. GFS runs 4x daily at "
                "00z, 06z, 12z, 18z UTC. Use ['00', '12'] for twice-daily."
            ),
        ),
        th.Property(
            "forecast_hours",
            th.ArrayType(th.IntegerType),
            default=[0, 6, 12, 18, 24, 48, 72, 96, 120, 168, 240, 336],
            description=(
                "Which forecast hours to extract. GFS produces hourly forecasts "
                "0-120, then 3-hourly 123-384. Default selects key horizons."
            ),
        ),
        th.Property(
            "variables",
            th.ArrayType(th.StringType),
            default=["TMP", "UGRD", "VGRD", "APCP", "RH"],
            description=(
                "GRIB2 variable names to extract. Use NOMADS parameter names "
                "(e.g. TMP, UGRD, VGRD, APCP, RH, HGT, PRES)."
            ),
        ),
        th.Property(
            "levels",
            th.ArrayType(th.StringType),
            default=["2 m above ground", "10 m above ground", "surface"],
            description=(
                "Atmospheric levels to extract. Uses NOMADS level names "
                "(e.g. '2 m above ground', '10 m above ground', 'surface', '850 mb')."
            ),
        ),
        th.Property(
            "bounding_box",
            th.ObjectType(
                th.Property("north", th.NumberType, default=50),
                th.Property("south", th.NumberType, default=24),
                th.Property("west", th.NumberType, default=-125),
                th.Property("east", th.NumberType, default=-66),
            ),
            default={"north": 50, "south": 24, "west": -125, "east": -66},
            description=(
                "Geographic bounding box for CONUS. Latitude in degrees N, "
                "longitude in degrees E (use negative for west)."
            ),
        ),
        th.Property(
            "grid_step",
            th.IntegerType,
            default=4,
            description=(
                "Extract every Nth grid point. 1=full 0.25-degree resolution "
                "(~26,000 CONUS points), 4=1-degree (~1,600 points). "
                "Higher values reduce data volume proportionally."
            ),
        ),
        th.Property(
            "max_concurrent_downloads",
            th.IntegerType,
            default=2,
            description="Max parallel GRIB2 file downloads per partition.",
        ),
        th.Property(
            "max_requests_per_minute",
            th.IntegerType,
            default=20,
            description="Rate limit for NOMADS requests. Be a good citizen.",
        ),
        th.Property(
            "temp_dir",
            th.StringType,
            description="Directory for temporary GRIB2 files. Defaults to system temp.",
        ),
        th.Property(
            "strict_mode",
            th.BooleanType,
            default=False,
            description=(
                "If True, fail on any download/parse error. "
                "If False, log warnings and skip failed files."
            ),
        ),
    ).to_dict()

    @override
    def discover_streams(self) -> list:
        """Return discovered GFS streams.

        Returns:
            List containing ForecastRunsStream and ForecastDataStream.
        """
        self._validate_gfs_config()
        return [
            ForecastRunsStream(self),
            ForecastDataStream(self),
        ]

    def _validate_gfs_config(self) -> None:
        """Validate configuration values and warn about potential issues."""
        configured_cycles = self.config.get("cycles", VALID_CYCLES)
        invalid_cycles = set(configured_cycles) - set(VALID_CYCLES)
        if invalid_cycles:
            msg = f"Invalid cycle(s): {invalid_cycles}. GFS only produces cycles: {VALID_CYCLES}"
            raise ValueError(msg)

        configured_fh = self.config.get("forecast_hours", [0])
        invalid_fh = set(configured_fh) - set(ALL_FORECAST_HOURS)
        if invalid_fh:
            logger.warning(
                "Forecast hours %s are not standard GFS hours. "
                "Valid hours: 0-120 (hourly), 123-384 (3-hourly). "
                "NOMADS will return 404 for non-existent hours.",
                sorted(invalid_fh),
            )

        configured_vars = self.config.get("variables", [])
        unmapped_vars = [v for v in configured_vars if v not in NOMADS_TO_CFGRIB_NAMES]
        if unmapped_vars:
            logger.warning(
                "Variables %s have no cfgrib name mapping in NOMADS_TO_CFGRIB_NAMES. "
                "They will be downloaded from NOMADS but may not be parsed correctly. "
                "Verify with a test download and add mappings to grib_parser.py.",
                unmapped_vars,
            )

        source = self.config.get("source", "nomads")
        if source not in VALID_SOURCES:
            msg = f"Invalid source '{source}'. Must be one of: {VALID_SOURCES}"
            raise ValueError(msg)


if __name__ == "__main__":
    TapNoaaGfs.cli()
