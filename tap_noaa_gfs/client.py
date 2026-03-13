"""GFS base stream class for downloading and parsing GRIB2 forecast files.

Extends singer_sdk.Stream (not RESTStream) because GFS data comes from
GRIB2 binary file downloads, not JSON REST endpoints.
"""

from __future__ import annotations

import logging
import random
import sys
import tempfile
import time
from abc import ABC
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, ClassVar

import backoff
import requests
from singer_sdk import Stream

from tap_noaa_gfs.grib_parser import GribParser
from tap_noaa_gfs.helpers import (
    VALID_SOURCES,
    build_nomads_url,
    build_s3_url,
    filter_variables_for_forecast_hour,
    generate_date_range,
)

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context

logger = logging.getLogger(__name__)


class GFSBaseStream(Stream, ABC):
    """Base class for GFS streams.

    Provides GRIB2 file download with retry/backoff, sliding window rate limiting,
    and parallel download infrastructure. Subclasses implement get_records() to
    define what data is extracted.
    """

    _throttle_lock: ClassVar[Lock] = Lock()
    _request_timestamps: ClassVar[deque[float]] = deque()

    @property
    def _max_requests_per_minute(self) -> int:
        return int(self.config.get("max_requests_per_minute", 20))

    @property
    def _temp_dir(self) -> Path:
        return Path(self.config.get("temp_dir", tempfile.gettempdir()))

    @property
    def _source(self) -> str:
        return self.config.get("source", "nomads")

    @property
    def _grid_step(self) -> int:
        return int(self.config.get("grid_step", 4))

    @property
    def _strict_mode(self) -> bool:
        return bool(self.config.get("strict_mode", False))

    @property
    def _configured_variables(self) -> list[str]:
        return list(self.config.get("variables", ["TMP", "UGRD", "VGRD", "APCP", "RH"]))

    @property
    def _configured_levels(self) -> list[str]:
        return list(self.config.get("levels", ["2 m above ground", "10 m above ground", "surface"]))

    @property
    def _bounding_box(self) -> dict:
        return dict(
            self.config.get(
                "bounding_box",
                {"north": 50, "south": 24, "west": -125, "east": -66},
            )
        )

    def _throttle(self) -> None:
        """Sliding window rate limiter.

        Tracks request timestamps in a 60-second window and sleeps if the
        configured max_requests_per_minute limit is reached. Sleeps outside
        the lock to avoid blocking other threads during rate-limit waits.
        """
        sleep_duration = 0.0
        with self._throttle_lock:
            now = time.time()
            window_start = now - 60.0

            while self._request_timestamps and self._request_timestamps[0] < window_start:
                self._request_timestamps.popleft()

            if len(self._request_timestamps) >= self._max_requests_per_minute:
                oldest = self._request_timestamps[0]
                wait_time = oldest + 60.0 - now
                if wait_time > 0:
                    sleep_duration = wait_time + random.uniform(0.1, 0.5)  # noqa: S311

            self._request_timestamps.append(time.time())

        if sleep_duration > 0:
            logger.info("Rate limit reached, sleeping %.1fs", sleep_duration)
            time.sleep(sleep_duration)

    @backoff.on_exception(
        backoff.expo,
        (requests.exceptions.RequestException, IOError),
        max_tries=5,
        max_time=300,
        jitter=backoff.full_jitter,
        giveup=lambda e: (
            isinstance(e, requests.exceptions.HTTPError)
            and e.response is not None
            and e.response.status_code not in {429, 500, 502, 503, 504}
        ),
    )
    def _download_grib_file(self, url: str, dest_path: Path) -> Path:
        """Download a GRIB2 file from NOMADS or S3 with retry and backoff.

        Args:
            url: URL of the GRIB2 file.
            dest_path: Local path to write the downloaded file.

        Returns:
            The dest_path on success.

        Raises:
            requests.exceptions.HTTPError: On non-retryable HTTP errors.
            IOError: On file write errors.
        """
        self._throttle()
        response = requests.get(url, stream=True, timeout=(30, 300))
        response.raise_for_status()

        with dest_path.open("wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        file_size_mb = dest_path.stat().st_size / (1024 * 1024)
        logger.debug("Downloaded %.1fMB: %s", file_size_mb, dest_path)
        return dest_path

    def _build_download_url(
        self,
        run_date: str,
        cycle: str,
        forecast_hour: int,
    ) -> str:
        """Build the download URL based on configured source."""
        if self._source == "nomads":
            return build_nomads_url(
                base_url=self.config.get(
                    "nomads_url",
                    "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl",
                ),
                run_date=run_date,
                cycle=cycle,
                forecast_hour=forecast_hour,
                variables=self._configured_variables,
                levels=self._configured_levels,
                bounding_box=self._bounding_box,
            )
        if self._source == "aws_s3":
            return build_s3_url(
                bucket=self.config.get("aws_s3_bucket", "noaa-gfs-bdp-pds"),
                run_date=run_date,
                cycle=cycle,
                forecast_hour=forecast_hour,
            )
        msg = f"Unsupported source: {self._source}. Use one of: {VALID_SOURCES}"
        raise ValueError(msg)

    def _download_and_parse_forecast_hour(
        self,
        run_date: str,
        cycle: str,
        forecast_hour: int,
    ) -> list[dict]:
        """Download one GRIB2 file, parse it, return records, clean up temp file.

        This method is designed to be called from a ThreadPoolExecutor for
        parallel downloads within a single (run_date, cycle) partition.
        """
        url = self._build_download_url(run_date, cycle, forecast_hour)
        dest = self._temp_dir / f"gfs_{run_date}_{cycle}_f{forecast_hour:03d}.grib2"

        # Filter variables unavailable at this forecast hour
        active_variables = filter_variables_for_forecast_hour(
            self._configured_variables, forecast_hour
        )
        if not active_variables:
            logger.debug("No active variables for forecast hour %d, skipping", forecast_hour)
            return []

        try:
            self._download_grib_file(url, dest)
            parser = GribParser()
            return parser.parse_grib_file(
                file_path=str(dest),
                variables=active_variables,
                bounding_box=self._bounding_box,
                run_date=run_date,
                cycle=cycle,
                forecast_hour=forecast_hour,
                grid_step=self._grid_step,
            )
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            if status == 404:  # noqa: PLR2004
                logger.warning(
                    "GRIB2 file not found (404): run=%s cycle=%s fh=%d — skipping",
                    run_date,
                    cycle,
                    forecast_hour,
                )
                return []
            logger.warning(
                "HTTP %s downloading run=%s cycle=%s fh=%d: %s",
                status,
                run_date,
                cycle,
                forecast_hour,
                e,
            )
            if self._strict_mode:
                raise
            return []
        except Exception:
            logger.exception(
                "Failed to download/parse run=%s cycle=%s fh=%d",
                run_date,
                cycle,
                forecast_hour,
            )
            if self._strict_mode:
                raise
            return []
        finally:
            if dest.exists():
                dest.unlink()

    def _download_and_yield_partition_records(
        self,
        run_date: str,
        cycle: str,
        forecast_hours: list[int],
    ) -> Iterable[dict]:
        """Download forecast hour files in parallel, yield records sequentially.

        Uses ThreadPoolExecutor for I/O-bound GRIB2 downloads. Records are
        yielded sequentially as required by the Singer protocol.
        """
        max_workers = min(
            int(self.config.get("max_concurrent_downloads", 2)),
            len(forecast_hours),
        )

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(
                    self._download_and_parse_forecast_hour,
                    run_date,
                    cycle,
                    fh,
                ): fh
                for fh in forecast_hours
            }

            for future in as_completed(futures):
                fh = futures[future]
                try:
                    records = future.result()
                    yield from records
                except Exception:
                    logger.exception(
                        "Unhandled error for run=%s cycle=%s fh=%d",
                        run_date,
                        cycle,
                        fh,
                    )
                    if self._strict_mode:
                        raise


class ForecastRunsStream(GFSBaseStream):
    """Discover which (run_date, cycle) combinations have data available.

    Probes NOMADS or S3 to check availability. Useful for data quality
    monitoring and understanding what data exists before extraction.
    """

    name = "forecast_runs"
    primary_keys = ("run_date", "cycle")
    replication_key = "run_date"
    is_sorted = True

    schema: ClassVar[dict] = {
        "type": "object",
        "properties": {
            "run_date": {"type": "string", "format": "date"},
            "cycle": {"type": "string"},
            "status": {"type": "string"},
            "source": {"type": "string"},
        },
        "required": ["run_date", "cycle", "status"],
    }

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Probe each (run_date, cycle) to check if data is available."""
        dates = generate_date_range(
            self.config["start_date"],
            self.config.get("end_date"),
        )
        cycles = list(self.config.get("cycles", ["00", "06", "12", "18"]))

        for run_date in dates:
            for cycle in cycles:
                status = self._probe_run_availability(run_date, cycle)
                yield {
                    "run_date": run_date,
                    "cycle": cycle,
                    "status": status,
                    "source": self._source,
                }

    def _probe_run_availability(self, run_date: str, cycle: str) -> str:
        """Check if a forecast run exists by probing the f000 (analysis) file.

        Uses a streaming GET with early close rather than HEAD, because NOMADS
        CGI filter scripts do not reliably support HEAD requests.

        Returns "available" or "missing".
        """
        url = self._build_download_url(run_date, cycle, forecast_hour=0)
        try:
            self._throttle()
            response = requests.get(url, stream=True, timeout=30)
            status = response.status_code
            response.close()
        except requests.exceptions.RequestException:
            return "missing"
        else:
            if status == 200:  # noqa: PLR2004
                return "available"
            return "missing"


class ForecastDataStream(GFSBaseStream):
    """Extract GFS forecast data — the main data stream.

    Partitioned by (run_date, cycle). For each partition, downloads all
    configured forecast hour GRIB2 files in parallel, parses them, and
    yields flat records with one row per (lat, lon, variable, forecast_hour).

    Primary keys: run_date + cycle + forecast_hour + latitude + longitude + variable
    Replication key: run_date (enables incremental sync)
    """

    name = "forecast_data"
    primary_keys = (
        "run_date",
        "cycle",
        "forecast_hour",
        "latitude",
        "longitude",
        "variable",
    )
    replication_key = "run_date"
    is_sorted = True

    schema: ClassVar[dict] = {
        "type": "object",
        "properties": {
            "run_date": {"type": "string", "format": "date"},
            "cycle": {"type": "string"},
            "forecast_hour": {"type": "integer"},
            "valid_time": {"type": "string", "format": "date-time"},
            "latitude": {"type": "number"},
            "longitude": {"type": "number"},
            "variable": {"type": "string"},
            "level": {"type": ["string", "null"]},
            "value": {"type": ["number", "null"]},
            "units": {"type": ["string", "null"]},
        },
        "required": [
            "run_date",
            "cycle",
            "forecast_hour",
            "valid_time",
            "latitude",
            "longitude",
            "variable",
        ],
    }

    @property
    def partitions(self) -> list[dict]:
        """Generate partitions as (run_date, cycle) combinations.

        Each partition downloads all configured forecast hours in parallel
        within get_records().
        """
        dates = generate_date_range(
            self.config["start_date"],
            self.config.get("end_date"),
        )
        cycles = list(self.config.get("cycles", ["00", "06", "12", "18"]))

        return [{"run_date": run_date, "cycle": cycle} for run_date in dates for cycle in cycles]

    @override
    def get_records(self, context: Context | None) -> Iterable[dict]:
        """Download and parse all forecast hours for a (run_date, cycle) partition."""
        if context is None:
            msg = "ForecastDataStream requires partition context (run_date, cycle)"
            raise ValueError(msg)

        run_date = context["run_date"]
        cycle = context["cycle"]
        forecast_hours = list(
            self.config.get(
                "forecast_hours",
                [0, 6, 12, 18, 24, 48, 72, 96, 120, 168, 240, 336],
            )
        )

        logger.info(
            "Extracting forecast data: run=%s cycle=%s forecast_hours=%s",
            run_date,
            cycle,
            forecast_hours,
        )

        yield from self._download_and_yield_partition_records(
            run_date=run_date,
            cycle=cycle,
            forecast_hours=forecast_hours,
        )
