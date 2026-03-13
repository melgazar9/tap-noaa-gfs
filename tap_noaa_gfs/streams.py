"""Stream type classes for tap-noaa-gfs.

Re-exports stream classes from client.py for use in tap.py discovery.
"""

from __future__ import annotations

from tap_noaa_gfs.client import ForecastDataStream, ForecastRunsStream

__all__ = ["ForecastDataStream", "ForecastRunsStream"]
