"""NoaaGfs entry point."""

from __future__ import annotations

from tap_noaa_gfs.tap import TapNoaaGfs

TapNoaaGfs.cli()
