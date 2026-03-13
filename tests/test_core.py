"""Tests for tap-noaa-gfs using the built-in SDK test library."""

from __future__ import annotations

from singer_sdk.testing import get_tap_test_class

from tap_noaa_gfs.tap import TapNoaaGfs

SAMPLE_CONFIG = {
    "start_date": "2026-03-12",
    "end_date": "2026-03-12",
    "cycles": ["00"],
    "forecast_hours": [0],
    "variables": ["TMP"],
    "levels": ["2 m above ground"],
    "grid_step": 4,
    "source": "nomads",
}

TestTapNoaaGfs = get_tap_test_class(
    tap_class=TapNoaaGfs,
    config=SAMPLE_CONFIG,
)
