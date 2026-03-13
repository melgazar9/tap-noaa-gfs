"""Smoke tests for tap-noaa-gfs.

Fast, offline tests that verify the tap can be imported, configured,
and discovers streams correctly. No network calls. Suitable for pre-commit.
"""

from __future__ import annotations

import pytest

from tap_noaa_gfs.grib_parser import NOMADS_TO_CFGRIB_NAMES, _build_cfgrib_to_nomads_map
from tap_noaa_gfs.helpers import (
    ALL_FORECAST_HOURS,
    VALID_CYCLES,
    build_nomads_url,
    build_s3_url,
    compute_valid_time,
    filter_variables_for_forecast_hour,
    generate_date_range,
)
from tap_noaa_gfs.tap import TapNoaaGfs

MINIMAL_CONFIG = {
    "start_date": "2026-01-01",
    "end_date": "2026-01-01",
    "cycles": ["00"],
    "forecast_hours": [0],
    "variables": ["TMP"],
    "levels": ["2 m above ground"],
    "grid_step": 4,
    "source": "nomads",
}


def test_tap_imports_and_initializes() -> None:
    """Tap class can be instantiated with minimal config."""
    tap = TapNoaaGfs(config=MINIMAL_CONFIG)
    assert tap.name == "tap-noaa-gfs"


def test_tap_discovers_two_streams() -> None:
    """Tap discovers exactly forecast_runs and forecast_data streams."""
    tap = TapNoaaGfs(config=MINIMAL_CONFIG)
    streams = tap.discover_streams()
    stream_names = {s.name for s in streams}
    assert stream_names == {"forecast_runs", "forecast_data"}


def test_forecast_data_schema_has_required_fields() -> None:
    """ForecastDataStream schema includes all fields needed for ML pipelines."""
    tap = TapNoaaGfs(config=MINIMAL_CONFIG)
    streams = {s.name: s for s in tap.discover_streams()}
    schema_props = streams["forecast_data"].schema["properties"]
    required_fields = {
        "run_date",
        "cycle",
        "forecast_hour",
        "valid_time",
        "latitude",
        "longitude",
        "variable",
        "level",
        "value",
        "units",
    }
    assert required_fields == set(schema_props.keys())


def test_generate_date_range_single_day() -> None:
    """Single day range returns exactly one date."""
    dates = generate_date_range("2026-01-15", "2026-01-15")
    assert dates == ["2026-01-15"]


def test_generate_date_range_multi_day() -> None:
    """Multi-day range is inclusive on both ends."""
    dates = generate_date_range("2026-01-01", "2026-01-03")
    assert dates == ["2026-01-01", "2026-01-02", "2026-01-03"]


def test_compute_valid_time() -> None:
    """Valid time = run_date + cycle + forecast_hour."""
    assert compute_valid_time("2026-01-01", "00", 0) == "2026-01-01T00:00:00Z"
    assert compute_valid_time("2026-01-01", "06", 24) == "2026-01-02T06:00:00Z"
    assert compute_valid_time("2026-01-01", "12", 120) == "2026-01-06T12:00:00Z"


def test_filter_variables_excludes_apcp_at_fh0() -> None:
    """Accumulated variables are filtered out at forecast hour 0."""
    all_vars = ["TMP", "UGRD", "VGRD", "APCP", "RH"]
    filtered = filter_variables_for_forecast_hour(all_vars, forecast_hour=0)
    assert "TMP" in filtered
    assert "APCP" not in filtered


def test_filter_variables_keeps_apcp_at_fh6() -> None:
    """Accumulated variables are preserved at forecast hour > 0."""
    all_vars = ["TMP", "UGRD", "VGRD", "APCP", "RH"]
    filtered = filter_variables_for_forecast_hour(all_vars, forecast_hour=6)
    assert "APCP" in filtered
    assert filtered == all_vars


def test_build_nomads_url_structure() -> None:
    """NOMADS URL includes file, dir, variable, level, and subregion params."""
    url = build_nomads_url(
        base_url="https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl",
        run_date="2026-01-15",
        cycle="00",
        forecast_hour=24,
        variables=["TMP", "RH"],
        levels=["2 m above ground"],
        bounding_box={"north": 50, "south": 24, "west": -125, "east": -66},
    )
    assert "file=gfs.t00z.pgrb2.0p25.f024" in url
    assert "dir=%2Fgfs.20260115%2F00%2Fatmos" in url
    assert "var_TMP=on" in url
    assert "var_RH=on" in url
    assert "lev_2_m_above_ground=on" in url
    assert "subregion=" in url
    assert "leftlon=-125" in url


def test_build_s3_url_post_2021() -> None:
    """S3 URL includes atmos/ subdirectory for post-June 2021 data."""
    url = build_s3_url("noaa-gfs-bdp-pds", "2026-01-15", "00", 24)
    assert "/atmos/" in url
    assert "gfs.t00z.pgrb2.0p25.f024" in url


def test_build_s3_url_pre_2021() -> None:
    """S3 URL omits atmos/ subdirectory for pre-June 2021 data."""
    url = build_s3_url("noaa-gfs-bdp-pds", "2021-01-15", "00", 0)
    assert "/atmos/" not in url


def test_cfgrib_mapping_covers_default_variables() -> None:
    """All default configured variables have cfgrib name mappings."""
    default_vars = ["TMP", "UGRD", "VGRD", "APCP", "RH"]
    for var in default_vars:
        assert var in NOMADS_TO_CFGRIB_NAMES, f"{var} missing from NOMADS_TO_CFGRIB_NAMES"
        assert len(NOMADS_TO_CFGRIB_NAMES[var]) > 0


def test_cfgrib_reverse_mapping() -> None:
    """Reverse mapping correctly maps cfgrib names back to NOMADS names."""
    reverse = _build_cfgrib_to_nomads_map(["TMP", "UGRD"])
    assert reverse["t2m"] == "TMP"
    assert reverse["t"] == "TMP"
    assert reverse["u10"] == "UGRD"
    assert "tp" not in reverse  # APCP not requested


def test_all_forecast_hours_constant() -> None:
    """ALL_FORECAST_HOURS matches GFS specification: 0-120 hourly, 123-384 3-hourly."""
    assert ALL_FORECAST_HOURS[0] == 0
    assert ALL_FORECAST_HOURS[120] == 120
    assert ALL_FORECAST_HOURS[121] == 123
    assert ALL_FORECAST_HOURS[-1] == 384
    assert len(ALL_FORECAST_HOURS) == 209


def test_valid_cycles() -> None:
    """VALID_CYCLES matches GFS specification."""
    assert VALID_CYCLES == ["00", "06", "12", "18"]


def test_config_validation_rejects_invalid_cycle() -> None:
    """Config validation rejects invalid cycle values."""
    bad_config = {**MINIMAL_CONFIG, "cycles": ["03"]}
    with pytest.raises(ValueError, match="Invalid cycle"):
        TapNoaaGfs(config=bad_config)


def test_config_validation_rejects_invalid_source() -> None:
    """Config validation rejects unsupported source values."""
    bad_config = {**MINIMAL_CONFIG, "source": "invalid"}
    with pytest.raises(ValueError, match="Invalid source"):
        TapNoaaGfs(config=bad_config)


def test_nomads_url_filters_apcp_at_fh0() -> None:
    """NOMADS URL builder excludes APCP at forecast hour 0."""
    url = build_nomads_url(
        base_url="https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl",
        run_date="2026-01-15",
        cycle="00",
        forecast_hour=0,
        variables=["TMP", "APCP"],
        levels=["surface"],
        bounding_box=None,
    )
    assert "var_TMP=on" in url
    assert "var_APCP" not in url
