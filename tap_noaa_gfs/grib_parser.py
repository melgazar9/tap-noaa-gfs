"""GRIB2 file parser for extracting GFS forecast data into tabular records.

Uses cfgrib/xarray to read GRIB2 binary files and extract specific variables
within a geographic bounding box. Outputs flat dictionaries suitable for
Singer record emission.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

import cfgrib  # type: ignore[import-untyped]
import numpy as np

from tap_noaa_gfs.helpers import compute_valid_time

if TYPE_CHECKING:
    import xarray as xr

logger = logging.getLogger(__name__)

# Mapping from NOMADS variable names to cfgrib xarray variable names.
# cfgrib uses ecCodes short names which differ from NOMADS/GRIB2 abbreviations.
# This mapping is derived from actual GRIB2 file inspection and depends on the
# level type. A single NOMADS variable can map to multiple cfgrib names
# (e.g. TMP → t2m at 2m height, t at surface/isobaric).
NOMADS_TO_CFGRIB_NAMES: dict[str, list[str]] = {
    # Temperature
    "TMP": ["t2m", "t"],
    "TMAX": ["tmax"],
    "TMIN": ["tmin"],
    "DPT": ["d2m", "dpt"],
    # Wind
    "UGRD": ["u10", "u"],
    "VGRD": ["v10", "v"],
    "GUST": ["gust", "i10fg"],
    # Moisture
    "RH": ["r2", "r"],
    "SPFH": ["q", "sh2"],
    "PWAT": ["pwat"],
    # Precipitation
    "APCP": ["tp"],
    "ACPCP": ["acpcp"],
    "PRATE": ["prate"],
    "CPRAT": ["cprat"],
    # Categorical weather
    "CSNOW": ["csnow"],
    "CRAIN": ["crain"],
    "CFRZR": ["cfrzr"],
    "CICEP": ["cicep"],
    # Pressure / heights
    "HGT": ["gh", "orog"],
    "PRES": ["sp", "pres"],
    "PRMSL": ["prmsl"],
    "MSLET": ["mslet"],
    # Stability
    "CAPE": ["cape"],
    "CIN": ["cin"],
    # Radiation
    "DSWRF": ["dswrf", "sdswrf"],
    "USWRF": ["uswrf"],
    "DLWRF": ["dlwrf"],
    "ULWRF": ["ulwrf"],
    # Cloud cover
    "TCDC": ["tcc"],
    "LCDC": ["lcc"],
    "MCDC": ["mcc"],
    "HCDC": ["hcc"],
    # Snow / ice
    "SNOD": ["sde"],
    "WEASD": ["sd"],
    # Surface fluxes
    "LHTFL": ["slhf"],
    "SHTFL": ["sshf"],
    "HPBL": ["blh"],
    # Soil
    "SOILW": ["swvl1", "swvl2", "swvl3", "swvl4"],
    "TSOIL": ["stl1", "stl2", "stl3", "stl4"],
    # Other
    "VIS": ["vis"],
    "REFC": ["refc"],
    "VVEL": ["w"],
    "ABSV": ["absv"],
    "CLWMR": ["clwmr"],
    "SUNSD": ["sunsd"],
}


def _build_cfgrib_to_nomads_map(requested_nomads_vars: list[str]) -> dict[str, str]:
    """Build reverse mapping: cfgrib variable name → NOMADS variable name.

    Only includes entries for the requested NOMADS variables, so we don't
    accidentally match variables the user didn't ask for.
    """
    reverse_map: dict[str, str] = {}
    for nomads_name in requested_nomads_vars:
        cfgrib_names = NOMADS_TO_CFGRIB_NAMES.get(nomads_name, [])
        for cfgrib_name in cfgrib_names:
            reverse_map[cfgrib_name] = nomads_name
    return reverse_map


class GribParser:
    """Parse GRIB2 files and extract weather variables as flat records.

    GRIB2 files contain gridded meteorological data. This parser:
    1. Opens the file with cfgrib (returns multiple xarray Datasets, one per level type)
    2. Maps cfgrib variable names back to NOMADS names using NOMADS_TO_CFGRIB_NAMES
    3. Filters to requested variables and geographic bounding box
    4. Optionally subsamples the grid (grid_step) to reduce output volume
    5. Converts gridded data to flat (lat, lon, variable, value) records

    Notes on GRIB2 conventions:
    - Longitude uses 0-360 range; we convert to -180 to 180 for output
    - Latitude runs from 90 (north) to -90 (south)
    - Temperature is in Kelvin (not converted here)
    - Accumulated precipitation (APCP) is cumulative within a forecast run
    """

    def parse_grib_file(  # noqa: PLR0913
        self,
        file_path: str,
        variables: list[str],
        bounding_box: dict,
        run_date: str,
        cycle: str,
        forecast_hour: int,
        grid_step: int = 1,
    ) -> list[dict]:
        """Parse a GRIB2 file and return flat records for all matching variables.

        Args:
            file_path: Path to the GRIB2 file on disk.
            variables: NOMADS variable names to extract (e.g. ["TMP", "UGRD"]).
            bounding_box: Dict with north/south/east/west lat/lon bounds.
            run_date: Model run date (YYYY-MM-DD).
            cycle: Model cycle hour ("00", "06", "12", "18").
            forecast_hour: Forecast hour offset from run time.
            grid_step: Extract every Nth grid point (1=full resolution, 4=1-degree).

        Returns:
            List of flat record dictionaries.
        """
        datasets = self._open_grib_datasets(file_path)
        if not datasets:
            logger.warning("No datasets found in GRIB2 file: %s", file_path)
            return []

        valid_time = compute_valid_time(run_date, cycle, forecast_hour)
        cfgrib_to_nomads = _build_cfgrib_to_nomads_map(variables)
        records: list[dict] = []

        for ds in datasets:
            matched_cfgrib_vars = sorted(set(ds.data_vars) & set(cfgrib_to_nomads))
            if not matched_cfgrib_vars:
                continue

            for cfgrib_var_name in matched_cfgrib_vars:
                nomads_var_name = cfgrib_to_nomads[cfgrib_var_name]
                var_records = self._extract_variable_records(
                    ds=ds,
                    cfgrib_var_name=cfgrib_var_name,
                    nomads_var_name=nomads_var_name,
                    bounding_box=bounding_box,
                    run_date=run_date,
                    cycle=cycle,
                    forecast_hour=forecast_hour,
                    valid_time=valid_time,
                    grid_step=grid_step,
                )
                records.extend(var_records)

        logger.debug(
            "Parsed %d records from %s (run=%s, cycle=%s, fh=%d)",
            len(records),
            file_path,
            run_date,
            cycle,
            forecast_hour,
        )
        return records

    def _open_grib_datasets(self, file_path: str) -> list[xr.Dataset]:
        """Open a GRIB2 file and return all datasets.

        cfgrib.open_datasets() returns multiple datasets because a single GRIB2 file
        contains messages at different level types (e.g. surface, isobaric, height above
        ground). Each dataset groups messages by level type.
        """
        try:
            return cfgrib.open_datasets(file_path)
        except Exception:
            logger.exception("Failed to open GRIB2 file: %s", file_path)
            return []

    _LAT_CANDIDATES: ClassVar[list[str]] = ["latitude", "lat"]
    _LON_CANDIDATES: ClassVar[list[str]] = ["longitude", "lon"]

    def _extract_variable_records(  # noqa: PLR0913
        self,
        ds: xr.Dataset,
        cfgrib_var_name: str,
        nomads_var_name: str,
        bounding_box: dict,
        run_date: str,
        cycle: str,
        forecast_hour: int,
        valid_time: str,
        grid_step: int,
    ) -> list[dict]:
        """Extract flat records for a single variable from an xarray Dataset.

        Uses vectorized numpy operations to convert the 2D grid to records,
        avoiding nested Python loops over individual grid points.
        """
        da = ds[cfgrib_var_name]
        level = self._extract_level_string(ds, cfgrib_var_name)
        units = da.attrs.get("units", "unknown")

        # Resolve dimension names once for the pipeline
        lat_dim = self._find_dimension_name(da, self._LAT_CANDIDATES)
        lon_dim = self._find_dimension_name(da, self._LON_CANDIDATES)
        if lat_dim is None or lon_dim is None:
            logger.warning(
                "Could not find lat/lon dimensions in DataArray %s; skipping",
                da.name,
            )
            return []

        da = self._apply_bounding_box(da, bounding_box, lat_dim, lon_dim)
        if da.size == 0:
            return []

        da = self._apply_grid_step(da, grid_step, lat_dim, lon_dim)

        return self._dataarray_to_records(
            da=da,
            run_date=run_date,
            cycle=cycle,
            forecast_hour=forecast_hour,
            valid_time=valid_time,
            nomads_var_name=nomads_var_name,
            level=level,
            units=units,
            lat_dim=lat_dim,
            lon_dim=lon_dim,
        )

    def _apply_bounding_box(
        self,
        da: xr.DataArray,
        bounding_box: dict,
        lat_dim: str,
        lon_dim: str,
    ) -> xr.DataArray:
        """Subset a DataArray to the configured geographic bounding box.

        GRIB2 longitude convention is 0-360. If the bounding box uses negative
        longitudes (e.g. -125 for western US), we convert to 0-360 for slicing.
        Latitude runs north (90) to south (-90), so slice(north, south) works.
        """
        north = bounding_box["north"]
        south = bounding_box["south"]
        west = bounding_box["west"] % 360
        east = bounding_box["east"] % 360

        # Determine latitude sort order (GRIB2 is typically 90→-90 descending,
        # but NOMADS-filtered subregions may be ascending 24→50)
        lat_values = da[lat_dim].values
        lat_ascending = len(lat_values) > 1 and lat_values[0] < lat_values[-1]
        slices = {lat_dim: slice(south, north)} if lat_ascending else {lat_dim: slice(north, south)}

        if west < east:
            slices[lon_dim] = slice(west, east)
        else:
            # Bounding box crosses the antimeridian — requires two slices for
            # full correctness (west→360 + 0→east). Current approach works for
            # CONUS but not for boxes spanning the Pacific.
            slices[lon_dim] = slice(west, east + 360)

        return da.sel(indexers=slices)

    def _apply_grid_step(
        self,
        da: xr.DataArray,
        grid_step: int,
        lat_dim: str,
        lon_dim: str,
    ) -> xr.DataArray:
        """Subsample the grid by taking every Nth point in lat and lon."""
        if grid_step <= 1:
            return da

        indexers = {}
        if lat_dim in da.dims:
            indexers[lat_dim] = da[lat_dim].values[::grid_step]
        if lon_dim in da.dims:
            indexers[lon_dim] = da[lon_dim].values[::grid_step]

        return da.sel(indexers=indexers) if indexers else da

    def _dataarray_to_records(  # noqa: PLR0913
        self,
        da: xr.DataArray,
        run_date: str,
        cycle: str,
        forecast_hour: int,
        valid_time: str,
        nomads_var_name: str,
        level: str,
        units: str,
        lat_dim: str,
        lon_dim: str,
    ) -> list[dict]:
        """Convert a 2D DataArray to flat records using vectorized operations.

        Instead of nested Python loops over lat/lon, we use numpy meshgrid
        and ravel to vectorize the conversion.
        """
        lats = da[lat_dim].values
        lons = da[lon_dim].values
        values = da.values

        # Handle case where DataArray has extra dimensions (squeeze them)
        while values.ndim > 2:  # noqa: PLR2004
            values = values[0]

        if values.shape != (len(lats), len(lons)):
            logger.warning(
                "Shape mismatch for %s: values=%s, expected=(%d, %d)",
                nomads_var_name,
                values.shape,
                len(lats),
                len(lons),
            )
            return []

        # Vectorized: create meshgrid and flatten
        lon_grid, lat_grid = np.meshgrid(lons, lats)
        flat_lats = np.round(lat_grid.ravel(), 4)
        flat_vals = values.ravel()

        # Convert GRIB2 longitude (0-360) to standard (-180 to 180)
        raw_lons = lon_grid.ravel()
        flat_lons = np.round(np.where(raw_lons > 180, raw_lons - 360, raw_lons), 4)  # noqa: PLR2004

        # Filter NaN values using boolean mask (vectorized)
        valid_mask = ~np.isnan(flat_vals)
        flat_lats = flat_lats[valid_mask]
        flat_lons = flat_lons[valid_mask]
        flat_vals = flat_vals[valid_mask]

        # Build records list
        records: list[dict] = [
            {
                "run_date": run_date,
                "cycle": cycle,
                "forecast_hour": forecast_hour,
                "valid_time": valid_time,
                "latitude": float(flat_lats[i]),
                "longitude": float(flat_lons[i]),
                "variable": nomads_var_name,
                "level": level,
                "value": float(flat_vals[i]),
                "units": units,
            }
            for i in range(len(flat_vals))
        ]

        return records

    def _extract_level_string(self, ds: xr.Dataset, var_name: str) -> str:
        """Extract a human-readable level string from the dataset metadata.

        Level information may be in coordinate variables (e.g. 'heightAboveGround')
        or dataset attributes (e.g. 'GRIB_typeOfLevel').
        """
        da = ds[var_name]

        level_coord_names = [
            "heightAboveGround",
            "isobaricInhPa",
            "surface",
            "depthBelowLandLayer",
            "heightAboveGroundLayer",
        ]
        for coord_name in level_coord_names:
            if coord_name in da.coords:
                coord_val = da.coords[coord_name].values
                scalar_val = float(coord_val) if coord_val.ndim == 0 else float(coord_val[0])
                return self._format_level_label(coord_name, scalar_val)

        type_of_level = da.attrs.get("GRIB_typeOfLevel", "")
        if type_of_level:
            return type_of_level

        return "unknown"

    @staticmethod
    def _format_level_label(coord_name: str, value: float) -> str:
        """Format a level coordinate name and value into a readable label."""
        level_labels = {
            "heightAboveGround": f"{int(value)} m above ground",
            "isobaricInhPa": f"{int(value)} mb",
            "surface": "surface",
            "depthBelowLandLayer": f"{value} m below ground",
            "heightAboveGroundLayer": f"{int(value)} m above ground",
        }
        return level_labels.get(coord_name, f"{coord_name}={value}")

    @staticmethod
    def _find_dimension_name(da: xr.DataArray, candidates: list[str]) -> str | None:
        """Find the first matching dimension or coordinate name from candidates."""
        all_names = set(da.dims) | set(da.coords)
        for name in candidates:
            if name in all_names:
                return name
        return None
