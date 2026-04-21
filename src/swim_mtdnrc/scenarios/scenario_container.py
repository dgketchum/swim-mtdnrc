"""Build a scenario container by cloning a source and overwriting NDVI for
substituted fields.

The scenario container is a copy of the source (typically a hindcast container)
with specific fields' NDVI replaced by crop-library curves.  All other data —
meteorology, properties, calibration parameters — remain identical to the source.
The resulting container can be run with ``swim run`` without any modifications.

Simulation runs and the default restart state from the source are stripped so
the model starts fresh (e.g., with cyclic spinup) rather than reusing an
initializer that was computed from the original NDVI.
"""

from __future__ import annotations

import os
import shutil
import tempfile

import fiona
from fiona.crs import CRS
import numpy as np
import zarr

from swimrs.container import SwimContainer

from swim_mtdnrc.clustering.crop_library import load_crop_library
from swim_mtdnrc.scenarios.scenario_spec import ScenarioSpec


def _copy_zarr_group(src_group: zarr.Group, dst_parent: zarr.Group, name: str) -> None:
    """Recursively copy a zarr group, preserving zarr-native dtypes.

    ``SwimContainer._copy_zarr_group`` infers dtype from ``child[:]``, which
    returns numpy object arrays for ``VariableLengthUTF8`` columns and fails
    on zarr v3.  This version reads the stored zarr dtype and passes it
    explicitly to ``create_array``.
    """
    if name in dst_parent:
        del dst_parent[name]
    dst_group = dst_parent.create_group(name)
    src_attrs = dict(src_group.attrs)
    if src_attrs:
        dst_group.attrs.update(src_attrs)
    for child_name, child in src_group.members():
        if isinstance(child, zarr.Array):
            data = child[:]
            zdtype = child.metadata.data_type
            # zarr v3 can't pass both data and dtype; create with
            # shape+dtype then write data in.
            arr = dst_group.create_array(
                child_name,
                shape=data.shape,
                dtype=zdtype,
            )
            arr[:] = data
            arr_attrs = dict(child.attrs)
            if arr_attrs:
                arr.attrs.update(arr_attrs)
        elif isinstance(child, zarr.Group):
            _copy_zarr_group(child, dst_group, child_name)


def _overwrite_field_ndvi(
    container: SwimContainer,
    field_idx: int,
    curve_366: np.ndarray,
    masks: tuple[str, ...],
) -> None:
    """Overwrite a single field's NDVI column with a tiled crop curve.

    The 366-day crop curve is tiled across the container's time axis by
    indexing with day-of-year, following the same pattern as
    ``_tile_ndvi_climatology`` in ``swimrs.container.project``.

    Parameters
    ----------
    container : SwimContainer
        Target container opened in append/write mode.
    field_idx : int
        0-based positional index of the field in the container's field array.
    curve_366 : np.ndarray, shape (366,)
        Full-year NDVI curve (DOY 1 at index 0 through DOY 366 at index 365).
    masks : tuple of str
        NDVI mask paths to overwrite (under ``derived/merged_ndvi/``).
    """
    time_index = container._time_index
    doys = time_index.dayofyear.values  # (n_days,) values 1-366
    tiled = curve_366[doys - 1]  # (n_days,) — DOY-indexed lookup

    for mask in masks:
        path = f"derived/merged_ndvi/{mask}"
        if path not in container._root:
            continue
        arr = container._root[path]
        arr[:, field_idx] = tiled.astype(arr.dtype)


def _export_source_geometry(source: SwimContainer) -> tuple[str, str]:
    """Export source container geometry to a temporary shapefile.

    Returns
    -------
    tuple of (shapefile_path, temp_dir)
        The caller must clean up temp_dir when done.
    """
    uids = list(source._root["geometry/uid"][:])
    if uids and isinstance(uids[0], bytes):
        uids = [u.decode("utf-8") for u in uids]

    lons = source._root["geometry/lon"][:]
    lats = source._root["geometry/lat"][:]

    tmp_dir = tempfile.mkdtemp(prefix="swim_scenario_shp_")
    shp_path = os.path.join(tmp_dir, "fields.shp")

    schema = {"geometry": "Point", "properties": {"FID": "str"}}
    with fiona.open(
        shp_path, "w", driver="ESRI Shapefile", schema=schema, crs=CRS.from_epsg(4326)
    ) as dst:
        for uid, lon, lat in zip(uids, lons, lats):
            dst.write(
                {
                    "geometry": {
                        "type": "Point",
                        "coordinates": (float(lon), float(lat)),
                    },
                    "properties": {"FID": str(uid)},
                }
            )

    return shp_path, tmp_dir


def _copy_group_from_source(
    source: SwimContainer, target: SwimContainer, group_name: str
) -> None:
    """Copy a zarr group from source to target container."""
    if group_name not in source._root:
        return
    _copy_zarr_group(source._root[group_name], target._root, group_name)


def create_scenario_container(
    spec: ScenarioSpec,
    output_path: str,
    overwrite: bool = False,
) -> str:
    """Build a scenario container by creating a fresh target and copying data.

    Creates a new container with the same fields, date range, and data as the
    source, but with NDVI overwritten for substituted fields.  Simulation runs
    and the default restart state from the source are NOT copied, so the model
    will start fresh.

    Parameters
    ----------
    spec : ScenarioSpec
        Parsed scenario specification with substitutions.
    output_path : str
        Path for the new scenario container.
    overwrite : bool
        If True, replace an existing container at *output_path*.

    Returns
    -------
    str
        Path to the created scenario container.

    Raises
    ------
    FileExistsError
        If *output_path* exists and *overwrite* is False.
    ValueError
        If validation fails (missing FIDs, unknown crops).
    """
    source_path = spec.source_container

    if os.path.exists(output_path):
        if overwrite:
            if os.path.isdir(output_path):
                shutil.rmtree(output_path)
            else:
                os.remove(output_path)
        else:
            raise FileExistsError(
                f"Scenario container already exists: {output_path}\n"
                "Use --overwrite to replace."
            )

    # Load crop library
    library = load_crop_library(spec.crop_library_path)

    # Open source through the container API (works for both zip and directory)
    source = SwimContainer.open(source_path, mode="r")
    target = None
    tmp_shp_dir = None

    try:
        # Export source geometry to temp shapefile for SwimContainer.create
        shp_path, tmp_shp_dir = _export_source_geometry(source)

        start_date = str(source.start_date.date())
        end_date = str(source.end_date.date())

        # Create fresh target container
        print(f"Creating scenario container: {output_path}")
        target = SwimContainer.create(
            output_path,
            fields_shapefile=shp_path,
            uid_column="FID",
            start_date=start_date,
            end_date=end_date,
            project_name=f"scenario_{spec.name}",
            overwrite=False,
        )

        # Overwrite geometry with source's original geometry (WKB polygons,
        # area_m2, lat/lon, shapefile properties).  SwimContainer.create()
        # derived these from the temp point shapefile, which is not faithful
        # to the source field geometries.
        _copy_zarr_group(source._root["geometry"], target._root, "geometry")
        print("  Copied geometry from source (preserves original WKB/area)")

        # Copy static data (properties, calibration params, select dynamics).
        # We use our dtype-aware _copy_zarr_group instead of
        # target.copy_static_groups(source) because the swim-rs version
        # can't handle VariableLengthUTF8 arrays (e.g., irr_yearly).
        for group_name in ("properties", "calibration"):
            if group_name in source._root:
                _copy_zarr_group(source._root[group_name], target._root, group_name)

        # Selectively copy dynamics: ke_max, kc_max, gwsub_data carry
        # forward; irr_data is recomputed after NDVI overwrite.
        # gwsub_data is VariableLengthUTF8, so use shape+dtype pattern.
        if "derived/dynamics" in source._root:
            target._root.require_group("derived/dynamics")
            dst_dyn = target._root["derived/dynamics"]
            for arr_name in ("ke_max", "kc_max", "gwsub_data"):
                src_path = f"derived/dynamics/{arr_name}"
                if src_path in source._root:
                    if arr_name in dst_dyn:
                        del dst_dyn[arr_name]
                    src_arr = source._root[src_path]
                    new_arr = dst_dyn.create_array(
                        arr_name,
                        shape=src_arr.shape,
                        dtype=src_arr.metadata.data_type,
                    )
                    new_arr[:] = src_arr[:]
        print("  Copied static groups (properties, calibration, dynamics)")

        # Copy meteorology and snow groups
        for group in ("meteorology", "snow"):
            _copy_group_from_source(source, target, group)
            if group in source._root:
                print(f"  Copied {group}")

        # Copy remote_sensing group (raw NDVI needed by some health checks)
        _copy_group_from_source(source, target, "remote_sensing")

        # Copy merged NDVI (the array the model actually reads)
        if "derived/merged_ndvi" in source._root:
            _copy_zarr_group(
                source._root["derived/merged_ndvi"],
                target._root.require_group("derived"),
                "merged_ndvi",
            )
            print("  Copied derived/merged_ndvi")

        # Build UID -> index mapping
        uids = list(target._root["geometry/uid"][:])
        if uids and isinstance(uids[0], bytes):
            uids = [u.decode("utf-8") for u in uids]
        uid_to_idx = {uid: i for i, uid in enumerate(uids)}

        # Validate
        messages = spec.validate(library, uids)
        errors = [m for m in messages if m.startswith("ERROR:")]
        if errors:
            raise ValueError("Scenario validation failed:\n" + "\n".join(errors))
        for msg in messages:
            print(f"  {msg}")

        # Determine masks present (irr/inv_irr only — no_mask not supported)
        masks = tuple(
            m for m in ("irr", "inv_irr") if f"derived/merged_ndvi/{m}" in target._root
        )
        if not masks:
            raise ValueError(
                "No derived/merged_ndvi/{irr,inv_irr} arrays found in container. "
                "Scenario substitution requires irrigation-masked NDVI."
            )

        # Apply substitutions
        n_applied = 0
        for sub in spec.substitutions:
            if sub.fid not in uid_to_idx:
                continue
            if sub.target_crop not in library:
                continue

            field_idx = uid_to_idx[sub.fid]
            curve = library[sub.target_crop]["curve_366"]
            _overwrite_field_ndvi(target, field_idx, curve, masks=masks)
            n_applied += 1

        print(f"Overwrote NDVI for {n_applied} fields across masks {masks}")

        # Recompute irrigation windows from modified NDVI
        print("Recomputing irrigation windows (irr_data)...")
        target.compute.compute_irr_data(
            masks=masks,
            use_mask=True,
            overwrite=True,
        )

        # Record provenance
        target.provenance.record(
            "create_scenario_container",
            source=source_path,
            params={
                "scenario_name": spec.name,
                "n_substitutions": n_applied,
                "crops_used": sorted({s.target_crop for s in spec.substitutions}),
                "crop_library": spec.crop_library_path,
            },
        )

        target.save()

    finally:
        if target is not None:
            target.close()
        source.close()
        if tmp_shp_dir is not None:
            shutil.rmtree(tmp_shp_dir, ignore_errors=True)

    print(f"Scenario container ready: {output_path}")
    return output_path
