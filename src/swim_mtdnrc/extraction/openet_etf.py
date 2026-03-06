# OpenET ETf extraction for state field shapefiles (NV, NM, etc.).
#
# Replicates the SID ETf extraction workflow (sid_etf.py) for arbitrary field
# boundaries.  Groups fields by county FIPS for batched Earth Engine export.
# County FIPS can come from an existing column or a spatial join with
# /nas/boundaries/counties/western_17_states_counties_wgs.shp.

import os
import sys
import time

import ee
import geopandas as gpd

from swimrs.data_extraction.ee.common import (
    shapefile_to_feature_collection,
)
from swimrs.data_extraction.ee.ee_utils import is_authorized

from swim_mtdnrc.extraction.sid_etf import (
    IRR,
    extract_etf,
)

COUNTIES_SHP = "/nas/boundaries/counties/western_17_states_counties_wgs.shp"
CHUNK_SUFFIXES = "abcdefghijklmnopqrstuvwxyz"


def _chunk_list(lst, n):
    """Split list into n roughly equal chunks."""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


def assign_county_fips(fields_shp, feature_id, fips_col=None, state_fips=None):
    """Load fields shapefile and assign county FIPS codes.

    If fips_col exists in the shapefile, use it directly.  Otherwise spatial-join
    field centroids against the western US counties shapefile.

    Parameters
    ----------
    fields_shp : str
        Path to the fields shapefile.
    feature_id : str
        Column name for the unique field identifier.
    fips_col : str or None
        Existing column with 5-digit county FIPS codes.
    state_fips : str or None
        2-digit state FIPS to pre-filter counties for spatial join.

    Returns
    -------
    geopandas.GeoDataFrame
        Fields in EPSG:4326 with a ``COUNTY_FIPS`` column.
    """
    gdf = gpd.read_file(fields_shp, engine="fiona")
    if gdf.crs and not gdf.crs.is_geographic:
        gdf = gdf.to_crs(4326)

    if fips_col and fips_col in gdf.columns:
        gdf["COUNTY_FIPS"] = gdf[fips_col].astype(str).str.zfill(5)
    else:
        counties = gpd.read_file(COUNTIES_SHP, engine="fiona")
        if state_fips:
            counties = counties[counties["STATEFP"] == state_fips]
        pts = gdf.copy()
        pts.geometry = pts.geometry.representative_point()
        joined = gpd.sjoin(
            pts,
            counties[["GEOID", "NAME", "geometry"]],
            how="left",
            predicate="within",
        )
        gdf["COUNTY_FIPS"] = joined["GEOID"].values
        gdf["COUNTY_NAME"] = joined["NAME"].values
        missing = gdf["COUNTY_FIPS"].isna().sum()
        if missing:
            print(f"WARNING: {missing} fields not assigned to a county (dropped)")
            gdf = gdf.dropna(subset=["COUNTY_FIPS"])

    return gdf


def run(
    shapefile,
    feature_id,
    file_prefix,
    fips_col=None,
    state_fips=None,
    counties=None,
    select_fids=None,
    max_fields=1000,
    chunk_index=None,
    models=None,
    mask_types=None,
    start_yr=2016,
    end_yr=2025,
    years=None,
    dest="bucket",
    bucket="wudr",
    project="ee-dgketchum",
):
    """Run county-batched OpenET ETf extraction for a state shapefile.

    Counties with more than *max_fields* fields are automatically split into
    chunks of at most that size (labeled with letter suffixes a, b, c, ...).

    Parameters
    ----------
    shapefile : str
        Path to the fields shapefile.
    feature_id : str
        Column name for the unique field identifier.
    file_prefix : str
        Prefix for GCS / local output paths (e.g. ``"nv"``, ``"nm"``).
    fips_col : str or None
        Column with existing FIPS codes (skip spatial join).
    state_fips : str or None
        2-digit state FIPS for county filter during spatial join.
    counties : list[str] or None
        Restrict to these 5-digit county FIPS codes.
    select_fids : list or None
        Restrict to these feature IDs before county grouping.
    max_fields : int
        Maximum fields per chunk.  Counties exceeding this are auto-split.
    chunk_index : int or None
        Run only this chunk (0-indexed).  Useful with ``--counties`` to
        target a single chunk of a single county.
    models : list[str] or None
        OpenET model names.  Defaults to ``["ensemble"]``.
    mask_types : list[str] or None
        Irrigation mask types.  Defaults to ``["irr", "inv_irr"]``.
    start_yr, end_yr : int
        Year range for extraction.
    years : list[int] or None
        Explicit year list (overrides start_yr / end_yr).
    dest : str
        ``"bucket"`` or ``"local"``.
    bucket : str
        GCS bucket name.
    project : str
        Earth Engine project ID.
    """
    if models is None:
        models = ["ensemble"]
    if mask_types is None:
        mask_types = ["irr", "inv_irr"]

    sys.setrecursionlimit(5000)
    is_authorized(project)

    irr_coll = ee.ImageCollection(IRR)
    remap = irr_coll.filterDate("1987-01-01", "2026-01-01").select("classification")
    irr_min_yr_mask = remap.map(lambda img: img.lt(1)).sum().gte(5)
    print("Computed irr_min_yr_mask (live)")

    gdf = assign_county_fips(shapefile, feature_id, fips_col, state_fips)
    if select_fids is not None:
        gdf = gdf[gdf[feature_id].isin(select_fids)]
        print(f"Filtered to {len(gdf)} fields by --fids")
    county_fids = gdf.groupby("COUNTY_FIPS")[feature_id].apply(list).to_dict()
    print(f"Loaded {len(gdf)} fields in {len(county_fids)} counties")

    if counties:
        county_fids = {k: v for k, v in county_fids.items() if k in counties}
        print(f"Filtered to {len(county_fids)} selected counties")

    for county_fips, fids in sorted(county_fids.items()):
        n_chunks = -(-len(fids) // max_fields)  # ceil division
        if n_chunks > 1:
            chunk_groups = _chunk_list(fids, n_chunks)
            print(f"County {county_fips}: {len(fids)} fields -> {n_chunks} chunks")
        else:
            chunk_groups = [fids]

        for ci, chunk_fids in enumerate(chunk_groups):
            if chunk_index is not None and ci != chunk_index:
                continue

            suffix = CHUNK_SUFFIXES[ci] if len(chunk_groups) > 1 else ""
            label = f"{county_fips}{suffix}"

            for model in models:
                for mask_type in mask_types:
                    print(
                        f"\n=== {label} ({len(chunk_fids)} fields) "
                        f"model={model} mask={mask_type} ==="
                    )

                    fc = shapefile_to_feature_collection(
                        shapefile, feature_id, select=chunk_fids
                    )

                    start_time = time.time()
                    result = extract_etf(
                        fc,
                        irr_coll,
                        irr_min_yr_mask,
                        model=model,
                        mask_type=mask_type,
                        start_yr=start_yr,
                        end_yr=end_yr,
                        years=years,
                        feature_id=feature_id,
                        dest=dest,
                        bucket=bucket,
                        file_prefix=f"{file_prefix}/{label}",
                    )
                    elapsed = time.time() - start_time

                    if result is not None:
                        out_dir = f"/data/ssd2/swim/{file_prefix}"
                        os.makedirs(out_dir, exist_ok=True)
                        out_csv = os.path.join(
                            out_dir, f"{label}_{model}_etf_{mask_type}.csv"
                        )
                        result.to_csv(out_csv)
                        print(
                            f"  {result.shape[0]} fields x {result.shape[1]} scenes "
                            f"in {elapsed:.1f}s -> {out_csv}"
                        )
                    else:
                        print(f"  Export tasks submitted in {elapsed:.1f}s")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="OpenET ETf extraction for state field shapefiles"
    )
    parser.add_argument("--shapefile", required=True, help="Path to fields shapefile")
    parser.add_argument("--feature-id", required=True, help="Feature ID column name")
    parser.add_argument(
        "--file-prefix",
        required=True,
        help="Output prefix for GCS paths (e.g. 'nv', 'nm')",
    )
    parser.add_argument(
        "--fips-col",
        default=None,
        help="Column with county FIPS codes (skip spatial join)",
    )
    parser.add_argument(
        "--state-fips",
        default=None,
        help="2-digit state FIPS to filter counties during spatial join",
    )
    parser.add_argument(
        "--counties",
        type=str,
        default=None,
        help="Comma-separated 5-digit county FIPS codes to process",
    )
    parser.add_argument(
        "--fids",
        type=str,
        default=None,
        help="Comma-separated feature IDs to extract (filters shapefile)",
    )
    parser.add_argument(
        "--max-fields",
        type=int,
        default=1000,
        help="Max fields per chunk (counties exceeding this are auto-split)",
    )
    parser.add_argument(
        "--chunk-index",
        type=int,
        default=None,
        help="Run only this chunk (0-indexed, use with --counties)",
    )
    parser.add_argument(
        "--models",
        type=str,
        default="ensemble",
        help="Comma-separated OpenET model names",
    )
    parser.add_argument(
        "--mask-types",
        type=str,
        default="irr,inv_irr",
        help="Comma-separated mask types",
    )
    parser.add_argument("--start-yr", type=int, default=2016)
    parser.add_argument("--end-yr", type=int, default=2025)
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma-separated years (overrides start/end)",
    )
    parser.add_argument("--dest", choices=["bucket", "local"], default="bucket")
    parser.add_argument("--bucket", type=str, default="wudr")
    parser.add_argument("--project", type=str, default="ee-dgketchum")
    args = parser.parse_args()

    year_list = [int(y) for y in args.years.split(",")] if args.years else None
    county_list = (
        [c.strip() for c in args.counties.split(",")] if args.counties else None
    )
    fid_list = [int(f) for f in args.fids.split(",")] if args.fids else None

    run(
        shapefile=args.shapefile,
        feature_id=args.feature_id,
        file_prefix=args.file_prefix,
        fips_col=args.fips_col,
        state_fips=args.state_fips,
        counties=county_list,
        select_fids=fid_list,
        max_fields=args.max_fields,
        chunk_index=args.chunk_index,
        models=[m.strip() for m in args.models.split(",")],
        mask_types=[m.strip() for m in args.mask_types.split(",")],
        start_yr=args.start_yr,
        end_yr=args.end_yr,
        years=year_list,
        dest=args.dest,
        bucket=args.bucket,
        project=args.project,
    )


if __name__ == "__main__":
    main()
