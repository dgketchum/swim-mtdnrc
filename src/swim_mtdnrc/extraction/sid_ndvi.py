# NDVI extraction for the Montana SID 1991-2023 using swimrs EE modules.
# Outputs raw per-county DataFrames as CSV (no netCDF post-processing).

import os
import sys
import time

import ee
import geopandas as gpd
import pandas as pd

from swimrs.data_extraction.ee.common import (
    export_table,
    shapefile_to_feature_collection,
)
from swimrs.data_extraction.ee.ee_utils import is_authorized, landsat_masked

WAIT_MINUTES = 10
MAX_RETRIES = 6

IRR = "projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp"
IRR_MIN_YR_ASSET = "projects/ee-dgketchum/assets/SID/irr_min_yr_mask"
FEATURE_ID = "FID"
SHAPEFILE = "/nas/Montana/statewide_irrigation_dataset/statewide_irrigation_dataset_15FEB2024_aea.shp"


def export_irr_min_yr_mask(feature_coll):
    """Export the multi-year IrrMapper mask as an EE asset (one-time).

    The mask identifies pixels irrigated in >= 5 of 37 years (1987-2023).
    Pre-computing it as an asset eliminates the 37-image computation graph
    that otherwise causes 'too many bands' errors in toBands() exports.
    """
    irr_coll = ee.ImageCollection(IRR)
    remap = irr_coll.filterDate("1987-01-01", "2024-12-31").select("classification")
    irr_min_yr_mask = remap.map(lambda img: img.lt(1)).sum().gte(5).toByte()

    region = feature_coll.geometry().bounds()
    task = ee.batch.Export.image.toAsset(
        image=irr_min_yr_mask,
        description="sid_irr_min_yr_mask",
        assetId=IRR_MIN_YR_ASSET,
        region=region,
        scale=30,
        maxPixels=1e13,
    )
    task.start()
    print(f"Exporting irr_min_yr_mask asset — task ID: {task.id}")
    print(f"  Asset path: {IRR_MIN_YR_ASSET}")
    print(f"  Monitor: earthengine task info {task.id}")
    return task


def extract_ndvi(
    feature_coll,
    mask_type="irr",
    start_yr=2004,
    end_yr=2024,
    years=None,
    half=None,
    feature_id="FID",
    dest="local",
    bucket="wudr",
    file_prefix="sid",
):
    """Extract mean NDVI per field using harmonized Landsat.

    Uses SBAF-adjusted NIR_H/RED_H for consistent cross-sensor NDVI.

    When dest="local", uses ee.data.computeFeatures for synchronous return
    and returns a concatenated DataFrame.

    When dest="bucket", starts one ee.batch export task per year to GCS
    and returns None.

    Parameters
    ----------
    years : list[int] or None
        Explicit list of years to process.  Overrides start_yr/end_yr.
    half : str or None
        "h1" for Jan-Jun, "h2" for Jul-Dec.  Reduces band count per export.
    """
    irr_coll = ee.ImageCollection(IRR)

    # Load pre-computed multi-year mask asset to avoid graph explosion in
    # toBands(). Falls back to live computation if asset doesn't exist.
    try:
        irr_min_yr_mask = ee.Image(IRR_MIN_YR_ASSET)
        irr_min_yr_mask.getInfo()  # verify asset exists
        print("  Using pre-computed irr_min_yr_mask asset")
    except ee.ee_exception.EEException:
        print("  WARNING: irr_min_yr_mask asset not found, computing live")
        remap = irr_coll.filterDate("1987-01-01", "2024-12-31").select("classification")
        irr_min_yr_mask = remap.map(lambda img: img.lt(1)).sum().gte(5)

    if years is None:
        years = list(range(start_yr, end_yr + 1))

    dfs = []

    for year in years:
        irr = (
            irr_coll.filterDate(f"{year}-01-01", f"{year}-12-31")
            .select("classification")
            .mosaic()
        )
        irr_mask = irr_min_yr_mask.updateMask(irr.lt(1))

        coll = landsat_masked(year, feature_coll, harmonize=True).select(
            ["NIR_H", "RED_H"]
        )

        if half == "h1":
            coll = coll.filterDate(f"{year}-01-01", f"{year}-07-01")
        elif half == "h2":
            coll = coll.filterDate(f"{year}-07-01", f"{year + 1}-01-01")

        # Apply mask per-image BEFORE toBands to avoid EE graph-expansion
        # bug where mask(irr_mask) + reduceRegions on a toBands image
        # causes EE to count >5000 bands from the IrrMapper .sum() graph.
        if mask_type == "irr":
            coll = coll.map(
                lambda x, _m=irr_mask: x.normalizedDifference(
                    ["NIR_H", "RED_H"]
                ).updateMask(_m)
            )
        elif mask_type == "inv_irr":
            coll = coll.map(
                lambda x, _i=irr: x.normalizedDifference(["NIR_H", "RED_H"]).updateMask(
                    _i.gt(0)
                )
            )
        else:
            coll = coll.map(lambda x: x.normalizedDifference(["NIR_H", "RED_H"]))

        for attempt in range(MAX_RETRIES):
            try:
                scenes = coll.aggregate_histogram("system:index").getInfo()
                break
            except ee.ee_exception.EEException as exc:
                if attempt == MAX_RETRIES - 1:
                    raise
                print(f"  getInfo failed ({exc}), retrying in {WAIT_MINUTES} min...")
                time.sleep(WAIT_MINUTES * 60)

        band_names = sorted(scenes.keys())
        half_tag = f" ({half})" if half else ""
        print(f"  {year}{half_tag}: {len(band_names)} scenes")
        bands = coll.toBands().rename(band_names)

        data = bands.reduceRegions(
            collection=feature_coll,
            reducer=ee.Reducer.mean(),
            scale=30,
            tileScale=8,
        )

        if dest == "local":
            data_df = ee.data.computeFeatures(
                {"expression": data, "fileFormat": "PANDAS_DATAFRAME"}
            )
            data_df.index = data_df[feature_id]
            data_df.drop(columns=["geo"], inplace=True, errors="ignore")
            dfs.append(data_df)
        elif dest == "bucket":
            half_suffix = f"_{half}" if half else ""
            desc = f"ndvi_{mask_type}_{year}{half_suffix}"
            selectors = [feature_id] + band_names
            for attempt in range(MAX_RETRIES):
                try:
                    export_table(
                        data,
                        desc=desc,
                        selectors=selectors,
                        dest="bucket",
                        bucket=bucket,
                        fn_prefix=f"{file_prefix}/ndvi/{mask_type}/{desc}",
                    )
                    break
                except ee.ee_exception.EEException as exc:
                    if attempt == MAX_RETRIES - 1:
                        raise
                    print(f"  export failed ({exc}), retrying in {WAIT_MINUTES} min...")
                    time.sleep(WAIT_MINUTES * 60)

    if dest == "local":
        return pd.concat(dfs, axis=1)
    return None


def _chunk_list(lst, n):
    """Split list into n roughly equal chunks."""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


CHUNK_SUFFIXES = "abcdefghijklmnopqrstuvwxyz"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SID NDVI extraction")
    parser.add_argument(
        "--counties", type=str, default=None, help="Comma-separated county numbers"
    )
    parser.add_argument(
        "--chunks", type=int, default=1, help="Split each county into N groups"
    )
    parser.add_argument(
        "--chunk-index",
        type=int,
        default=None,
        help="Run only this chunk (0-indexed, e.g. 2 for 'c')",
    )
    parser.add_argument("--start-yr", type=int, default=1991)
    parser.add_argument("--end-yr", type=int, default=2023)
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma-separated years (overrides start/end)",
    )
    parser.add_argument(
        "--mask-types",
        type=str,
        default="irr,inv_irr",
        help="Comma-separated mask types",
    )
    parser.add_argument(
        "--half",
        choices=["h1", "h2"],
        default=None,
        help="Export half-year: h1=Jan-Jun, h2=Jul-Dec",
    )
    parser.add_argument("--dest", choices=["bucket", "local"], default="bucket")
    parser.add_argument("--bucket", type=str, default="wudr")
    parser.add_argument(
        "--project", type=str, default="ee-dgketchum", help="EE project ID"
    )
    parser.add_argument(
        "--export-mask",
        action="store_true",
        help="Export irr_min_yr_mask as EE asset and exit",
    )
    args = parser.parse_args()

    year_list = [int(y) for y in args.years.split(",")] if args.years else None
    mask_types = [m.strip() for m in args.mask_types.split(",")]

    root = "/data/ssd2/swim/sid"
    os.makedirs(root, exist_ok=True)
    sys.setrecursionlimit(5000)

    is_authorized(args.project)

    if args.export_mask:
        fc = shapefile_to_feature_collection(SHAPEFILE, FEATURE_ID)
        export_irr_min_yr_mask(fc)
        sys.exit(0)

    gdf = gpd.read_file(SHAPEFILE)
    county_fids = gdf.groupby("COUNTY_NO")[FEATURE_ID].apply(list).to_dict()

    if args.counties:
        selected = {int(c.strip()) for c in args.counties.split(",")}
        county_fids = {k: v for k, v in county_fids.items() if k in selected}

    for county_no, fids in county_fids.items():
        county = f"{county_no:03d}"
        name = gdf.loc[gdf["COUNTY_NO"] == county_no, "COUNTYNAME"].iloc[0]

        if args.chunks > 1:
            chunks = _chunk_list(fids, args.chunks)
        else:
            chunks = [fids]

        for ci, chunk_fids in enumerate(chunks):
            if args.chunk_index is not None and ci != args.chunk_index:
                continue

            suffix = CHUNK_SUFFIXES[ci] if len(chunks) > 1 else ""
            label = f"{county}{suffix}"

            for mask_type in mask_types:
                print(
                    f"\n=== {label} ({name}, {len(chunk_fids)} fields) mask={mask_type} ==="
                )

                fc = shapefile_to_feature_collection(
                    SHAPEFILE, FEATURE_ID, select=chunk_fids
                )

                start_time = time.time()
                result = extract_ndvi(
                    fc,
                    mask_type=mask_type,
                    start_yr=args.start_yr,
                    end_yr=args.end_yr,
                    years=year_list,
                    half=args.half,
                    feature_id=FEATURE_ID,
                    dest=args.dest,
                    bucket=args.bucket,
                    file_prefix=f"sid/{label}",
                )
                elapsed = time.time() - start_time

                if result is not None:
                    out_csv = os.path.join(root, f"{label}_ndvi_{mask_type}.csv")
                    result.to_csv(out_csv)
                    print(
                        f"  {result.shape[0]} fields x {result.shape[1]} scenes "
                        f"in {elapsed:.1f}s -> {out_csv}"
                    )
                else:
                    print(f"  Export tasks submitted in {elapsed:.1f}s")

# ========================= EOF ====================================================================
