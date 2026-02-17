# IrrMapper extraction for the Montana SID 1987-2024.
# Exports per-county irrigation fraction CSVs to GCS bucket.

import sys
import time

import ee
import geopandas as gpd

from swimrs.data_extraction.ee.common import (
    export_table,
    shapefile_to_feature_collection,
)
from swimrs.data_extraction.ee.ee_utils import is_authorized

WAIT_MINUTES = 10
MAX_RETRIES = 6

IRR = "projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp"
FEATURE_ID = "FID"
SHAPEFILE = "/nas/Montana/statewide_irrigation_dataset/statewide_irrigation_dataset_15FEB2024_aea.shp"


def extract_irrigation(
    feature_coll,
    feature_id="FID",
    start_yr=1987,
    end_yr=2024,
    dest="bucket",
    bucket="wudr",
    file_prefix="sid",
):
    """Extract mean irrigation fraction per field per year using IrrMapper.

    Builds a multi-band image with one band per year (irr_{year}), where each
    pixel is binary irrigated (classification < 1). reduceRegions with mean
    gives the irrigation fraction per field.

    When dest="bucket", starts an ee.batch export task to GCS.
    """
    irr_coll = ee.ImageCollection(IRR)

    irr_img = None
    selectors = [feature_id]

    for year in range(start_yr, end_yr + 1):
        irr = (
            irr_coll.filterDate(f"{year}-01-01", f"{year}-12-31")
            .select("classification")
            .mosaic()
        )
        band = irr.lt(1).rename(f"irr_{year}").int()
        name = f"irr_{year}"
        selectors.append(name)

        if irr_img is None:
            irr_img = band
        else:
            irr_img = irr_img.addBands(band)

    data = irr_img.reduceRegions(
        collection=feature_coll,
        reducer=ee.Reducer.mean(),
        scale=30,
    )

    desc = f"irr_{file_prefix.replace('/', '_')}"

    if dest == "bucket":
        for attempt in range(MAX_RETRIES):
            try:
                export_table(
                    data,
                    desc=desc,
                    selectors=selectors,
                    dest="bucket",
                    bucket=bucket,
                    fn_prefix=f"{file_prefix}/properties/{desc}",
                )
                break
            except ee.ee_exception.EEException as exc:
                if attempt == MAX_RETRIES - 1:
                    raise
                print(f"  export failed ({exc}), retrying in {WAIT_MINUTES} min...")
                time.sleep(WAIT_MINUTES * 60)


def _chunk_list(lst, n):
    """Split list into n roughly equal chunks."""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


CHUNK_SUFFIXES = "abcdefghijklmnopqrstuvwxyz"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SID IrrMapper extraction")
    parser.add_argument(
        "--counties", type=str, default=None, help="Comma-separated county numbers"
    )
    parser.add_argument(
        "--chunks", type=int, default=1, help="Split each county into N groups"
    )
    parser.add_argument(
        "--project", type=str, default="ee-dgketchum", help="EE project ID"
    )
    args = parser.parse_args()

    sys.setrecursionlimit(5000)

    is_authorized(args.project)

    dest = "bucket"
    bucket = "wudr"

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
            suffix = CHUNK_SUFFIXES[ci] if len(chunks) > 1 else ""
            label = f"{county}{suffix}"

            print(f"\n=== {label} ({name}, {len(chunk_fids)} fields) ===")

            fc = shapefile_to_feature_collection(
                SHAPEFILE, FEATURE_ID, select=chunk_fids
            )

            start_time = time.time()
            extract_irrigation(
                fc,
                feature_id=FEATURE_ID,
                dest=dest,
                bucket=bucket,
                file_prefix=f"sid/{label}",
            )
            elapsed = time.time() - start_time
            print(f"  Export task submitted in {elapsed:.1f}s")

# ========================= EOF ====================================================================
