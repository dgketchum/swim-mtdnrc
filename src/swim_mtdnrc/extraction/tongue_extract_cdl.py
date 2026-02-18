"""CDL crop type extraction for Tongue River Basin (2,084 fields).

Extracts the modal (most common) USDA Cropland Data Layer class per field
per year using reduceRegions with Reducer.mode().

CDL is available 2008-2024 at 30m. Each year is a single ee.Image
at 'USDA/NASS/CDL/{year}'.

Usage:
    python -m swim_mtdnrc.extraction.tongue_extract_cdl --dest bucket
    python -m swim_mtdnrc.extraction.tongue_extract_cdl --dest local
"""

import argparse
import os
import sys

import ee

from swimrs.data_extraction.ee.common import (
    export_table,
    shapefile_to_feature_collection,
)
from swimrs.data_extraction.ee.ee_utils import is_authorized

FEATURE_ID = "FID"
SHAPEFILE = "/nas/swim/examples/tongue_new/data/gis/tongue_fields_gfid.shp"
OUTPUT_DIR = "/nas/swim/examples/tongue_new/data/landsat/extracts/cdl"

CDL_FIRST_YEAR = 2008
CDL_LAST_YEAR = 2024


def extract_cdl(
    feature_coll,
    start_yr=CDL_FIRST_YEAR,
    end_yr=CDL_LAST_YEAR,
    feature_id="FID",
    dest="bucket",
    bucket="wudr",
    file_prefix="tongue",
):
    """Extract modal CDL crop class per field, all years in one export.

    Builds a multi-band image (one band per year) and runs a single
    reduceRegions with Reducer.mode() to get the dominant crop type.

    Parameters
    ----------
    feature_coll : ee.FeatureCollection
    start_yr, end_yr : int
    feature_id : str
    dest : {'bucket', 'local'}
    bucket : str
    file_prefix : str
    """
    years = list(range(start_yr, end_yr + 1))
    selectors = [feature_id]
    first = True
    crops = None

    for year in years:
        band_name = f"crop_{year}"
        selectors.append(band_name)
        crop = ee.Image(f"USDA/NASS/CDL/{year}").select("cropland").rename(band_name)

        if first:
            crops = crop
            first = False
        else:
            crops = crops.addBands(crop)

    print(f"Reducing {len(years)} CDL years over {feature_id} features...")

    modes = crops.reduceRegions(
        collection=feature_coll,
        reducer=ee.Reducer.mode(),
        scale=30,
    )

    desc = f"cdl_crop_type_{start_yr}_{end_yr}"

    if dest == "bucket":
        export_table(
            modes,
            desc=desc,
            selectors=selectors,
            dest="bucket",
            bucket=bucket,
            fn_prefix=f"{file_prefix}/cdl/{desc}",
        )
    elif dest == "local":
        out_dir = OUTPUT_DIR
        os.makedirs(out_dir, exist_ok=True)
        data = modes.getInfo()
        import pandas as pd

        rows = [f["properties"] for f in data["features"]]
        df = pd.DataFrame(rows)[selectors]
        out_path = os.path.join(out_dir, f"{desc}.csv")
        df.to_csv(out_path, index=False)
        print(f"  -> {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="CDL crop type extraction for Tongue River Basin"
    )
    parser.add_argument("--start-yr", type=int, default=CDL_FIRST_YEAR)
    parser.add_argument("--end-yr", type=int, default=CDL_LAST_YEAR)
    parser.add_argument("--dest", choices=["bucket", "local"], default="bucket")
    parser.add_argument("--bucket", type=str, default="wudr")
    parser.add_argument(
        "--project", type=str, default="ee-dgketchum", help="EE project ID"
    )
    args = parser.parse_args()

    sys.setrecursionlimit(5000)
    is_authorized(args.project)

    fc = shapefile_to_feature_collection(SHAPEFILE, FEATURE_ID)

    extract_cdl(
        fc,
        start_yr=args.start_yr,
        end_yr=args.end_yr,
        feature_id=FEATURE_ID,
        dest=args.dest,
        bucket=args.bucket,
    )
    print("Done — CDL export submitted.")


if __name__ == "__main__":
    main()
