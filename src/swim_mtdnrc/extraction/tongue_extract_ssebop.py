"""SSEBop ETf extraction for Tongue River Basin (2,084 fields).

Adapts sid_etf.py for the tongue shapefile using SSEBop ETf (et_fraction / 10000).

Usage:
    python -m swim_mtdnrc.extraction.tongue_extract_ssebop \
        --years 2016,2017,2018 --mask-types irr,inv_irr --dest bucket
"""

import argparse
import os
import sys
import time

import ee

from swimrs.data_extraction.ee.common import (
    export_table,
    shapefile_to_feature_collection,
)
from swimrs.data_extraction.ee.ee_utils import is_authorized

WAIT_MINUTES = 10
MAX_RETRIES = 6

IRR = "projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp"
FEATURE_ID = "FID"
SHAPEFILE = "/nas/swim/examples/tongue_new/data/gis/tongue_fields_gfid.shp"

SSEBOP_COLLECTION = "projects/openet/assets/ssebop/conus/gridmet/landsat/v2_1"
IRR_MAX_YEAR = 2023

OUTPUT_ROOT = "/nas/swim/examples/tongue_new/data/landsat/extracts/ssebop_etf"


def extract_ssebop_etf(
    feature_coll,
    irr_coll,
    irr_min_yr_mask,
    mask_type="irr",
    start_yr=2016,
    end_yr=2024,
    years=None,
    feature_id="FID",
    dest="bucket",
    bucket="wudr",
    file_prefix="tongue",
):
    """Extract mean SSEBop ETf per field.

    SSEBop band: et_fraction / 10000, clamped to [0, 2].

    Parameters
    ----------
    years : list[int] or None
        Explicit list of years. Overrides start_yr/end_yr.
    """
    if years is None:
        years = list(range(start_yr, end_yr + 1))

    for year in years:
        irr_year = min(year, IRR_MAX_YEAR)
        irr = (
            irr_coll.filterDate(f"{irr_year}-01-01", f"{irr_year}-12-31")
            .select("classification")
            .mosaic()
        )
        irr_mask = irr_min_yr_mask.updateMask(irr.lt(1))

        coll = (
            ee.ImageCollection(SSEBOP_COLLECTION)
            .filterDate(f"{year}-01-01", f"{year}-12-31")
            .filterBounds(feature_coll.geometry())
        )

        # Normalize: et_fraction / 10000, clamp [0, 2]
        def normalize(img):
            etf = img.select("et_fraction").divide(10000).clamp(0, 2).rename("etf")
            return ee.Image(
                etf.copyProperties(img, ["system:time_start", "system:index"])
            )

        if mask_type == "irr":
            coll = coll.map(lambda x, _m=irr_mask: normalize(x).updateMask(_m))
        elif mask_type == "inv_irr":
            coll = coll.map(lambda x, _i=irr: normalize(x).updateMask(_i.gt(0)))
        else:
            coll = coll.map(normalize)

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
        print(f"  {year}: {len(band_names)} scenes (ssebop)")
        bands = coll.toBands().rename(band_names)

        data = bands.reduceRegions(
            collection=feature_coll,
            reducer=ee.Reducer.mean(),
            scale=30,
            tileScale=8,
        )

        desc = f"ssebop_etf_{mask_type}_{year}"
        selectors = [feature_id] + band_names

        if dest == "bucket":
            for attempt in range(MAX_RETRIES):
                try:
                    export_table(
                        data,
                        desc=desc,
                        selectors=selectors,
                        dest="bucket",
                        bucket=bucket,
                        fn_prefix=f"{file_prefix}/ssebop_etf/{mask_type}/{desc}",
                    )
                    break
                except ee.ee_exception.EEException as exc:
                    if attempt == MAX_RETRIES - 1:
                        raise
                    print(f"  export failed ({exc}), retrying in {WAIT_MINUTES} min...")
                    time.sleep(WAIT_MINUTES * 60)
        elif dest == "local":
            out_dir = os.path.join(OUTPUT_ROOT, mask_type)
            os.makedirs(out_dir, exist_ok=True)
            data_df = ee.data.computeFeatures(
                {"expression": data, "fileFormat": "PANDAS_DATAFRAME"}
            )
            data_df.index = data_df[feature_id]
            data_df.drop(columns=["geo"], inplace=True, errors="ignore")
            out_path = os.path.join(out_dir, f"{desc}.csv")
            data_df.to_csv(out_path)
            print(f"  -> {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="SSEBop ETf extraction for Tongue River Basin"
    )
    parser.add_argument("--start-yr", type=int, default=2016)
    parser.add_argument("--end-yr", type=int, default=2024)
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
    parser.add_argument("--dest", choices=["bucket", "local"], default="bucket")
    parser.add_argument("--bucket", type=str, default="wudr")
    parser.add_argument(
        "--project", type=str, default="ee-dgketchum", help="EE project ID"
    )
    args = parser.parse_args()

    year_list = [int(y) for y in args.years.split(",")] if args.years else None
    mask_types = [m.strip() for m in args.mask_types.split(",")]

    sys.setrecursionlimit(5000)
    is_authorized(args.project)

    irr_coll = ee.ImageCollection(IRR)
    remap = irr_coll.filterDate("1987-01-01", "2024-12-31").select("classification")
    irr_min_yr_mask = remap.map(lambda img: img.lt(1)).sum().gte(5)
    print("Computed irr_min_yr_mask (live)")

    fc = shapefile_to_feature_collection(SHAPEFILE, FEATURE_ID)

    for mask_type in mask_types:
        print(f"\n=== SSEBop ETf mask={mask_type} ===")
        extract_ssebop_etf(
            fc,
            irr_coll,
            irr_min_yr_mask,
            mask_type=mask_type,
            start_yr=args.start_yr,
            end_yr=args.end_yr,
            years=year_list,
            feature_id=FEATURE_ID,
            dest=args.dest,
            bucket=args.bucket,
        )


if __name__ == "__main__":
    main()
