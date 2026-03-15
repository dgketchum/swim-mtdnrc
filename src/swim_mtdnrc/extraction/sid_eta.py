# Monthly ensemble ETa extraction for Montana SID fields 1984-2025.
# Outputs per-county CSVs to GCS (same chunking/CLI pattern as sid_etf.py).

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
from swimrs.data_extraction.ee.ee_utils import is_authorized

WAIT_MINUTES = 10
MAX_RETRIES = 6

IRR = "projects/ee-dgketchum/assets/IrrMapper/IrrMapperComp"
IRR_MAX_YEAR = 2025
FEATURE_ID = "FID"
SHAPEFILE = "/nas/Montana/statewide_irrigation_dataset/statewide_irrigation_dataset_15FEB2024_aea.shp"

# OpenET monthly ensemble ETa collections
OPENET_ETa_V2 = "projects/openet/assets/ensemble/conus/gridmet/monthly/v2_0"
OPENET_ETa_PRE2000 = (
    "projects/openet/assets/ensemble/conus/gridmet/monthly/v2_0_pre2000"
)
ETa_SPLIT_YEAR = 1999  # v2_0 starts 1999-10; route 1999 to v2_0 for full year coverage
ETa_BAND = "et_ensemble_mad"  # band name in monthly ensemble (mm/month)


def _blob_exists(bucket_name: str, blob_name: str, project: str) -> bool:
    from google.cloud import storage

    client = storage.Client(project=project)
    return client.bucket(bucket_name).blob(blob_name).exists()


def extract_eta(
    feature_coll,
    irr_coll,
    irr_min_yr_mask,
    mask_type="irr",
    start_yr=1984,
    end_yr=2024,
    years=None,
    feature_id="FID",
    dest="bucket",
    bucket="wudr",
    file_prefix="sid",
    project="ee-hoylman",
    skip_exists_check=False,
):
    """Extract mean monthly ETa per field from OpenET monthly ensemble collections.

    When dest="bucket", starts one ee.batch export task per year to GCS.
    When dest="local", uses ee.data.computeFeatures for synchronous return.

    Parameters
    ----------
    years : list[int] or None
        Explicit list of years to process. Overrides start_yr/end_yr.
    skip_exists_check : bool
        If True, skip the GCS blob-exists check (use when project lacks bucket read access).
    """
    if years is None:
        years = list(range(start_yr, end_yr + 1))

    dfs = []

    for year in years:
        src_path = OPENET_ETa_V2 if year >= ETa_SPLIT_YEAR else OPENET_ETa_PRE2000

        fn_prefix = (
            f"{file_prefix}/eta/monthly/{mask_type}/ensemble_eta_{mask_type}_{year}"
        )

        if (
            not skip_exists_check
            and dest == "bucket"
            and _blob_exists(bucket, fn_prefix + ".csv", project)
        ):
            print(f"  {year}: skip (exists) gs://{bucket}/{fn_prefix}.csv")
            continue

        irr_year = min(year, IRR_MAX_YEAR)
        irr = (
            irr_coll.filterDate(f"{irr_year}-01-01", f"{irr_year}-12-31")
            .select("classification")
            .mosaic()
        )
        irr_mask = irr_min_yr_mask.updateMask(irr.lt(1))

        coll = (
            ee.ImageCollection(src_path)
            .filterDate(f"{year}-01-01", f"{year + 1}-01-01")
            .filterBounds(feature_coll.geometry())
            .select(ETa_BAND)
        )

        if mask_type == "irr":
            coll = coll.map(lambda x, _m=irr_mask: x.updateMask(_m))
        elif mask_type == "inv_irr":
            coll = coll.map(lambda x, _i=irr: x.updateMask(_i.gt(0)))

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
        print(f"  {year}: {len(band_names)} months ({src_path.split('/')[-1]})")
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
            desc = f"ensemble_eta_{mask_type}_{year}"
            selectors = [feature_id] + band_names
            for attempt in range(MAX_RETRIES):
                try:
                    export_table(
                        data,
                        desc=desc,
                        selectors=selectors,
                        dest="bucket",
                        bucket=bucket,
                        fn_prefix=fn_prefix,
                    )
                    break
                except ee.ee_exception.EEException as exc:
                    if attempt == MAX_RETRIES - 1:
                        raise
                    print(f"  export failed ({exc}), retrying in {WAIT_MINUTES} min...")
                    time.sleep(WAIT_MINUTES * 60)

    if dest == "local":
        return pd.concat(dfs, axis=1) if dfs else None
    return None


def _chunk_list(lst, n):
    """Split list into n roughly equal chunks."""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


CHUNK_SUFFIXES = "abcdefghijklmnopqrstuvwxyz"


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="SID monthly ETa extraction (OpenET ensemble)"
    )
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
    parser.add_argument(
        "--mask-types",
        type=str,
        default="irr,inv_irr",
        help="Comma-separated mask types",
    )
    parser.add_argument("--start-yr", type=int, default=1984)
    parser.add_argument("--end-yr", type=int, default=2024)
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma-separated years (overrides start/end)",
    )
    parser.add_argument("--dest", choices=["bucket", "local"], default="bucket")
    parser.add_argument("--bucket", type=str, default="wudr")
    parser.add_argument(
        "--project", type=str, default="ee-hoylman", help="EE project ID"
    )
    parser.add_argument(
        "--skip-exists-check",
        action="store_true",
        default=False,
        help="Skip GCS blob-exists check (use when project lacks bucket read access)",
    )
    args = parser.parse_args()

    year_list = [int(y) for y in args.years.split(",")] if args.years else None
    mask_types = [m.strip() for m in args.mask_types.split(",")]

    root = "/data/ssd2/swim/sid"
    os.makedirs(root, exist_ok=True)
    sys.setrecursionlimit(5000)

    is_authorized(args.project)

    irr_coll = ee.ImageCollection(IRR)
    remap = irr_coll.filterDate("1987-01-01", "2026-01-01").select("classification")
    irr_min_yr_mask = remap.map(lambda img: img.lt(1)).sum().gte(5)
    print("Computed irr_min_yr_mask (live)")

    gdf = gpd.read_file(SHAPEFILE, engine="fiona")
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
                    f"\n=== {label} ({name}, {len(chunk_fids)} fields) "
                    f"mask={mask_type} ==="
                )

                fc = shapefile_to_feature_collection(
                    SHAPEFILE, FEATURE_ID, select=chunk_fids
                )

                start_time = time.time()
                result = extract_eta(
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
                    file_prefix=f"sid/{label}",
                    project=args.project,
                    skip_exists_check=args.skip_exists_check,
                )
                elapsed = time.time() - start_time

                if result is not None:
                    out_csv = os.path.join(root, f"{label}_eta_{mask_type}.csv")
                    result.to_csv(out_csv)
                    print(
                        f"  {result.shape[0]} fields x {result.shape[1]} months "
                        f"in {elapsed:.1f}s -> {out_csv}"
                    )
                else:
                    print(f"  Export tasks submitted in {elapsed:.1f}s")

# ========================= EOF ====================================================================
