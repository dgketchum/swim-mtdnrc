"""NDVI extraction for Tongue River Basin (2000 fields).

Reuses sid_ndvi.extract_ndvi() with the Tongue shapefile, chunking 2000
fields into batches of --max-fields (default 1000) to stay within EE limits.

Usage:
    python -m swim_mtdnrc.extraction.tongue_extract_ndvi \
        --years 2025 --mask-types irr,inv_irr --dest bucket

    # Run a single chunk
    python -m swim_mtdnrc.extraction.tongue_extract_ndvi \
        --years 2025 --max-fields 1000 --chunk-index 0
"""

import argparse
import sys
import time

import geopandas as gpd

from swimrs.data_extraction.ee.common import shapefile_to_feature_collection
from swimrs.data_extraction.ee.ee_utils import is_authorized

from swim_mtdnrc.extraction.sid_ndvi import extract_ndvi

FEATURE_ID = "FID"
SHAPEFILE = "/nas/swim/examples/tongue/data/gis/tongue_fields_gfid.shp"
CHUNK_SUFFIXES = "abcdefghijklmnopqrstuvwxyz"


def _chunk_list(lst, n):
    """Split list into n roughly equal chunks."""
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


def main():
    parser = argparse.ArgumentParser(
        description="NDVI extraction for Tongue River Basin"
    )
    parser.add_argument("--start-yr", type=int, default=2025)
    parser.add_argument("--end-yr", type=int, default=2025)
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
        "--max-fields",
        type=int,
        default=1000,
        help="Max fields per chunk (default: 1000)",
    )
    parser.add_argument(
        "--chunk-index",
        type=int,
        default=None,
        help="Run only this chunk (0-indexed)",
    )
    parser.add_argument("--dest", choices=["bucket", "local"], default="bucket")
    parser.add_argument("--bucket", type=str, default="wudr")
    parser.add_argument("--project", type=str, default="ee-dgketchum")
    args = parser.parse_args()

    year_list = [int(y) for y in args.years.split(",")] if args.years else None
    mask_types = [m.strip() for m in args.mask_types.split(",")]

    sys.setrecursionlimit(5000)
    is_authorized(args.project)

    gdf = gpd.read_file(SHAPEFILE, engine="fiona")
    all_fids = sorted(gdf[FEATURE_ID].tolist())
    print(f"Loaded {len(all_fids)} fields from {SHAPEFILE}")

    n_chunks = -(-len(all_fids) // args.max_fields)  # ceil division
    if n_chunks > 1:
        chunks = _chunk_list(all_fids, n_chunks)
        print(f"Splitting into {n_chunks} chunks of ~{args.max_fields} fields")
    else:
        chunks = [all_fids]

    for ci, chunk_fids in enumerate(chunks):
        if args.chunk_index is not None and ci != args.chunk_index:
            continue

        suffix = CHUNK_SUFFIXES[ci] if len(chunks) > 1 else ""
        label = f"tongue{suffix}"

        for mask_type in mask_types:
            print(f"\n=== {label} ({len(chunk_fids)} fields) mask={mask_type} ===")

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
                feature_id=FEATURE_ID,
                dest=args.dest,
                bucket=args.bucket,
                file_prefix=f"tongue/{label}",
            )
            elapsed = time.time() - start_time

            if result is not None:
                print(
                    f"  {result.shape[0]} fields x {result.shape[1]} scenes "
                    f"in {elapsed:.1f}s"
                )
            else:
                print(f"  Export tasks submitted in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
