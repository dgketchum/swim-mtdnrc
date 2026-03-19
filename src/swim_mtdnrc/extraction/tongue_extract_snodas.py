"""SNODAS SWE extraction for Tongue River Basin (2000 fields).

Extracts monthly SNODAS SWE via Earth Engine in chunks to avoid EE memory
limits, downloads from GCS, merges chunks, and renames to ingestor format.

Usage:
    # Step 1: Submit EE export tasks (requires confirmation)
    python -m swim_mtdnrc.extraction.tongue_extract_snodas --extract \
        --start-yr 2024 --end-yr 2025

    # Step 2: Download from GCS, merge chunks, and rename
    python -m swim_mtdnrc.extraction.tongue_extract_snodas --download \
        --start-yr 2024 --end-yr 2025
"""

import argparse
import subprocess
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

from swimrs.data_extraction.ee.ee_utils import is_authorized

FEATURE_ID = "FID"
SHAPEFILE = "/nas/swim/examples/tongue/data/gis/tongue_fields_gfid.shp"
SNODAS_DIR = Path("/nas/swim/examples/tongue/data/snow/snodas/extracts")
BUCKET = "wudr"
FILE_PREFIX = "tongue"
PROJECT = "ee-dgketchum"
CHUNK_SIZE = 500
CHUNK_LABELS = "abcdefghijklmnopqrstuvwxyz"


def _get_chunks():
    """Read shapefile and split FIDs into chunks of CHUNK_SIZE."""
    gdf = gpd.read_file(SHAPEFILE, engine="fiona")
    all_fids = sorted(gdf[FEATURE_ID].tolist())
    chunks = [all_fids[i : i + CHUNK_SIZE] for i in range(0, len(all_fids), CHUNK_SIZE)]
    return chunks


def extract_snodas(
    start_yr=2024, end_yr=2025, bucket=BUCKET, project=PROJECT, chunk_index=None
):
    """Submit EE export tasks for SNODAS SWE, chunked by field count."""
    from swimrs.data_extraction.ee.snodas_export import sample_snodas_swe

    sys.setrecursionlimit(5000)
    is_authorized(project)

    chunks = _get_chunks()
    print(
        f"Extracting SNODAS SWE {start_yr}-{end_yr}: {len(chunks)} chunks of ~{CHUNK_SIZE}"
    )

    for ci, chunk_fids in enumerate(chunks):
        if chunk_index is not None and ci != chunk_index:
            continue

        label = CHUNK_LABELS[ci]
        prefix = f"{FILE_PREFIX}{label}"
        print(
            f"\n=== Chunk {label} ({len(chunk_fids)} fields, FID {chunk_fids[0]}-{chunk_fids[-1]}) ==="
        )

        sample_snodas_swe(
            feature_coll=SHAPEFILE,
            start_yr=start_yr,
            end_yr=end_yr,
            feature_id=FEATURE_ID,
            select=chunk_fids,
            bucket=bucket,
            dest="bucket",
            file_prefix=prefix,
        )


def download_snodas(start_yr=2024, end_yr=2025, bucket=BUCKET, output_dir=None):
    """Download SNODAS chunk exports from GCS, merge, and rename."""
    output_dir = Path(output_dir or SNODAS_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    chunks = _get_chunks()
    staging = Path("/tmp/snodas_staging")

    # Download each chunk's exports
    for ci in range(len(chunks)):
        label = CHUNK_LABELS[ci]
        prefix = f"{FILE_PREFIX}{label}"
        chunk_dir = staging / label
        chunk_dir.mkdir(parents=True, exist_ok=True)

        gcs_path = f"gs://{bucket}/{prefix}/snow/snodas/extracts/*.csv"
        print(f"Downloading chunk {label} from {gcs_path}...")
        subprocess.run(
            ["gsutil", "-m", "cp", gcs_path, str(chunk_dir)],
            check=True,
        )

    # Merge chunks per month and rename
    merged = 0
    for yr in range(start_yr, end_yr + 1):
        for mo in range(1, 13):
            fname = f"swe_{yr}_{mo:02d}.csv"
            dst = output_dir / f"snodas_{yr}{mo:02d}.csv"

            if dst.exists():
                print(f"  skip (exists): {dst.name}")
                continue

            # Collect chunk CSVs for this month
            chunk_dfs = []
            for ci in range(len(chunks)):
                label = CHUNK_LABELS[ci]
                src = staging / label / fname
                if src.exists():
                    chunk_dfs.append(pd.read_csv(src, index_col=FEATURE_ID))

            if not chunk_dfs:
                continue

            df = pd.concat(chunk_dfs, axis=0)
            df = df.sort_index()
            df.index.name = FEATURE_ID
            df.to_csv(dst)
            merged += 1
            print(f"  merged {len(chunk_dfs)} chunks -> {dst.name} ({len(df)} rows)")

    print(f"Merged {merged} monthly SNODAS files -> {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="SNODAS SWE extraction for Tongue River Basin"
    )
    parser.add_argument("--extract", action="store_true", help="Submit EE export tasks")
    parser.add_argument(
        "--download", action="store_true", help="Download from GCS, merge, and rename"
    )
    parser.add_argument("--start-yr", type=int, default=2024)
    parser.add_argument("--end-yr", type=int, default=2025)
    parser.add_argument("--bucket", type=str, default=BUCKET)
    parser.add_argument("--project", type=str, default=PROJECT)
    parser.add_argument("--output-dir", type=str, default=None)
    parser.add_argument(
        "--chunk-index", type=int, default=None, help="Run only this chunk (0-indexed)"
    )
    args = parser.parse_args()

    if not args.extract and not args.download:
        parser.print_help()
        return

    if args.extract:
        extract_snodas(
            start_yr=args.start_yr,
            end_yr=args.end_yr,
            bucket=args.bucket,
            project=args.project,
            chunk_index=args.chunk_index,
        )

    if args.download:
        download_snodas(
            start_yr=args.start_yr,
            end_yr=args.end_yr,
            bucket=args.bucket,
            output_dir=args.output_dir,
        )


if __name__ == "__main__":
    main()
