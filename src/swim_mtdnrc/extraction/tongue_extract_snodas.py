"""SNODAS SWE extraction for Tongue River Basin (2000 fields).

Extracts monthly SNODAS SWE via Earth Engine, downloads from GCS,
and renames to the ingestor format (snodas_YYYYMM.csv).

Usage:
    # Step 1: Submit EE export tasks (requires confirmation)
    python -m swim_mtdnrc.extraction.tongue_extract_snodas --extract \
        --start-yr 2024 --end-yr 2025

    # Step 2: Download from GCS and rename
    python -m swim_mtdnrc.extraction.tongue_extract_snodas --download \
        --start-yr 2024 --end-yr 2025
"""

import argparse
import subprocess
import sys
from pathlib import Path

from swimrs.data_extraction.ee.ee_utils import is_authorized

FEATURE_ID = "FID"
SHAPEFILE = "/nas/swim/examples/tongue/data/gis/tongue_fields_gfid.shp"
SNODAS_DIR = Path("/nas/swim/examples/tongue/data/snow/snodas/extracts")
BUCKET = "wudr"
FILE_PREFIX = "tongue"
PROJECT = "ee-dgketchum"


def extract_snodas(start_yr=2024, end_yr=2025, bucket=BUCKET, project=PROJECT):
    """Submit EE export tasks for SNODAS SWE."""
    from swimrs.data_extraction.ee.snodas_export import sample_snodas_swe

    sys.setrecursionlimit(5000)
    is_authorized(project)

    print(f"Extracting SNODAS SWE {start_yr}-{end_yr} for Tongue fields")
    sample_snodas_swe(
        feature_coll=SHAPEFILE,
        start_yr=start_yr,
        end_yr=end_yr,
        feature_id=FEATURE_ID,
        bucket=bucket,
        dest="bucket",
        file_prefix=FILE_PREFIX,
        check_dir=str(SNODAS_DIR),
    )


def download_snodas(start_yr=2024, end_yr=2025, bucket=BUCKET, output_dir=None):
    """Download SNODAS exports from GCS and rename to ingestor format."""
    output_dir = Path(output_dir or SNODAS_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    gcs_prefix = f"gs://{bucket}/{FILE_PREFIX}/snow/snodas/extracts"
    staging = Path("/tmp/snodas_staging")
    staging.mkdir(parents=True, exist_ok=True)

    # Download all swe_*.csv from GCS
    print(f"Downloading from {gcs_prefix}...")
    subprocess.run(
        ["gsutil", "-m", "cp", f"{gcs_prefix}/swe_*.csv", str(staging)],
        check=True,
    )

    # Rename swe_YYYY_MM.csv -> snodas_YYYYMM.csv and copy to output
    renamed = 0
    for yr in range(start_yr, end_yr + 1):
        for mo in range(1, 13):
            src = staging / f"swe_{yr}_{mo:02d}.csv"
            dst = output_dir / f"snodas_{yr}{mo:02d}.csv"

            if not src.exists():
                continue
            if dst.exists():
                print(f"  skip (exists): {dst.name}")
                continue

            src.rename(dst)
            renamed += 1
            print(f"  {src.name} -> {dst.name}")

    print(f"Renamed {renamed} SNODAS files -> {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="SNODAS SWE extraction for Tongue River Basin"
    )
    parser.add_argument("--extract", action="store_true", help="Submit EE export tasks")
    parser.add_argument(
        "--download", action="store_true", help="Download from GCS and rename"
    )
    parser.add_argument("--start-yr", type=int, default=2024)
    parser.add_argument("--end-yr", type=int, default=2025)
    parser.add_argument("--bucket", type=str, default=BUCKET)
    parser.add_argument("--project", type=str, default=PROJECT)
    parser.add_argument("--output-dir", type=str, default=None)
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
