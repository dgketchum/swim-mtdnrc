"""Merge and assemble Tongue extract CSVs into unified output.

Three operations:

1. merge_ndvi: Merge legacy NDVI (1987-2024, 2000 fields) with SID NDVI
   (1991-2023, 638 MT fields). SID-preferred for overlap years.

2. assemble_wy_etf: Merge WY ETf chunks (56033a/b from GCS staging) with
   existing SID ETf (638 MT fields) into unified CSVs with all fields.

3. assemble_ndvi_chunks: Merge NDVI 2025 chunks (tonguea/b from GCS staging)
   into single CSVs with all 2000 fields.

Usage:
    python -m swim_mtdnrc.calibration.merge_legacy --steps ndvi
    python -m swim_mtdnrc.calibration.merge_legacy --steps wy_etf,ndvi_2025
    python -m swim_mtdnrc.calibration.merge_legacy --steps all
    python -m swim_mtdnrc.calibration.merge_legacy --dry-run
"""

import argparse
from pathlib import Path

import pandas as pd

TONGUE_ROOT = Path("/nas/swim/examples/tongue")
TONGUE_NEW_ROOT = Path("/nas/swim/examples/tongue_new")
CROSSWALK_CSV = TONGUE_ROOT / "data/gis/tongue_sid_crosswalk.csv"

LEGACY_NDVI_DIR = TONGUE_ROOT / "data/landsat/extracts/ndvi"
SID_NDVI_DIR = TONGUE_NEW_ROOT / "data/landsat/extracts/ndvi"
OUTPUT_NDVI_DIR = SID_NDVI_DIR  # overwrites SID files in-place, adding missing fields

STAGING_DIR = Path("/tmp/swim_tongue_staging")

ETF_MODELS = ["disalexi", "eemetric", "ensemble", "geesebal", "ptjpl", "sims", "ssebop"]
WY_CHUNKS = ["56033a", "56033b"]
NDVI_CHUNKS = ["tonguea", "tongueb"]

MASKS = ["irr", "inv_irr"]
LEGACY_YEARS = range(1987, 2025)  # 1987-2024
SID_YEARS = range(1991, 2024)  # 1991-2023
ETF_YEARS = range(2016, 2026)  # 2016-2025


def load_accepted_fids(crosswalk_csv=None):
    """Load crosswalk and return set of accepted Tongue FIDs."""
    crosswalk_csv = Path(crosswalk_csv or CROSSWALK_CSV)
    df = pd.read_csv(crosswalk_csv)
    accepted = df[df["match_flag"] == "accepted"]
    fids = set(accepted["tongue_fid"].astype(int))
    print(f"Crosswalk: {len(fids)} accepted FIDs")
    return fids


def merge_ndvi(
    legacy_dir=None,
    sid_dir=None,
    output_dir=None,
    accepted_fids=None,
    crosswalk_csv=None,
    masks=None,
    dry_run=False,
):
    """Merge legacy and SID NDVI CSVs into unified output with all 2000 fields.

    For overlap years (1991-2023): accepted FIDs use SID rows, others use legacy.
    For non-overlap years (1987-1990, 2024): all rows from legacy.
    """
    legacy_dir = Path(legacy_dir or LEGACY_NDVI_DIR)
    sid_dir = Path(sid_dir or SID_NDVI_DIR)
    output_dir = Path(output_dir or OUTPUT_NDVI_DIR)
    masks = masks or list(MASKS)

    if accepted_fids is None:
        accepted_fids = load_accepted_fids(crosswalk_csv)

    n_written = 0
    for mask in masks:
        legacy_mask_dir = legacy_dir / mask
        sid_mask_dir = sid_dir / mask
        out_mask_dir = output_dir / mask
        out_mask_dir.mkdir(parents=True, exist_ok=True)

        for year in LEGACY_YEARS:
            legacy_file = legacy_mask_dir / f"ndvi_{year}_{mask}.csv"
            if not legacy_file.exists():
                print(f"  WARNING: missing legacy {legacy_file.name}")
                continue

            legacy_df = pd.read_csv(legacy_file)
            legacy_df["FID"] = legacy_df["FID"].astype(int)

            sid_file = sid_mask_dir / f"ndvi_{mask}_{year}.csv"
            has_sid = year in SID_YEARS and sid_file.exists()

            if has_sid:
                sid_df = pd.read_csv(sid_file)
                sid_df["FID"] = sid_df["FID"].astype(int)

                # SID rows for accepted FIDs
                sid_accepted = sid_df[sid_df["FID"].isin(accepted_fids)]
                # Legacy rows for all other FIDs
                legacy_other = legacy_df[~legacy_df["FID"].isin(accepted_fids)]

                merged = pd.concat(
                    [sid_accepted, legacy_other], axis=0, ignore_index=True
                )
            else:
                merged = legacy_df

            merged = merged.sort_values("FID").reset_index(drop=True)

            out_file = out_mask_dir / f"ndvi_{mask}_{year}.csv"
            if dry_run:
                print(f"  [dry-run] {out_file.name}: {len(merged)} rows")
            else:
                merged.to_csv(out_file, index=False)
                n_written += 1

            if year == LEGACY_YEARS[0] or year == LEGACY_YEARS[-1]:
                src = "SID+legacy" if has_sid else "legacy-only"
                print(f"  {mask}/{year}: {len(merged)} rows ({src})")

    if not dry_run:
        print(f"\nFiles written: {n_written}")
    print(f"Output: {output_dir}")


def assemble_wy_etf(
    staging_dir=None,
    output_root=None,
    masks=None,
    models=None,
    dry_run=False,
):
    """Merge WY ETf chunks with existing SID ETf into unified CSVs.

    For each model/mask/year: concat chunk CSVs from staging (WY fields),
    then concat with existing SID CSV (MT fields) if present.
    """
    staging_dir = Path(staging_dir or STAGING_DIR)
    output_root = Path(output_root or (TONGUE_NEW_ROOT / "data/landsat/extracts"))
    masks = masks or list(MASKS)
    models = models or list(ETF_MODELS)

    n_written = 0
    for model in models:
        for mask in masks:
            out_dir = output_root / f"{model}_etf" / mask
            out_dir.mkdir(parents=True, exist_ok=True)
            sid_dir = out_dir  # existing SID files live here

            for year in ETF_YEARS:
                filename = f"{model}_etf_{mask}_{year}.csv"

                # Gather WY chunks
                chunk_dfs = []
                for chunk in WY_CHUNKS:
                    csv_path = staging_dir / chunk / "etf" / mask / filename
                    if csv_path.exists():
                        df = pd.read_csv(csv_path)
                        df["FID"] = df["FID"].astype(int)
                        chunk_dfs.append(df)

                if not chunk_dfs:
                    print(f"  WARNING: no WY data for {filename}")
                    continue

                wy_df = pd.concat(chunk_dfs, axis=0, ignore_index=True)

                # Load existing SID file (MT fields) if present
                sid_file = sid_dir / filename
                if sid_file.exists():
                    sid_df = pd.read_csv(sid_file)
                    sid_df["FID"] = sid_df["FID"].astype(int)
                    # Remove any FIDs already in WY data (shouldn't happen, but safe)
                    wy_fids = set(wy_df["FID"])
                    sid_df = sid_df[~sid_df["FID"].isin(wy_fids)]
                    merged = pd.concat([sid_df, wy_df], axis=0, ignore_index=True)
                else:
                    merged = wy_df

                merged = merged.sort_values("FID").reset_index(drop=True)

                if dry_run:
                    print(
                        f"  [dry-run] {model}_etf/{mask}/{filename}: {len(merged)} rows"
                    )
                else:
                    merged.to_csv(out_dir / filename, index=False)
                    n_written += 1

                if year == ETF_YEARS[0]:
                    print(f"  {model}/{mask}: {len(merged)} fields ({year})")

    if not dry_run:
        print(f"\nWY ETf files written: {n_written}")


def assemble_ndvi_chunks(
    staging_dir=None,
    output_dir=None,
    masks=None,
    years=None,
    dry_run=False,
):
    """Merge NDVI chunks (tonguea/b from GCS) into single CSVs."""
    staging_dir = Path(staging_dir or STAGING_DIR)
    output_dir = Path(output_dir or OUTPUT_NDVI_DIR)
    masks = masks or list(MASKS)
    years = years or [2025]

    n_written = 0
    for mask in masks:
        out_mask_dir = output_dir / mask
        out_mask_dir.mkdir(parents=True, exist_ok=True)

        for year in years:
            filename = f"ndvi_{mask}_{year}.csv"
            chunk_dfs = []
            for chunk in NDVI_CHUNKS:
                csv_path = staging_dir / chunk / "ndvi" / mask / filename
                if csv_path.exists():
                    df = pd.read_csv(csv_path)
                    df["FID"] = df["FID"].astype(int)
                    chunk_dfs.append(df)

            if not chunk_dfs:
                print(f"  WARNING: no chunks for {filename}")
                continue

            merged = pd.concat(chunk_dfs, axis=0, ignore_index=True)
            merged = merged.sort_values("FID").reset_index(drop=True)

            out_file = out_mask_dir / filename
            if dry_run:
                print(f"  [dry-run] {out_file.name}: {len(merged)} rows")
            else:
                merged.to_csv(out_file, index=False)
                n_written += 1

            print(f"  ndvi/{mask}/{year}: {len(merged)} fields")

    if not dry_run:
        print(f"\nNDVI chunk files written: {n_written}")


def main():
    parser = argparse.ArgumentParser(
        description="Merge and assemble Tongue extract CSVs"
    )
    parser.add_argument(
        "--steps",
        type=str,
        default="all",
        help="Comma-separated steps: ndvi,wy_etf,ndvi_2025,all (default: all)",
    )
    parser.add_argument(
        "--crosswalk",
        type=str,
        default=str(CROSSWALK_CSV),
        help="Crosswalk CSV path (default: %(default)s)",
    )
    parser.add_argument(
        "--staging",
        type=str,
        default=str(STAGING_DIR),
        help="GCS staging directory (default: %(default)s)",
    )
    parser.add_argument(
        "--masks",
        type=str,
        default=",".join(MASKS),
        help="Comma-separated masks (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without writing",
    )
    args = parser.parse_args()

    steps = args.steps.split(",")
    if "all" in steps:
        steps = ["ndvi", "wy_etf", "ndvi_2025"]

    masks = args.masks.split(",")

    if "ndvi" in steps:
        print("\n=== Merging legacy NDVI ===")
        accepted_fids = load_accepted_fids(args.crosswalk)
        merge_ndvi(accepted_fids=accepted_fids, masks=masks, dry_run=args.dry_run)

    if "wy_etf" in steps:
        print("\n=== Assembling WY ETf ===")
        assemble_wy_etf(staging_dir=args.staging, masks=masks, dry_run=args.dry_run)

    if "ndvi_2025" in steps:
        print("\n=== Assembling NDVI 2025 ===")
        assemble_ndvi_chunks(
            staging_dir=args.staging, masks=masks, dry_run=args.dry_run
        )


if __name__ == "__main__":
    main()
