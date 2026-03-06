"""Assemble SID ETf/NDVI from GCS for Tongue fields.

Downloads OpenET ETf (7 models, 2016-2025) and NDVI (1991-2023) CSVs from
gs://wudr/sid/<county>/, remaps SID FIDs to Tongue integer FIDs using the
crosswalk, merges across counties, and writes output in the per-model
directory layout expected by the SwimContainer ingestor.

Usage:
    python -m swim_mtdnrc.calibration.assemble_sid
    python -m swim_mtdnrc.calibration.assemble_sid --steps etf
    python -m swim_mtdnrc.calibration.assemble_sid --steps ndvi
    python -m swim_mtdnrc.calibration.assemble_sid --dry-run
    python -m swim_mtdnrc.calibration.assemble_sid --no-download
    python -m swim_mtdnrc.calibration.assemble_sid --models ssebop,ensemble
"""

import argparse
import subprocess
from pathlib import Path

import pandas as pd

TONGUE_ROOT = Path("/nas/swim/examples/tongue")
TONGUE_NEW_ROOT = Path("/nas/swim/examples/tongue_new")
CROSSWALK_CSV = TONGUE_ROOT / "data/gis/tongue_sid_crosswalk.csv"
OUTPUT_ROOT = TONGUE_NEW_ROOT / "data/landsat/extracts"
STAGING_DIR = Path("/tmp/swim_sid_staging")
GCS_BUCKET = "wudr"
GCS_PREFIX = "sid"

ETF_MODELS = ["disalexi", "eemetric", "ensemble", "geesebal", "ptjpl", "sims", "ssebop"]
MASKS = ["irr", "inv_irr"]
ETF_YEARS = range(2016, 2026)
NDVI_YEARS = range(1991, 2024)


def load_crosswalk(crosswalk_csv=None):
    """Load crosswalk, filter to accepted matches.

    Returns:
        sid_to_tongue: dict mapping SID FID string -> Tongue integer FID
        counties: sorted list of zero-padded county strings
    """
    crosswalk_csv = Path(crosswalk_csv or CROSSWALK_CSV)
    df = pd.read_csv(crosswalk_csv)
    accepted = df[df["match_flag"] == "accepted"].copy()

    sid_to_tongue = dict(
        zip(accepted["sid_fid"].astype(str), accepted["tongue_fid"].astype(int))
    )

    # county_no is float in CSV (e.g. 17.0) — convert to zero-padded string
    counties = sorted(accepted["county_no"].dropna().unique())
    counties = [f"{int(c):03d}" for c in counties]

    print(f"Crosswalk: {len(sid_to_tongue)} accepted fields across counties {counties}")
    return sid_to_tongue, counties


def download_from_gcs(counties, staging_dir, data_type, masks, dry_run=False):
    """Download CSV files from GCS to local staging.

    Args:
        counties: list of zero-padded county strings
        staging_dir: local staging directory
        data_type: "etf" or "ndvi"
        masks: list of mask names
        dry_run: if True, print commands without executing
    """
    staging_dir = Path(staging_dir)
    for county in counties:
        for mask in masks:
            src = f"gs://{GCS_BUCKET}/{GCS_PREFIX}/{county}/{data_type}/{mask}/*.csv"
            dst = staging_dir / county / data_type / mask
            dst.mkdir(parents=True, exist_ok=True)

            cmd = ["gsutil", "-m", "cp", src, str(dst)]
            if dry_run:
                print(f"  [dry-run] {' '.join(cmd)}")
                continue

            print(f"  Downloading {src} -> {dst}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"  WARNING: gsutil failed for {src}: {result.stderr.strip()}")
            else:
                n_files = len(list(dst.glob("*.csv")))
                print(f"    {n_files} files downloaded")


def remap_and_merge(staging_dir, counties, sid_to_tongue, filename):
    """Remap SID FIDs to Tongue FIDs and merge across counties.

    Args:
        staging_dir: local staging directory
        counties: list of zero-padded county strings
        sid_to_tongue: dict SID FID -> Tongue integer FID
        filename: CSV filename to load from each county (e.g. "ssebop_etf_irr_2020.csv")

    Returns:
        merged DataFrame with integer Tongue FIDs, or None if no data found
    """
    staging_dir = Path(staging_dir)
    # Infer data_type and mask from filename
    # ETf: ssebop_etf_irr_2020.csv or ssebop_etf_inv_irr_2020.csv
    # NDVI: ndvi_irr_2020.csv or ndvi_inv_irr_2020.csv
    if "inv_irr" in filename:
        mask = "inv_irr"
    else:
        mask = "irr"

    if filename.startswith("ndvi_"):
        data_type = "ndvi"
    else:
        data_type = "etf"

    dfs = []
    for county in counties:
        csv_path = staging_dir / county / data_type / mask / filename
        if not csv_path.exists():
            continue

        df = pd.read_csv(csv_path, dtype={"FID": str})
        # Filter to SID FIDs in crosswalk
        df = df[df["FID"].isin(sid_to_tongue)].copy()
        if len(df) == 0:
            continue

        # Map SID -> Tongue integer FID
        df["FID"] = df["FID"].map(sid_to_tongue)
        dfs.append(df)

    if not dfs:
        return None

    merged = pd.concat(dfs, axis=0, ignore_index=True)
    merged = merged.sort_values("FID").reset_index(drop=True)
    return merged


def assemble(
    crosswalk_csv=None,
    output_root=None,
    staging_dir=None,
    steps=None,
    models=None,
    masks=None,
    dry_run=False,
    download=True,
):
    """Orchestrate download, remap, and output of SID data for Tongue.

    Args:
        crosswalk_csv: path to crosswalk CSV
        output_root: root output directory for extracts
        staging_dir: local staging directory for GCS downloads
        steps: list of steps to run ("etf", "ndvi")
        models: list of ETf models to process
        masks: list of masks to process
        dry_run: print what would be done without writing
        download: if False, skip GCS download (use existing staging)
    """
    output_root = Path(output_root or OUTPUT_ROOT)
    staging_dir = Path(staging_dir or STAGING_DIR)
    steps = steps or ["etf", "ndvi"]
    models = models or ETF_MODELS
    masks = masks or list(MASKS)

    sid_to_tongue, counties = load_crosswalk(crosswalk_csv)

    # Download
    if download:
        if "etf" in steps:
            print("\n=== Downloading ETf from GCS ===")
            download_from_gcs(counties, staging_dir, "etf", masks, dry_run=dry_run)
        if "ndvi" in steps:
            print("\n=== Downloading NDVI from GCS ===")
            download_from_gcs(counties, staging_dir, "ndvi", masks, dry_run=dry_run)
    else:
        print("\nSkipping GCS download (--no-download)")

    if dry_run:
        _print_dry_run_summary(output_root, steps, models, masks)
        return

    n_written = 0
    n_fields_seen = set()

    # ETf: model x mask x year
    if "etf" in steps:
        print("\n=== Assembling ETf ===")
        for model in models:
            for mask in masks:
                out_dir = output_root / f"{model}_etf" / mask
                out_dir.mkdir(parents=True, exist_ok=True)
                for year in ETF_YEARS:
                    filename = f"{model}_etf_{mask}_{year}.csv"
                    df = remap_and_merge(staging_dir, counties, sid_to_tongue, filename)
                    if df is None:
                        print(f"  WARNING: no data for {filename}")
                        continue
                    out_path = out_dir / filename
                    df.to_csv(out_path, index=False)
                    n_written += 1
                    n_fields_seen.update(df["FID"].tolist())
                    if year == ETF_YEARS[0]:
                        print(
                            f"  {model}/{mask}: {len(df)} fields, {len(df.columns) - 1} scenes ({year})"
                        )

    # NDVI: mask x year
    if "ndvi" in steps:
        print("\n=== Assembling NDVI ===")
        for mask in masks:
            out_dir = output_root / "ndvi" / mask
            out_dir.mkdir(parents=True, exist_ok=True)
            for year in NDVI_YEARS:
                filename = f"ndvi_{mask}_{year}.csv"
                df = remap_and_merge(staging_dir, counties, sid_to_tongue, filename)
                if df is None:
                    print(f"  WARNING: no data for {filename}")
                    continue
                out_path = out_dir / filename
                df.to_csv(out_path, index=False)
                n_written += 1
                n_fields_seen.update(df["FID"].tolist())
                if year == NDVI_YEARS[0]:
                    print(
                        f"  ndvi/{mask}: {len(df)} fields, {len(df.columns) - 1} scenes ({year})"
                    )

    print("\n=== Summary ===")
    print(f"Files written: {n_written}")
    print(f"Unique Tongue FIDs seen: {len(n_fields_seen)}")
    print(f"Output root: {output_root}")


def _print_dry_run_summary(output_root, steps, models, masks):
    """Print summary of what would be written."""
    print("\n=== Dry Run Summary ===")
    if "etf" in steps:
        n_etf = len(models) * len(masks) * len(ETF_YEARS)
        print(
            f"ETf: {len(models)} models x {len(masks)} masks x {len(ETF_YEARS)} years = {n_etf} files"
        )
        for model in models:
            for mask in masks:
                out_dir = output_root / f"{model}_etf" / mask
                print(f"  {out_dir}/")
    if "ndvi" in steps:
        n_ndvi = len(masks) * len(NDVI_YEARS)
        print(f"NDVI: {len(masks)} masks x {len(NDVI_YEARS)} years = {n_ndvi} files")
        for mask in masks:
            out_dir = output_root / "ndvi" / mask
            print(f"  {out_dir}/")


def main():
    parser = argparse.ArgumentParser(
        description="Assemble SID ETf/NDVI from GCS for Tongue fields"
    )
    parser.add_argument(
        "--crosswalk",
        type=str,
        default=str(CROSSWALK_CSV),
        help="Crosswalk CSV path (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(OUTPUT_ROOT),
        help="Output root directory (default: %(default)s)",
    )
    parser.add_argument(
        "--staging",
        type=str,
        default=str(STAGING_DIR),
        help="Local staging directory (default: %(default)s)",
    )
    parser.add_argument(
        "--steps",
        type=str,
        default="etf,ndvi",
        help="Comma-separated steps: etf,ndvi (default: %(default)s)",
    )
    parser.add_argument(
        "--models",
        type=str,
        default=",".join(ETF_MODELS),
        help="Comma-separated ETf models (default: %(default)s)",
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
    parser.add_argument(
        "--no-download",
        action="store_true",
        help="Skip GCS download, use existing staging",
    )
    args = parser.parse_args()

    assemble(
        crosswalk_csv=args.crosswalk,
        output_root=args.output,
        staging_dir=args.staging,
        steps=args.steps.split(","),
        models=args.models.split(","),
        masks=args.masks.split(","),
        dry_run=args.dry_run,
        download=not args.no_download,
    )


if __name__ == "__main__":
    main()
