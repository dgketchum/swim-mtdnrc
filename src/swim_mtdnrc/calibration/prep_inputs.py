"""Prepare inputs for Tongue River Basin SwimContainer.

Utilities:
  1a. Deduplicate shapefile (2,084 rows → 2,000 unique FIDs)
  1b. Convert GridMET parquets to container format (rename + remap columns)
  1c. Convert SNODAS JSON files to CSV format (mm → meters)
  1d. Extend GridMET parquets to cover full project date range (append mode)

Usage:
    python -m swim_mtdnrc.calibration.prep_inputs --all
    python -m swim_mtdnrc.calibration.prep_inputs --dedup-shp
    python -m swim_mtdnrc.calibration.prep_inputs --gridmet
    python -m swim_mtdnrc.calibration.prep_inputs --snodas
    python -m swim_mtdnrc.calibration.prep_inputs --extend-gridmet
"""

import argparse
import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

TONGUE_ROOT = Path("/nas/swim/examples/tongue")

SHP_PATH = TONGUE_ROOT / "data/gis/tongue_fields_gfid.shp"
GRIDMET_DIR = TONGUE_ROOT / "data/met_timeseries/gridmet"
BIAS_TIF_DIR = TONGUE_ROOT / "data/bias_correction_tif"
FACTORS_JSON = TONGUE_ROOT / "data/met_timeseries/gridmet_factors.json"

SNODAS_OUT = TONGUE_ROOT / "data/snow/snodas/extracts"

GRIDMET_COL_MAP = {
    "tmin_c": "tmin",
    "tmax_c": "tmax",
    "eto_mm_uncorr": "eto",
    "eto_mm": "eto_corr",
    "etr_mm_uncorr": "etr",
    "etr_mm": "etr_corr",
    "prcp_mm": "prcp",
    "srad_wm2": "srad",
    "elev_m": "elev",
}

KEEP_COLS = list(GRIDMET_COL_MAP.values())


def deduplicate_shapefile(shp_path=None, dry_run=False):
    """Remove duplicate FIDs from shapefile, keeping first occurrence."""
    shp_path = Path(shp_path or SHP_PATH)
    gdf = gpd.read_file(shp_path, engine="fiona")
    n_before = len(gdf)
    n_unique = gdf["FID"].nunique()
    n_dupes = n_before - n_unique

    print(f"Shapefile: {n_before} rows, {n_unique} unique FIDs, {n_dupes} duplicates")

    if n_dupes == 0:
        print("No duplicates found, nothing to do.")
        return

    gdf_dedup = gdf.drop_duplicates(subset="FID", keep="first").copy()
    print(f"After dedup: {len(gdf_dedup)} rows")

    if dry_run:
        print("Dry run — not writing.")
        return

    gdf_dedup.to_file(shp_path, engine="fiona")
    print(f"Wrote deduplicated shapefile: {shp_path}")


def convert_gridmet(gridmet_dir=None, output_dir=None, dry_run=False):
    """Rename and remap GridMET parquets to container format.

    Input:  gridmet_historical_{gfid}.parquet (39 columns)
    Output: {gfid}.parquet (9 columns: tmin, tmax, eto, eto_corr, etr, etr_corr, prcp, srad, elev)
    """
    gridmet_dir = Path(gridmet_dir or GRIDMET_DIR)
    output_dir = Path(output_dir or gridmet_dir)

    parquets = sorted(gridmet_dir.glob("gridmet_historical_*.parquet"))
    if not parquets:
        print(f"No gridmet_historical_*.parquet files in {gridmet_dir}")
        return

    print(f"Found {len(parquets)} GridMET parquets to convert")

    for pq in parquets:
        gfid = pq.stem.replace("gridmet_historical_", "")
        out_path = output_dir / f"{gfid}.parquet"

        if dry_run:
            print(f"  {pq.name} → {out_path.name}")
            continue

        df = pd.read_parquet(pq)
        df = df.rename(columns=GRIDMET_COL_MAP)
        df = df[KEEP_COLS]
        df.to_parquet(out_path)

    if not dry_run:
        # Remove old files after all new ones are written
        for pq in parquets:
            gfid = pq.stem.replace("gridmet_historical_", "")
            new_path = output_dir / f"{gfid}.parquet"
            if new_path.exists() and pq != new_path:
                pq.unlink()

    print(f"Converted {len(parquets)} parquets → {output_dir}")


def convert_snodas(
    tongue_json,
    annex_json,
    output_dir=None,
    dry_run=False,
):
    """Merge legacy SNODAS JSONs and write monthly CSVs.

    JSON structure: {date_YYYYMMDD: {fid: swe_mm, ...}, ...}
    Output CSVs: rows=FID, columns=YYYYMMDD dates, values=SWE in meters.
    The ingestor multiplies by 1000 to store as mm internally.
    """
    tongue_json = Path(tongue_json)
    annex_json = Path(annex_json)
    output_dir = Path(output_dir or SNODAS_OUT)

    print(f"Loading {tongue_json.name}...")
    with open(tongue_json) as f:
        tongue_data = json.load(f)

    print(f"Loading {annex_json.name}...")
    with open(annex_json) as f:
        annex_data = json.load(f)

    # Merge: combine FID dicts for overlapping dates
    all_dates = sorted(set(tongue_data.keys()) | set(annex_data.keys()))
    print(f"Total dates: {len(all_dates)} ({all_dates[0]} to {all_dates[-1]})")

    # Collect all unique FIDs
    all_fids = set()
    for d in tongue_data.values():
        all_fids.update(d.keys())
    for d in annex_data.values():
        all_fids.update(d.keys())
    all_fids = sorted(all_fids, key=lambda x: int(x))
    print(f"Total FIDs: {len(all_fids)}")

    # Group dates by year-month
    monthly_dates = {}
    for date_str in all_dates:
        ym = date_str[:6]  # YYYYMM
        monthly_dates.setdefault(ym, []).append(date_str)

    output_dir.mkdir(parents=True, exist_ok=True)
    n_written = 0

    for ym, dates in sorted(monthly_dates.items()):
        out_path = output_dir / f"snodas_{ym}.csv"

        if dry_run:
            print(f"  {ym}: {len(dates)} dates → {out_path.name}")
            continue

        # Build DataFrame: rows=FID, columns=date strings
        data = {}
        for date_str in sorted(dates):
            col_values = {}
            merged = {}
            if date_str in tongue_data:
                merged.update(tongue_data[date_str])
            if date_str in annex_data:
                merged.update(annex_data[date_str])

            for fid in all_fids:
                val = merged.get(fid, np.nan)
                if val is not None and not (isinstance(val, float) and np.isnan(val)):
                    # Convert mm → meters
                    col_values[fid] = float(val) / 1000.0
                else:
                    col_values[fid] = np.nan
            data[date_str] = col_values

        df = pd.DataFrame(data, index=all_fids)
        df.index.name = "FID"
        df.to_csv(out_path)
        n_written += 1

    print(f"Wrote {n_written} monthly SNODAS CSVs → {output_dir}")


def extend_gridmet(
    shp_path=None,
    gridmet_dir=None,
    bias_tif_dir=None,
    factors_json=None,
    start="1987-01-01",
    end="2025-12-31",
):
    """Extend GridMET parquets to cover the full project date range.

    1. Generate bias-correction factors JSON (if missing).
    2. Append new dates to existing parquets via download_gridmet(append=True).
    """
    from swimrs.data_extraction.gridmet.gridmet import (
        download_gridmet,
        sample_gridmet_corrections,
    )

    shp_path = str(shp_path or SHP_PATH)
    gridmet_dir = str(gridmet_dir or GRIDMET_DIR)
    bias_tif_dir = str(bias_tif_dir or BIAS_TIF_DIR)
    factors_json = str(factors_json or FACTORS_JSON)

    if not Path(factors_json).exists():
        print("Generating GridMET correction factors...")
        sample_gridmet_corrections(shp_path, bias_tif_dir, factors_json)
    else:
        print(f"Correction factors already exist: {factors_json}")

    print(f"Appending GridMET data {start} to {end}...")
    download_gridmet(
        fields=shp_path,
        gridmet_factors=factors_json,
        gridmet_csv_dir=gridmet_dir,
        start=start,
        end=end,
        append=True,
        feature_id="FID",
    )
    print("GridMET extension complete.")


def main():
    parser = argparse.ArgumentParser(
        description="Prepare Tongue River Basin inputs for SwimContainer"
    )
    parser.add_argument("--all", action="store_true", help="Run all prep steps")
    parser.add_argument(
        "--dedup-shp", action="store_true", help="Deduplicate shapefile"
    )
    parser.add_argument(
        "--gridmet", action="store_true", help="Convert GridMET parquets"
    )
    parser.add_argument("--snodas", action="store_true", help="Convert SNODAS JSONs")
    parser.add_argument(
        "--extend-gridmet",
        action="store_true",
        help="Extend GridMET parquets to 2025 (append mode)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done"
    )
    parser.add_argument("--shp-path", type=str, help="Override shapefile path")
    parser.add_argument("--gridmet-dir", type=str, help="Override GridMET directory")
    parser.add_argument(
        "--output-dir", type=str, help="Override SNODAS output directory"
    )
    parser.add_argument("--tongue-json", type=str, help="Path to tongue SNODAS JSON")
    parser.add_argument("--annex-json", type=str, help="Path to annex SNODAS JSON")
    args = parser.parse_args()

    if not any(
        [args.all, args.dedup_shp, args.gridmet, args.snodas, args.extend_gridmet]
    ):
        parser.print_help()
        return

    if args.all or args.dedup_shp:
        print("\n=== 1a. Deduplicate Shapefile ===")
        deduplicate_shapefile(shp_path=args.shp_path, dry_run=args.dry_run)

    if args.all or args.gridmet:
        print("\n=== 1b. Convert GridMET Parquets ===")
        convert_gridmet(gridmet_dir=args.gridmet_dir, dry_run=args.dry_run)

    if args.all or args.snodas:
        if not args.tongue_json or not args.annex_json:
            parser.error("--tongue-json and --annex-json required for --snodas")
        print("\n=== 1c. Convert SNODAS JSONs ===")
        convert_snodas(
            tongue_json=args.tongue_json,
            annex_json=args.annex_json,
            output_dir=args.output_dir,
            dry_run=args.dry_run,
        )

    if args.extend_gridmet:
        print("\n=== 1d. Extend GridMET (append 2022-2025) ===")
        extend_gridmet(
            shp_path=args.shp_path,
            gridmet_dir=args.gridmet_dir,
        )


if __name__ == "__main__":
    main()
