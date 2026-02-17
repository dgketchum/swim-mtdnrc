"""Merge tongue + tongue_annex EE CSV extracts into unified per-year files.

Concatenates NDVI (and optionally ETf) extracts from the legacy tongue (FIDs 1-1916)
and tongue_annex (FIDs 1917-2000) directories into single per-year CSVs with all
2,000 fields. Output goes to tongue_new/data/landsat/merged/.

Usage:
    python -m swim_mtdnrc.clustering.merge_extracts \
        --variable ndvi --mask-types irr,inv_irr
"""

import argparse
import os

import pandas as pd

TONGUE_ROOT = "/nas/swim/examples/tongue/landsat/extracts"
ANNEX_ROOT = "/nas/swim/examples/tongue_annex/landsat/extracts"
OUTPUT_ROOT = "/nas/swim/examples/tongue_new/data/landsat/merged"

YEARS = list(range(1987, 2022))
FEATURE_ID = "FID"


def merge_variable(variable, mask_type, years=None, dry_run=False):
    """Merge tongue + annex CSVs for a single variable/mask combination.

    Parameters
    ----------
    variable : str
        'ndvi' or 'etf'
    mask_type : str
        'irr' or 'inv_irr'
    years : list[int] or None
        Years to process. Defaults to 1987-2021.
    dry_run : bool
        If True, print what would be done without writing.

    Returns
    -------
    dict
        {year: n_fields} for successfully merged years.
    """
    if years is None:
        years = YEARS

    tongue_dir = os.path.join(TONGUE_ROOT, variable, mask_type)
    annex_dir = os.path.join(ANNEX_ROOT, variable, mask_type)
    out_dir = os.path.join(OUTPUT_ROOT, variable, mask_type)

    if not os.path.isdir(tongue_dir):
        raise FileNotFoundError(f"Tongue directory not found: {tongue_dir}")
    if not os.path.isdir(annex_dir):
        raise FileNotFoundError(f"Annex directory not found: {annex_dir}")

    if not dry_run:
        os.makedirs(out_dir, exist_ok=True)

    results = {}

    for year in years:
        fname = f"{variable}_{year}_{mask_type}.csv"
        tongue_path = os.path.join(tongue_dir, fname)
        annex_path = os.path.join(annex_dir, fname)

        if not os.path.exists(tongue_path):
            print(f"  SKIP {year}: tongue file missing ({tongue_path})")
            continue
        if not os.path.exists(annex_path):
            print(f"  SKIP {year}: annex file missing ({annex_path})")
            continue

        tongue_df = pd.read_csv(tongue_path)
        annex_df = pd.read_csv(annex_path)

        # Verify no FID overlap
        tongue_fids = set(tongue_df[FEATURE_ID])
        annex_fids = set(annex_df[FEATURE_ID])
        overlap = tongue_fids & annex_fids
        if overlap:
            print(
                f"  WARNING {year}: {len(overlap)} overlapping FIDs: {sorted(overlap)[:5]}..."
            )

        # Ensure same columns (scene IDs should match since same path/row)
        tongue_cols = set(tongue_df.columns) - {FEATURE_ID}
        annex_cols = set(annex_df.columns) - {FEATURE_ID}
        if tongue_cols != annex_cols:
            # Use union of columns — missing scenes become NaN
            all_cols = sorted(tongue_cols | annex_cols)
            print(
                f"  INFO {year}: tongue has {len(tongue_cols)} scenes, "
                f"annex has {len(annex_cols)} scenes, union={len(all_cols)}"
            )
        else:
            all_cols = sorted(tongue_cols)

        merged = pd.concat([tongue_df, annex_df], ignore_index=True)
        merged = merged.sort_values(FEATURE_ID).reset_index(drop=True)

        # Verify FID uniqueness
        if merged[FEATURE_ID].duplicated().any():
            dups = merged[FEATURE_ID][merged[FEATURE_ID].duplicated()].tolist()
            print(f"  WARNING {year}: duplicate FIDs after merge: {dups[:5]}...")

        n_fields = len(merged)

        if dry_run:
            print(f"  {year}: would write {n_fields} fields x {len(all_cols)} scenes")
        else:
            out_path = os.path.join(out_dir, fname)
            # Reorder columns: FID first, then sorted scene IDs
            col_order = [FEATURE_ID] + [c for c in all_cols if c in merged.columns]
            merged[col_order].to_csv(out_path, index=False)
            print(f"  {year}: {n_fields} fields x {len(all_cols)} scenes -> {out_path}")

        results[year] = n_fields

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Merge tongue + annex EE extracts into unified per-year CSVs"
    )
    parser.add_argument(
        "--variable",
        choices=["ndvi", "etf"],
        default="ndvi",
        help="Variable to merge (default: ndvi)",
    )
    parser.add_argument(
        "--mask-types",
        type=str,
        default="irr,inv_irr",
        help="Comma-separated mask types",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma-separated years (default: 1987-2021)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without writing",
    )
    args = parser.parse_args()

    mask_types = [m.strip() for m in args.mask_types.split(",")]
    years = [int(y) for y in args.years.split(",")] if args.years else None

    for mask_type in mask_types:
        print(f"\n=== {args.variable} / {mask_type} ===")
        results = merge_variable(
            args.variable, mask_type, years=years, dry_run=args.dry_run
        )
        print(f"  Merged {len(results)} years")


if __name__ == "__main__":
    main()
