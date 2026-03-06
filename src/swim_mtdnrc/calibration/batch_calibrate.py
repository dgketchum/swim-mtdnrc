"""Batch calibration for Tongue River Basin using PEST++ IES.

Partitions ~2,000 fields into ~40 GFID-based batches of ~50 fields each,
builds PEST++ setups for each batch, runs them, and merges results.

Usage:
    python -m swim_mtdnrc.calibration.batch_calibrate --action prep
    python -m swim_mtdnrc.calibration.batch_calibrate --action build-all
    python -m swim_mtdnrc.calibration.batch_calibrate --action run-batch --batch-id 0
    python -m swim_mtdnrc.calibration.batch_calibrate --action run-all
    python -m swim_mtdnrc.calibration.batch_calibrate --action merge
"""

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd

TONGUE_ROOT = Path("/nas/swim/examples/tongue")
DEFAULT_CONTAINER = TONGUE_ROOT / "data/tongue.swim"
DEFAULT_TOML = TONGUE_ROOT / "tongue.toml"
DEFAULT_OUTPUT = TONGUE_ROOT / "pestrun"
DEFAULT_SHP = TONGUE_ROOT / "data/gis/tongue_fields_gfid.shp"


def partition_fields_by_gfid(shapefile, batch_size=50):
    """Group FIDs by GFID and greedily pack into batches.

    Parameters
    ----------
    shapefile : str or Path
        Path to shapefile with FID and GFID columns.
    batch_size : int
        Target number of fields per batch.

    Returns
    -------
    list[list[str]]
        Each inner list is a batch of FID strings.
    """
    gdf = gpd.read_file(str(shapefile), engine="fiona")
    gdf = gdf.drop_duplicates(subset="FID", keep="first")

    # Group FIDs by GFID
    groups = {}
    for _, row in gdf.iterrows():
        gfid = str(int(row["GFID"]))
        fid = str(int(row["FID"]))
        groups.setdefault(gfid, []).append(fid)

    # Greedy bin-packing: add GFIDs to current batch until it exceeds target
    batches = []
    current_batch = []
    for gfid in sorted(groups.keys(), key=int):
        fids = groups[gfid]
        if current_batch and len(current_batch) + len(fids) > batch_size:
            batches.append(current_batch)
            current_batch = []
        current_batch.extend(fids)

    if current_batch:
        batches.append(current_batch)

    return batches


def build_batch(
    container_path, toml_path, batch_fids, batch_id, output_root, noptmax=4, reals=200
):
    """Build PEST++ setup for a single batch of fields.

    Parameters
    ----------
    container_path : str or Path
        Path to .swim container.
    toml_path : str or Path
        Path to project TOML config.
    batch_fids : list[str]
        Field UIDs in this batch.
    batch_id : int
        Batch index.
    output_root : str or Path
        Root directory for batch outputs.
    noptmax : int
        Maximum PEST++ optimization iterations.
    reals : int
        Number of ensemble realizations.
    """
    from swimrs.calibrate.pest_builder import PestBuilder
    from swimrs.container.container import SwimContainer
    from swimrs.swim.config import ProjectConfig

    batch_dir = Path(output_root) / f"batch_{batch_id:03d}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    config = ProjectConfig()
    config.read_config(
        str(toml_path), calibrate=True, calibration_dir_override=str(batch_dir)
    )

    container = SwimContainer.open(str(container_path), mode="r")

    # Subset container field list — keep original zarr indices intact so
    # downstream reads (exporter, calculator) pull the correct array positions.
    batch_fid_set = set(batch_fids)
    container._field_uids = [
        uid for uid in container._field_uids if uid in batch_fid_set
    ]

    try:
        builder = PestBuilder(config, container)
        print(f"  Batch {batch_id:03d}: spinup ({len(batch_fids)} fields)...")
        builder.spinup()
        print(f"  Batch {batch_id:03d}: build_pest...")
        builder.build_pest(target_etf="ssebop")
        print(f"  Batch {batch_id:03d}: build_localizer...")
        builder.build_localizer()
        print(f"  Batch {batch_id:03d}: write_control_settings...")
        builder.write_control_settings(noptmax=noptmax, reals=reals)
        print(f"  Batch {batch_id:03d}: done.")
    finally:
        builder.close()


def run_batch(batch_dir, num_workers=10, pst_name=None):
    """Run PEST++ IES for a single batch.

    Parameters
    ----------
    batch_dir : str or Path
        Directory containing the batch's PEST++ setup.
    num_workers : int
        Number of parallel PEST++ workers.
    pst_name : str or None
        Name of the .pst file. Auto-detected if None.
    """
    from swimrs.calibrate.run_pest import run_pst

    batch_dir = Path(batch_dir)
    pest_dir = batch_dir / "pest"
    master_dir = batch_dir / "master"
    workers_dir = batch_dir / "workers"

    if pst_name is None:
        pst_files = list(pest_dir.glob("*.pst"))
        if not pst_files:
            raise FileNotFoundError(f"No .pst file found in {pest_dir}")
        pst_name = pst_files[0].name

    print(f"Running PEST++ IES: {pest_dir / pst_name} with {num_workers} workers")
    run_pst(
        _dir=str(pest_dir),
        _cmd="pestpp-ies",
        pst_file=pst_name,
        num_workers=num_workers,
        worker_root=str(workers_dir),
        master_dir=str(master_dir),
    )


def merge_parameters(output_root):
    """Merge calibrated parameters from all batches.

    Reads the final .par.csv from each batch's master directory and
    concatenates into a unified parameter set.

    Parameters
    ----------
    output_root : str or Path
        Root directory containing batch_NNN subdirectories.

    Returns
    -------
    Path
        Path to the merged CSV.
    """
    output_root = Path(output_root)
    batch_dirs = sorted(output_root.glob("batch_*"))

    all_params = []
    for bd in batch_dirs:
        master = bd / "master"
        par_files = sorted(master.glob("*.par.csv"))
        if not par_files:
            print(f"  Warning: no .par.csv in {master}, skipping")
            continue
        # Use the last (most recent) iteration
        par_csv = par_files[-1]
        df = pd.read_csv(par_csv)
        df["batch"] = bd.name
        all_params.append(df)
        print(f"  {bd.name}: {len(df)} parameters from {par_csv.name}")

    if not all_params:
        print("No parameter files found.")
        return None

    merged = pd.concat(all_params, ignore_index=True)
    out_path = output_root / "tongue_calibrated_params.csv"
    merged.to_csv(out_path, index=False)
    print(f"\nMerged {len(merged)} parameters → {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(
        description="Batch PEST++ IES calibration for Tongue River Basin"
    )
    parser.add_argument(
        "--action",
        required=True,
        choices=["prep", "build-all", "run-batch", "run-all", "merge"],
        help="Action to perform",
    )
    parser.add_argument("--batch-id", type=int, help="Batch ID for run-batch")
    parser.add_argument("--batch-size", type=int, default=50, help="Fields per batch")
    parser.add_argument(
        "--workers", type=int, default=10, help="PEST workers per batch"
    )
    parser.add_argument("--noptmax", type=int, default=4, help="Max PEST iterations")
    parser.add_argument("--reals", type=int, default=200, help="Ensemble realizations")
    parser.add_argument(
        "--container",
        type=str,
        default=str(DEFAULT_CONTAINER),
        help="Path to .swim container",
    )
    parser.add_argument(
        "--toml", type=str, default=str(DEFAULT_TOML), help="Path to TOML config"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(DEFAULT_OUTPUT),
        help="Root output directory for batches",
    )
    parser.add_argument(
        "--shapefile",
        type=str,
        default=str(DEFAULT_SHP),
        help="Path to fields shapefile",
    )
    args = parser.parse_args()

    output_root = Path(args.output)
    output_root.mkdir(parents=True, exist_ok=True)

    if args.action == "prep":
        batches = partition_fields_by_gfid(args.shapefile, args.batch_size)
        print(f"Partitioned into {len(batches)} batches:")
        for i, batch in enumerate(batches):
            print(f"  Batch {i:03d}: {len(batch)} fields")
        # Write batch manifest
        manifest = output_root / "batch_manifest.csv"
        rows = []
        for i, batch in enumerate(batches):
            for fid in batch:
                rows.append({"batch_id": i, "FID": fid})
        pd.DataFrame(rows).to_csv(manifest, index=False)
        print(f"\nWrote manifest: {manifest}")

    elif args.action == "build-all":
        batches = partition_fields_by_gfid(args.shapefile, args.batch_size)
        print(f"Building {len(batches)} batches...")
        for i, batch_fids in enumerate(batches):
            print(f"\n--- Batch {i:03d} ({len(batch_fids)} fields) ---")
            build_batch(
                args.container,
                args.toml,
                batch_fids,
                i,
                output_root,
                noptmax=args.noptmax,
                reals=args.reals,
            )

    elif args.action == "run-batch":
        if args.batch_id is None:
            parser.error("--batch-id required for run-batch")
        batch_dir = output_root / f"batch_{args.batch_id:03d}"
        if not batch_dir.exists():
            parser.error(f"Batch directory not found: {batch_dir}")
        run_batch(batch_dir, num_workers=args.workers)

    elif args.action == "run-all":
        batch_dirs = sorted(output_root.glob("batch_*"))
        if not batch_dirs:
            parser.error(f"No batch directories found in {output_root}")
        print(f"Running {len(batch_dirs)} batches sequentially...")
        for bd in batch_dirs:
            print(f"\n=== {bd.name} ===")
            run_batch(bd, num_workers=args.workers)

    elif args.action == "merge":
        merge_parameters(output_root)


if __name__ == "__main__":
    main()
