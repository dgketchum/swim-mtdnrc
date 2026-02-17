"""Build SwimContainer for Tongue River Basin (tongue_new).

Writes the TOML config and invokes SwimContainer to ingest GridMET, NDVI, ETf,
properties, and bias correction rasters.

Usage:
    python -m swim_mtdnrc.calibration.build_container
"""

import argparse
import os

import toml

from swimrs.container.container import SwimContainer

TONGUE_NEW_ROOT = "/nas/swim/examples/tongue_new"
TOML_PATH = os.path.join(TONGUE_NEW_ROOT, "tongue_new_swim.toml")

DEFAULT_CONFIG = {
    "project": {
        "root": TONGUE_NEW_ROOT,
        "name": "tongue_new",
        "field_index": "FID",
        "shapefile": "{root}/data/gis/tongue_fields_gfid.shp",
    },
    "time": {
        "start_date": "1989-01-01",
        "end_date": "2021-12-31",
    },
    "remote_sensing": {
        "kc_proxy": "etf",
        "cover_proxy": "ndvi",
        "etf_target_model": "ssebop",
        "instruments": ["landsat"],
        "mask_mode": "irr",
    },
    "meteorology": {
        "source": "gridmet",
        "refet_type": "eto",
    },
    "misc": {
        "runoff_process": "cn",
    },
}


def write_toml(config, path):
    """Write TOML config file."""
    with open(path, "w") as f:
        toml.dump(config, f)
    print(f"Wrote TOML config: {path}")


def build(config_path=None, steps=None):
    """Build the SwimContainer from TOML config.

    Parameters
    ----------
    config_path : str
        Path to TOML config. Defaults to TOML_PATH.
    steps : list[str] or None
        Steps to run: 'ingest', 'compute', etc. Default runs all.
    """
    if config_path is None:
        config_path = TOML_PATH

    if not os.path.exists(config_path):
        print(f"Config not found, writing default: {config_path}")
        write_toml(DEFAULT_CONFIG, config_path)

    sc = SwimContainer(config_path)

    if steps is None:
        steps = ["ingest"]

    for step in steps:
        print(f"\n=== {step} ===")
        if step == "ingest":
            sc.ingest.gridmet()
            sc.ingest.ndvi()
            sc.ingest.etf()
            sc.ingest.properties()
        elif step == "compute":
            sc.compute.all()
        else:
            print(f"  Unknown step: {step}")


def main():
    parser = argparse.ArgumentParser(
        description="Build SwimContainer for Tongue River Basin"
    )
    parser.add_argument(
        "--write-toml-only",
        action="store_true",
        help="Only write the TOML config, don't build",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=TOML_PATH,
        help="Path to TOML config",
    )
    parser.add_argument(
        "--steps",
        type=str,
        default="ingest",
        help="Comma-separated steps to run (default: ingest)",
    )
    args = parser.parse_args()

    if args.write_toml_only:
        write_toml(DEFAULT_CONFIG, args.config)
        return

    steps = [s.strip() for s in args.steps.split(",")]
    build(args.config, steps)


if __name__ == "__main__":
    main()
