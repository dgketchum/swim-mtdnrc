"""Build SwimContainer for Tongue River Basin.

Creates a container at /nas/swim/examples/tongue/data/tongue.swim, ingests
GridMET, NDVI, ETf, SNODAS, and properties, then computes merged NDVI and
field dynamics.

Usage:
    python -m swim_mtdnrc.calibration.build_container
    python -m swim_mtdnrc.calibration.build_container --steps ingest
    python -m swim_mtdnrc.calibration.build_container --steps compute
    python -m swim_mtdnrc.calibration.build_container --steps ingest,compute
"""

import argparse
from pathlib import Path

from swimrs.container.container import SwimContainer

TONGUE_ROOT = Path("/nas/swim/examples/tongue")
DATA = TONGUE_ROOT / "data"

SHP_PATH = DATA / "gis/tongue_fields_gfid.shp"
CONTAINER_PATH = DATA / "tongue.swim"
MET_DIR = DATA / "met_timeseries/gridmet"
NDVI_DIR = DATA / "landsat/extracts/ndvi"
ETF_DIR = DATA / "landsat/extracts/etf"
SNODAS_DIR = DATA / "snow/snodas/extracts"
SSURGO_CSV = DATA / "properties/tongue_ssurgo.csv"
IRR_CSV = DATA / "properties/tongue_irr.csv"

START_DATE = "1989-01-01"
END_DATE = "2021-12-31"


def create_container(container_path=None, overwrite=False):
    """Create a new SwimContainer for the Tongue River Basin."""
    container_path = Path(container_path or CONTAINER_PATH)

    container = SwimContainer.create(
        uri=str(container_path),
        fields_shapefile=str(SHP_PATH),
        uid_column="FID",
        start_date=START_DATE,
        end_date=END_DATE,
        project_name="tongue",
        overwrite=overwrite,
        storage="directory",
    )
    print(f"Created container: {container_path}")
    return container


def ingest(container):
    """Ingest all data sources into the container."""
    print("\n--- GridMET ---")
    container.ingest.gridmet(
        source_dir=str(MET_DIR),
        grid_shapefile=str(SHP_PATH),
        uid_column="FID",
        grid_column="GFID",
    )

    print("\n--- NDVI (irr) ---")
    container.ingest.ndvi(
        source_dir=str(NDVI_DIR / "irr"),
        uid_column="FID",
        instrument="landsat",
        mask="irr",
    )

    print("\n--- NDVI (inv_irr) ---")
    container.ingest.ndvi(
        source_dir=str(NDVI_DIR / "inv_irr"),
        uid_column="FID",
        instrument="landsat",
        mask="inv_irr",
    )

    print("\n--- ETf (irr) ---")
    container.ingest.etf(
        source_dir=str(ETF_DIR / "irr"),
        uid_column="FID",
        model="ssebop",
        mask="irr",
        instrument="landsat",
    )

    print("\n--- ETf (inv_irr) ---")
    container.ingest.etf(
        source_dir=str(ETF_DIR / "inv_irr"),
        uid_column="FID",
        model="ssebop",
        mask="inv_irr",
        instrument="landsat",
    )

    print("\n--- SNODAS ---")
    container.ingest.snodas(
        source_dir=str(SNODAS_DIR),
        uid_column="FID",
    )

    print("\n--- Properties ---")
    container.ingest.properties(
        soils_csv=str(SSURGO_CSV),
        irr_csv=str(IRR_CSV),
        uid_column="FID",
    )

    print("\nIngestion complete.")


def compute(container):
    """Run compute steps: merged NDVI and dynamics."""
    print("\n--- Merged NDVI ---")
    container.compute.merged_ndvi(
        masks=("irr", "inv_irr"),
        instruments=("landsat",),
    )

    print("\n--- Dynamics ---")
    container.compute.dynamics(
        etf_model="ssebop",
        use_mask=True,
        use_lulc=False,
        masks=("irr", "inv_irr"),
        met_source="gridmet",
    )

    print("\nCompute complete.")


def build(container_path=None, steps=None, overwrite=False):
    """Build the SwimContainer: create, ingest, compute."""
    if steps is None:
        steps = ["ingest", "compute"]

    container_path = Path(container_path or CONTAINER_PATH)

    if "ingest" in steps and ("compute" not in steps or steps.index("ingest") == 0):
        container = create_container(container_path, overwrite=overwrite)
    else:
        container = SwimContainer.open(str(container_path), mode="r+")
        print(f"Opened existing container: {container_path}")

    try:
        for step in steps:
            print(f"\n=== {step} ===")
            if step == "ingest":
                ingest(container)
            elif step == "compute":
                compute(container)
            else:
                print(f"Unknown step: {step}")
    finally:
        container.close()
        print(f"\nContainer closed: {container_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Build SwimContainer for Tongue River Basin"
    )
    parser.add_argument(
        "--container",
        type=str,
        default=str(CONTAINER_PATH),
        help="Path to container (default: %(default)s)",
    )
    parser.add_argument(
        "--steps",
        type=str,
        default="ingest,compute",
        help="Comma-separated steps: ingest, compute (default: ingest,compute)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing container",
    )
    args = parser.parse_args()

    steps = [s.strip() for s in args.steps.split(",")]
    build(container_path=args.container, steps=steps, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
