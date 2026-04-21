"""Build a crop-type substitution scenario container and generate diagnostics.

Usage:
    python scripts/run_scenario.py \
        --scenario /path/to/scenario.toml \
        --output /path/to/tongue_corn_expansion.swim \
        --report-dir /path/to/reports/corn_expansion/

    # Or from CSV:
    python scripts/run_scenario.py \
        --csv /path/to/substitutions.csv \
        --library /path/to/tongue_crop_library.json \
        --source /path/to/tongue_hindcast.swim \
        --name corn_expansion \
        --output /path/to/tongue_corn_expansion.swim
"""

import argparse

from swim_mtdnrc.clustering.crop_library import load_crop_library
from swim_mtdnrc.scenarios.scenario_container import create_scenario_container
from swim_mtdnrc.scenarios.scenario_spec import ScenarioSpec


def main():
    parser = argparse.ArgumentParser(
        description="Build a crop-type substitution scenario container"
    )

    # TOML mode
    parser.add_argument("--scenario", type=str, help="Scenario TOML file")

    # CSV mode
    parser.add_argument("--csv", type=str, help="Substitutions CSV (FID, crop)")
    parser.add_argument("--library", type=str, help="Crop library JSON path")
    parser.add_argument("--source", type=str, help="Source container path")
    parser.add_argument("--name", type=str, help="Scenario name (CSV mode)")

    # Common
    parser.add_argument("--output", required=True, help="Output container path")
    parser.add_argument(
        "--report-dir", type=str, default=None, help="Diagnostics output directory"
    )
    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    if args.scenario:
        spec = ScenarioSpec.from_toml(args.scenario)
    elif args.csv:
        if not all([args.library, args.source, args.name]):
            parser.error("CSV mode requires --library, --source, and --name")
        spec = ScenarioSpec.from_csv(
            csv_path=args.csv,
            library_path=args.library,
            source_container=args.source,
            name=args.name,
        )
    else:
        parser.error("Provide either --scenario (TOML) or --csv")

    # Build scenario container
    create_scenario_container(
        spec=spec,
        output_path=args.output,
        overwrite=args.overwrite,
    )

    # Generate diagnostics report
    if args.report_dir:
        from swim_mtdnrc.scenarios.diagnostics import scenario_report

        library = load_crop_library(spec.crop_library_path)
        scenario_report(
            source_path=spec.source_container,
            scenario_path=args.output,
            spec=spec,
            library=library,
            output_dir=args.report_dir,
        )


if __name__ == "__main__":
    main()
