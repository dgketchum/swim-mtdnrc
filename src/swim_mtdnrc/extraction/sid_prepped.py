"""Assemble SID prepped directory tree from bucket mirror.

Run:
    python -m swim_mtdnrc.extraction.sid_prepped [options]
"""

from __future__ import annotations

import argparse
import logging
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import pandas as pd

log = logging.getLogger(__name__)

# Per-worker shapefile cache (populated by _init_worker)
_worker_gdf: gpd.GeoDataFrame | None = None
_worker_shapefile: str | None = None

DEFAULT_BUCKET_ROOT = Path("/nas/swim/sid/bucket")
DEFAULT_PREPPED_ROOT = Path("/nas/swim/sid/prepped")
DEFAULT_SHAPEFILE = Path(
    "/nas/Montana/statewide_irrigation_dataset"
    "/statewide_irrigation_dataset_15FEB2024_aea.shp"
)

# Sub-batch merging: output county → list of source bucket directories
BATCH_MAP: dict[str, list[str]] = {
    "073": ["073a", "073b"],
    "081": ["081a", "081b", "081c", "081d"],
}

ALL_MASKS = ("irr", "inv_irr", "no_mask")
ALL_VARIABLES = ("ndvi", "etf", "eta", "gis", "properties")


def _source_counties(county: str) -> list[str]:
    """Return the list of bucket subdirectories to read for this county."""
    return BATCH_MAP.get(county, [county])


def _concat_csvs(paths: list[Path]) -> pd.DataFrame:
    """Concatenate a list of CSVs; return empty DataFrame if none exist."""
    dfs = [pd.read_csv(p) for p in paths if p.exists()]
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _copy_ndvi(
    src_counties: list[str],
    bucket_root: Path,
    dst_dir: Path,
    masks: tuple[str, ...],
) -> None:
    """Merge sub-batch NDVI CSVs and write one file per year/mask.

    Handles two source layouts:
      - Annual: ndvi_{mask}_{year}.csv
      - Half:   ndvi_{mask}_{year}_h[12].csv  (merged into one annual file)
    Annual files take precedence over half files for the same year.
    """
    for mask in masks:
        # Build one DataFrame per (source-county, year), then concat across sources.
        # Each source county may use annual or half-year format — handle independently
        # so half-year sources are not dropped when another sub-batch has an annual file.
        year_dfs: dict[str, list[pd.DataFrame]] = {}

        for sc in src_counties:
            src = bucket_root / sc / "ndvi" / mask
            if not src.is_dir():
                continue

            annual: dict[str, Path] = {}
            halves: dict[str, list[Path]] = {}

            for f in sorted(src.glob("ndvi_*.csv")):
                m = re.search(r"ndvi_\w+_(\d{4})_h\d\.csv$", f.name)
                if m:
                    halves.setdefault(m.group(1), []).append(f)
                    continue
                m = re.search(r"ndvi_\w+_(\d{4})\.csv$", f.name)
                if m:
                    annual[m.group(1)] = f

            for year in sorted(set(annual) | set(halves)):
                if year in annual:
                    df = pd.read_csv(annual[year])
                else:
                    parts = [pd.read_csv(p) for p in sorted(halves[year])]
                    df = parts[0]
                    for p in parts[1:]:
                        df = df.merge(p, on="FID", how="outer")
                if not df.empty:
                    year_dfs.setdefault(year, []).append(df)

        if not year_dfs:
            continue

        out_dir = dst_dir / "ndvi" / mask
        out_dir.mkdir(parents=True, exist_ok=True)

        for year, dfs in sorted(year_dfs.items()):
            out_file = out_dir / f"ndvi_{mask}_{year}.csv"
            df = pd.concat(dfs, ignore_index=True)
            if df.empty:
                continue
            df.to_csv(out_file, index=False)
            log.debug("wrote %s", out_file)


def _copy_etf(
    src_counties: list[str],
    bucket_root: Path,
    dst_dir: Path,
    masks: tuple[str, ...],
    models: list[str] | None,
) -> None:
    """Merge sub-batch ETf CSVs, split by model, write one file per year/mask."""
    for mask in masks:
        # {model: {year: [Path]}}
        model_year_files: dict[str, dict[str, list[Path]]] = {}
        for sc in src_counties:
            src = bucket_root / sc / "etf" / mask
            if not src.is_dir():
                continue
            for f in sorted(src.glob("*_etf_*.csv")):
                m = re.match(r"(.+)_etf_\w+_(\d{4})\.csv", f.name)
                if not m:
                    continue
                model, year = m.group(1), m.group(2)
                if models and model not in models:
                    continue
                model_year_files.setdefault(model, {}).setdefault(year, []).append(f)

        for model, year_files in sorted(model_year_files.items()):
            out_dir = dst_dir / "etf" / model / mask
            out_dir.mkdir(parents=True, exist_ok=True)
            for year, paths in sorted(year_files.items()):
                out_file = out_dir / f"{model}_etf_{mask}_{year}.csv"
                df = _concat_csvs(paths)
                if df.empty:
                    continue
                df.to_csv(out_file, index=False)
                log.debug("wrote %s", out_file)


def _rename_eta_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename YYYY_MM columns to ensemble_eta_YYYYMM01."""
    rename = {}
    for col in df.columns:
        if col == "FID":
            continue
        m = re.fullmatch(r"(\d{4})_(\d{2})", col)
        if m:
            rename[col] = f"ensemble_eta_{m.group(1)}{m.group(2)}01"
    return df.rename(columns=rename)


def _copy_eta(
    src_counties: list[str],
    bucket_root: Path,
    dst_dir: Path,
    masks: tuple[str, ...],
) -> None:
    """Merge sub-batch ETa CSVs, rename columns, write one file per year/mask.

    Handles two source layouts:
      - Annual:  ensemble_eta_{mask}_{year}.csv        (one wide CSV per year)
      - Monthly: ensemble_eta_{mask}_{year}_{month}.csv (one column per month,
                 used when image gaps caused the annual extraction to drop)
    Annual files take precedence; monthly files are merged by FID into a wide
    DataFrame matching the annual format before column renaming.
    """
    for mask in masks:
        # Build one DataFrame per (source-county, year), then concat across sources.
        # Each source county may use annual format, monthly format, or a mix across
        # years — handle independently so no source is dropped when another uses a
        # different layout for the same year.
        year_dfs: dict[str, list[pd.DataFrame]] = {}

        for sc in src_counties:
            src = bucket_root / sc / "eta" / "monthly" / mask
            if not src.is_dir():
                continue

            annual: dict[str, Path] = {}
            monthly: dict[str, dict[str, Path]] = {}

            for f in sorted(src.glob("ensemble_eta_*.csv")):
                m = re.search(r"ensemble_eta_\w+_(\d{4})_(\d{2})\.csv$", f.name)
                if m:
                    monthly.setdefault(m.group(1), {})[m.group(2)] = f
                    continue
                m = re.search(r"ensemble_eta_\w+_(\d{4})\.csv$", f.name)
                if m:
                    annual[m.group(1)] = f

            for year in sorted(set(annual) | set(monthly)):
                if year in annual:
                    df = pd.read_csv(annual[year])
                else:
                    month_dfs = [
                        pd.read_csv(monthly[year][mo])
                        for mo in sorted(monthly[year])
                    ]
                    df = month_dfs[0]
                    for mdf in month_dfs[1:]:
                        df = df.merge(mdf, on="FID", how="outer")
                if not df.empty:
                    year_dfs.setdefault(year, []).append(df)

        if not year_dfs:
            continue

        out_dir = dst_dir / "eta" / mask
        out_dir.mkdir(parents=True, exist_ok=True)

        for year, dfs in sorted(year_dfs.items()):
            out_file = out_dir / f"ensemble_eta_{mask}_{year}.csv"
            df = pd.concat(dfs, ignore_index=True)
            if df.empty:
                continue
            df = _rename_eta_columns(df)
            df.to_csv(out_file, index=False)
            log.debug("wrote %s", out_file)


def _write_gis(gdf: gpd.GeoDataFrame, county: str, dst_dir: Path) -> None:
    """Write the per-county GIS subset to dst_dir/gis/sid_{county}.shp."""
    county_no = int(county)
    subset = gdf[gdf["COUNTY_NO"] == county_no]
    if subset.empty:
        log.warning("no features for county %s", county)
        return
    out_dir = dst_dir / "gis"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"sid_{county}.shp"
    subset.to_file(out_file, engine="fiona")
    log.debug("wrote %s (%d features)", out_file, len(subset))


def _copy_properties(
    county: str,
    src_counties: list[str],
    bucket_root: Path,
    dst_dir: Path,
) -> None:
    """Merge sub-batch properties CSVs into per-county output files.

    Expects the new schema with named files per property type:
      irrigation.csv  → irr_sid_{county}.csv
      ssurgo.csv      → ssurgo_{county}.csv
      landcover.csv   → landcover_{county}.csv
      cdl.csv         → cdl_{county}.csv
    """
    # Search each sub-batch dir; also check the main county dir if separate
    search_dirs = list(src_counties)
    if county not in src_counties:
        search_dirs.append(county)

    # property source name → output name suffix
    prop_files = {
        "irrigation.csv": f"irr_sid_{county}.csv",
        "ssurgo.csv": f"ssurgo_{county}.csv",
        "landcover.csv": f"landcover_{county}.csv",
        "cdl.csv": f"cdl_{county}.csv",
    }

    out_dir = dst_dir / "properties"
    found_any = False

    for src_name, out_name in prop_files.items():
        all_dfs: list[pd.DataFrame] = []
        for sc in search_dirs:
            f = bucket_root / sc / "properties" / src_name
            if not f.exists():
                continue
            try:
                all_dfs.append(pd.read_csv(f))
            except Exception as exc:
                log.warning("failed to read %s: %s", f, exc)

        if not all_dfs:
            continue

        found_any = True
        df = pd.concat(all_dfs, ignore_index=True).drop_duplicates(subset="FID")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / out_name
        df.to_csv(out_file, index=False)
        log.debug("wrote %s (%d rows)", out_file, len(df))

    if not found_any:
        log.warning("no properties found for county %s", county)


def assemble_county(
    county: str,
    bucket_root: Path,
    prepped_root: Path,
    gdf: gpd.GeoDataFrame | None,
    models: list[str] | None,
    masks: tuple[str, ...],
    variables: tuple[str, ...],
    overwrite: bool,
) -> None:
    """Assemble all requested variables for one county into the prepped layout."""
    src_counties = _source_counties(county)
    dst_dir = prepped_root / county
    log.info("assembling county %s ← %s", county, src_counties)

    if "ndvi" in variables:
        _copy_ndvi(src_counties, bucket_root, dst_dir, masks)

    if "etf" in variables:
        _copy_etf(src_counties, bucket_root, dst_dir, masks, models)

    if "eta" in variables:
        _copy_eta(src_counties, bucket_root, dst_dir, masks)

    if "gis" in variables and gdf is not None:
        _write_gis(gdf, county, dst_dir)

    if "properties" in variables:
        _copy_properties(county, src_counties, bucket_root, dst_dir)


def _all_counties(bucket_root: Path) -> list[str]:
    """Discover all output counties from the bucket mirror."""
    sub_batch_entries: set[str] = set()
    for batches in BATCH_MAP.values():
        sub_batch_entries.update(batches)

    output: list[str] = []
    for p in sorted(bucket_root.iterdir()):
        if not p.is_dir():
            continue
        name = p.name
        if name in sub_batch_entries:
            continue
        if re.fullmatch(r"\d{3}", name):
            output.append(name)

    # Add merged-county keys whose sub-batches exist in the bucket
    for merged, batches in BATCH_MAP.items():
        if any((bucket_root / b).is_dir() for b in batches):
            if merged not in output:
                output.append(merged)

    return sorted(output)


def _init_worker(shapefile_path: str | None) -> None:
    """Load shapefile once per worker process."""
    global _worker_gdf, _worker_shapefile
    _worker_shapefile = shapefile_path
    if shapefile_path:
        _worker_gdf = gpd.read_file(shapefile_path, engine="fiona")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def _assemble_county_worker(args: tuple) -> str:
    """Wrapper for ProcessPoolExecutor: unpack args and call assemble_county."""
    county, bucket_root, prepped_root, models, masks, variables, overwrite = args
    gdf = _worker_gdf if "gis" in variables else None
    assemble_county(
        county,
        Path(bucket_root),
        Path(prepped_root),
        gdf,
        models,
        masks,
        variables,
        overwrite,
    )
    return county


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Assemble SID prepped directory tree from bucket mirror"
    )
    parser.add_argument(
        "--bucket-root",
        default=str(DEFAULT_BUCKET_ROOT),
        help="Root of the local bucket mirror (default: /nas/swim/sid/bucket)",
    )
    parser.add_argument(
        "--prepped-root",
        default=str(DEFAULT_PREPPED_ROOT),
        help="Root of the prepped output tree (default: /nas/swim/sid/prepped)",
    )
    parser.add_argument(
        "--shapefile",
        default=str(DEFAULT_SHAPEFILE),
        help="SID shapefile path",
    )
    parser.add_argument(
        "--counties",
        default=None,
        help="Comma-separated county numbers (e.g. 003,073,081). Default: all.",
    )
    parser.add_argument(
        "--variables",
        default=",".join(ALL_VARIABLES),
        help="Comma-separated variables: ndvi,etf,eta,gis,properties. Default: all.",
    )
    parser.add_argument(
        "--models",
        default=None,
        help="Comma-separated ETf models to include. Default: all.",
    )
    parser.add_argument(
        "--masks",
        default=",".join(ALL_MASKS),
        help="Comma-separated mask types (default: irr,inv_irr).",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of parallel worker processes (default: 1).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    bucket_root = Path(args.bucket_root)
    prepped_root = Path(args.prepped_root)
    variables = tuple(v.strip() for v in args.variables.split(","))
    masks = tuple(m.strip() for m in args.masks.split(","))
    models = [m.strip() for m in args.models.split(",")] if args.models else None

    counties = (
        [c.strip() for c in args.counties.split(",")]
        if args.counties
        else _all_counties(bucket_root)
    )

    prepped_root.mkdir(parents=True, exist_ok=True)

    shapefile_path = args.shapefile if "gis" in variables else None

    if args.workers > 1:
        worker_args = [
            (county, str(bucket_root), str(prepped_root), models, masks, variables, args.overwrite)
            for county in counties
        ]
        with ProcessPoolExecutor(
            max_workers=args.workers,
            initializer=_init_worker,
            initargs=(shapefile_path,),
        ) as pool:
            futures = {pool.submit(_assemble_county_worker, a): a[0] for a in worker_args}
            for fut in as_completed(futures):
                county = futures[fut]
                try:
                    fut.result()
                    log.info("finished county %s", county)
                except Exception as exc:
                    log.error("county %s failed: %s", county, exc)
    else:
        gdf = None
        if shapefile_path:
            log.info("loading shapefile …")
            gdf = gpd.read_file(shapefile_path, engine="fiona")
        for county in counties:
            assemble_county(
                county,
                bucket_root,
                prepped_root,
                gdf,
                models,
                masks,
                variables,
                args.overwrite,
            )

    log.info("done — prepped root: %s", prepped_root)


if __name__ == "__main__":
    main()
