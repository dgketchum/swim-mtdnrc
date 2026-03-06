"""Tongue-SID spatial crosswalk builder.

Maps each Tongue field (FID 1-2000) to its best-matching SID field via
spatial intersection. Montana fields get SID FIDs for pulling ETf/NDVI
from gs://wudr/sid/<county>/. Wyoming and unmatched fields are flagged
as no_match for later direct EE extraction.

Usage:
    python -m swim_mtdnrc.calibration.crosswalk
    python -m swim_mtdnrc.calibration.crosswalk --dry-run
    python -m swim_mtdnrc.calibration.crosswalk --tongue-shp <path> --sid-shp <path>
"""

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd

TONGUE_ROOT = Path("/nas/swim/examples/tongue")
TONGUE_SHP = TONGUE_ROOT / "data/gis/tongue_fields_gfid.shp"
SID_SHP = Path(
    "/nas/Montana/statewide_irrigation_dataset/"
    "statewide_irrigation_dataset_15FEB2024_aea.shp"
)
OUTPUT_CSV = TONGUE_ROOT / "data/gis/tongue_sid_crosswalk.csv"

COVERAGE_THRESHOLD = 0.95
DISTANCE_THRESHOLD = 10.0  # meters


def build_crosswalk(
    tongue_shp=None,
    sid_shp=None,
    output_csv=None,
    coverage_threshold=COVERAGE_THRESHOLD,
    distance_threshold=DISTANCE_THRESHOLD,
    dry_run=False,
):
    """Build spatial crosswalk between Tongue fields and SID fields.

    Returns the crosswalk DataFrame.
    """
    tongue_shp = Path(tongue_shp or TONGUE_SHP)
    sid_shp = Path(sid_shp or SID_SHP)
    output_csv = Path(output_csv or OUTPUT_CSV)

    print(f"Loading Tongue shapefile: {tongue_shp}")
    tongue = gpd.read_file(tongue_shp, engine="fiona")
    print(f"  {len(tongue)} fields, CRS: {tongue.crs}")

    print(f"Loading SID shapefile: {sid_shp}")
    sid = gpd.read_file(sid_shp, engine="fiona")
    print(f"  {len(sid)} fields, CRS: {sid.crs}")

    if dry_run:
        print("\nDry run — exiting after loading shapefiles.")
        return None

    # Ensure matching CRS
    if tongue.crs != sid.crs:
        print(f"Reprojecting SID from {sid.crs} to {tongue.crs}")
        sid = sid.to_crs(tongue.crs)

    # Rename SID FID to avoid collision with Tongue FID
    sid = sid.rename(columns={"FID": "SID_FID"})

    # Spatial filter: keep only SID fields that intersect the Tongue bbox
    tongue_bbox = tongue.total_bounds  # (minx, miny, maxx, maxy)
    sid_filtered = sid.cx[
        tongue_bbox[0] : tongue_bbox[2], tongue_bbox[1] : tongue_bbox[3]
    ]
    print(f"SID fields in Tongue bbox: {len(sid_filtered)}")

    # Compute areas before overlay
    tongue["tongue_area"] = tongue.geometry.area
    sid_filtered = sid_filtered.copy()
    sid_filtered["sid_area"] = sid_filtered.geometry.area

    # Spatial overlay (intersection)
    print("Computing spatial intersection...")
    overlay = gpd.overlay(
        tongue[["FID", "STATE", "tongue_area", "geometry"]],
        sid_filtered[["SID_FID", "COUNTY_NO", "sid_area", "geometry"]],
        how="intersection",
    )
    print(f"  {len(overlay)} intersection polygons")

    if len(overlay) == 0:
        print("WARNING: No spatial intersections found!")
        xwalk = _build_empty_crosswalk(tongue)
        xwalk.to_csv(output_csv, index=False)
        print(f"Wrote crosswalk: {output_csv}")
        return xwalk

    # Compute metrics
    overlay["intersection_area"] = overlay.geometry.area
    overlay["coverage_tongue"] = overlay["intersection_area"] / overlay["tongue_area"]
    overlay["coverage_sid"] = overlay["intersection_area"] / overlay["sid_area"]
    overlay["iou"] = overlay["intersection_area"] / (
        overlay["tongue_area"] + overlay["sid_area"] - overlay["intersection_area"]
    )

    # Centroid distance: need original centroids
    tongue_centroids = (
        tongue[["FID", "geometry"]].copy().set_geometry(tongue.geometry.centroid)
    )
    tongue_centroids = tongue_centroids.rename(columns={"geometry": "tongue_centroid"})

    sid_centroids = (
        sid_filtered[["SID_FID", "geometry"]]
        .copy()
        .set_geometry(sid_filtered.geometry.centroid)
    )
    sid_centroids = sid_centroids.rename(columns={"geometry": "sid_centroid"})

    overlay = overlay.merge(
        pd.DataFrame(
            {
                "FID": tongue_centroids["FID"],
                "tongue_centroid": tongue_centroids["tongue_centroid"],
            }
        ),
        on="FID",
    )
    overlay = overlay.merge(
        pd.DataFrame(
            {
                "SID_FID": sid_centroids["SID_FID"],
                "sid_centroid": sid_centroids["sid_centroid"],
            }
        ),
        on="SID_FID",
    )
    overlay["centroid_distance_m"] = overlay.apply(
        lambda r: r["tongue_centroid"].distance(r["sid_centroid"]), axis=1
    )

    # Select best SID per Tongue field (max coverage_tongue)
    best_idx = overlay.groupby("FID")["coverage_tongue"].idxmax()
    best = overlay.loc[best_idx].copy()

    # Flag matches
    best["match_flag"] = "accepted"
    flagged = (best["coverage_tongue"] < coverage_threshold) | (
        best["centroid_distance_m"] > distance_threshold
    )
    best.loc[flagged, "match_flag"] = "flagged"

    # Build crosswalk with all Tongue fields
    xwalk = tongue[["FID", "STATE"]].copy().rename(columns={"FID": "tongue_fid"})
    best_out = best[
        [
            "FID",
            "SID_FID",
            "COUNTY_NO",
            "coverage_tongue",
            "coverage_sid",
            "iou",
            "centroid_distance_m",
            "match_flag",
        ]
    ].rename(
        columns={"FID": "tongue_fid", "SID_FID": "sid_fid", "COUNTY_NO": "county_no"}
    )

    xwalk = xwalk.merge(best_out, on="tongue_fid", how="left")

    # Fill unmatched
    xwalk["match_flag"] = xwalk["match_flag"].fillna("no_match")
    xwalk["state"] = xwalk["STATE"]
    xwalk = xwalk.drop(columns=["STATE"])

    # Reorder columns
    xwalk = xwalk[
        [
            "tongue_fid",
            "sid_fid",
            "county_no",
            "state",
            "coverage_tongue",
            "coverage_sid",
            "iou",
            "centroid_distance_m",
            "match_flag",
        ]
    ]

    print_crosswalk_report(xwalk)

    xwalk.to_csv(output_csv, index=False)
    print(f"\nWrote crosswalk: {output_csv}")

    return xwalk


def _build_empty_crosswalk(tongue):
    """Build a crosswalk where every field is no_match."""
    return pd.DataFrame(
        {
            "tongue_fid": tongue["FID"],
            "sid_fid": pd.NA,
            "county_no": pd.NA,
            "state": tongue["STATE"],
            "coverage_tongue": pd.NA,
            "coverage_sid": pd.NA,
            "iou": pd.NA,
            "centroid_distance_m": pd.NA,
            "match_flag": "no_match",
        }
    )


def print_crosswalk_report(xwalk):
    """Print QC summary of crosswalk."""
    total = len(xwalk)
    accepted = (xwalk["match_flag"] == "accepted").sum()
    flagged = (xwalk["match_flag"] == "flagged").sum()
    no_match = (xwalk["match_flag"] == "no_match").sum()

    print(f"\n{'=' * 50}")
    print("Crosswalk QC Report")
    print(f"{'=' * 50}")
    print(f"Total Tongue fields:  {total}")
    print(f"  Accepted matches:   {accepted}")
    print(f"  Flagged matches:    {flagged}")
    print(f"  No match:           {no_match}")

    # No-match by state
    nm = xwalk[xwalk["match_flag"] == "no_match"]
    if len(nm) > 0:
        print("\nNo-match by state:")
        for state, count in nm["state"].value_counts().items():
            print(f"  {state}: {count}")

    # Flagged details
    if flagged > 0:
        fl = xwalk[xwalk["match_flag"] == "flagged"]
        low_cov = (fl["coverage_tongue"] < COVERAGE_THRESHOLD).sum()
        high_dist = (fl["centroid_distance_m"] > DISTANCE_THRESHOLD).sum()
        print("\nFlagged reasons:")
        print(f"  Low coverage (<{COVERAGE_THRESHOLD}): {low_cov}")
        print(f"  High centroid distance (>{DISTANCE_THRESHOLD}m): {high_dist}")

    # County distribution of accepted
    acc = xwalk[xwalk["match_flag"] == "accepted"]
    if len(acc) > 0:
        print("\nAccepted matches by county:")
        for county, count in acc["county_no"].value_counts().sort_index().items():
            print(f"  County {int(county):03d}: {count}")

    # Coverage stats for matched fields
    matched = xwalk[xwalk["match_flag"].isin(["accepted", "flagged"])]
    if len(matched) > 0:
        print("\nCoverage stats (matched fields):")
        for col in ["coverage_tongue", "iou"]:
            vals = matched[col].dropna()
            print(
                f"  {col}: min={vals.min():.4f}  "
                f"median={vals.median():.4f}  max={vals.max():.4f}"
            )


def main():
    parser = argparse.ArgumentParser(description="Build Tongue-SID spatial crosswalk")
    parser.add_argument(
        "--tongue-shp",
        type=str,
        default=str(TONGUE_SHP),
        help="Tongue shapefile (default: %(default)s)",
    )
    parser.add_argument(
        "--sid-shp",
        type=str,
        default=str(SID_SHP),
        help="SID shapefile (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(OUTPUT_CSV),
        help="Output CSV path (default: %(default)s)",
    )
    parser.add_argument(
        "--coverage-threshold",
        type=float,
        default=COVERAGE_THRESHOLD,
        help="Min coverage_tongue for accepted (default: %(default)s)",
    )
    parser.add_argument(
        "--distance-threshold",
        type=float,
        default=DISTANCE_THRESHOLD,
        help="Max centroid distance (m) for accepted (default: %(default)s)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load shapefiles and print info, then exit",
    )
    args = parser.parse_args()

    build_crosswalk(
        tongue_shp=args.tongue_shp,
        sid_shp=args.sid_shp,
        output_csv=args.output,
        coverage_threshold=args.coverage_threshold,
        distance_threshold=args.distance_threshold,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
