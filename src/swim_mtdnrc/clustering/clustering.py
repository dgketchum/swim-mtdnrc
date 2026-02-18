"""NDVI time series clustering for Tongue River Basin irrigated fields.

Loads merged NDVI daily tables (1987-2021, up to 2084 fields), extracts
growing-season profiles (Apr 1 - Oct 31), interpolates to regular daily grid,
and runs k-means clustering.

Usage:
    python -m swim_mtdnrc.clustering.clustering \
        --ndvi-dir /nas/swim/examples/tongue_new/data/landsat/extracts/ndvi/irr \
        --k 6,8,10,12 --output-dir /nas/swim/examples/tongue_new/data/clustering
"""

import argparse
import json
import os
import re

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

FEATURE_ID = "FID"
GROWING_SEASON_START = 4  # April
GROWING_SEASON_END = 10  # October (inclusive)
MIN_SCENES = 5  # minimum Landsat scenes in growing season to keep a field-year
GROWING_SEASON_DAYS = 214  # Apr 1 - Oct 31


def _parse_scene_date(scene_id):
    """Extract date from Landsat scene ID like 'LE07_035028_20200107'.

    Returns pd.Timestamp or None if unparseable.
    """
    match = re.search(r"(\d{8})$", scene_id)
    if match:
        return pd.Timestamp(match.group(1))
    return None


def load_year_ndvi(csv_path):
    """Load a single year's NDVI CSV and return (fid_array, dates, values).

    Parameters
    ----------
    csv_path : str
        Path to ndvi_YYYY_irr.csv

    Returns
    -------
    fids : np.ndarray of int, shape (n_fields,)
    dates : list of pd.Timestamp, length n_scenes
    values : np.ndarray, shape (n_fields, n_scenes), NaN for missing
    """
    df = pd.read_csv(csv_path)
    fids = df[FEATURE_ID].values

    scene_cols = [c for c in df.columns if c != FEATURE_ID]
    dates = []
    valid_cols = []
    for col in scene_cols:
        dt = _parse_scene_date(col)
        if dt is not None:
            dates.append(dt)
            valid_cols.append(col)

    # Sort by date
    order = np.argsort(dates)
    dates = [dates[i] for i in order]
    valid_cols = [valid_cols[i] for i in order]

    values = df[valid_cols].values.astype(float)
    # Replace 0s with NaN (0 NDVI = masked/no data in EE extracts)
    values[values == 0] = np.nan

    return fids, dates, values


def extract_growing_season_profiles(ndvi_dir, years=None, min_scenes=MIN_SCENES):
    """Load all years, extract growing season profiles, interpolate to daily.

    Parameters
    ----------
    ndvi_dir : str
        Directory containing ndvi_YYYY_irr.csv files.
    years : list[int] or None
        Years to process. Auto-detected from filenames if None.
    min_scenes : int
        Minimum growing-season Landsat scenes to include a field-year.

    Returns
    -------
    profiles : np.ndarray, shape (n_valid, GROWING_SEASON_DAYS)
        Interpolated daily NDVI profiles (Apr 1 - Oct 31).
    labels : list of str
        Field-year labels like "123_2015".
    """
    if years is None:
        files = os.listdir(ndvi_dir)
        years = sorted(
            int(re.search(r"(\d{4})", f).group(1))
            for f in files
            if re.search(r"ndvi_(\d{4})_", f)
        )

    all_profiles = []
    all_labels = []

    for year in years:
        fname = [f for f in os.listdir(ndvi_dir) if f"_{year}_" in f]
        if not fname:
            print(f"  SKIP {year}: no file found")
            continue
        csv_path = os.path.join(ndvi_dir, fname[0])

        fids, dates, values = load_year_ndvi(csv_path)

        # Filter to growing season
        gs_mask = [GROWING_SEASON_START <= d.month <= GROWING_SEASON_END for d in dates]
        gs_dates = [d for d, m in zip(dates, gs_mask) if m]
        gs_values = values[:, gs_mask]

        if len(gs_dates) == 0:
            print(f"  SKIP {year}: no growing season scenes")
            continue

        # Convert scene dates to day-of-year offsets relative to Apr 1
        apr1 = pd.Timestamp(f"{year}-04-01")
        scene_doys = np.array([(d - apr1).days for d in gs_dates], dtype=float)
        target_doys = np.arange(GROWING_SEASON_DAYS, dtype=float)

        for i, fid in enumerate(fids):
            row = gs_values[i]
            valid = ~np.isnan(row)
            n_valid = valid.sum()

            if n_valid < min_scenes:
                continue

            # Interpolate to daily
            interp = np.interp(
                target_doys,
                scene_doys[valid],
                row[valid],
                left=np.nan,
                right=np.nan,
            )

            # Fill leading/trailing NaN with nearest valid
            first_valid = np.where(~np.isnan(interp))[0]
            if len(first_valid) == 0:
                continue
            interp[: first_valid[0]] = interp[first_valid[0]]
            last_valid = first_valid[-1]
            interp[last_valid + 1 :] = interp[last_valid]

            all_profiles.append(interp)
            all_labels.append(f"{fid}_{year}")

        print(
            f"  {year}: {sum(1 for lb in all_labels if lb.endswith(f'_{year}'))} valid field-years "
            f"from {len(fids)} fields, {len(gs_dates)} scenes"
        )

    profiles = np.array(all_profiles)
    print(f"\nTotal: {len(all_profiles)} field-year profiles, shape {profiles.shape}")

    return profiles, all_labels


def run_kmeans(profiles, k_values, random_state=42):
    """Run k-means for multiple k values and evaluate silhouette scores.

    Parameters
    ----------
    profiles : np.ndarray, shape (n_samples, n_features)
    k_values : list of int

    Returns
    -------
    results : dict
        {k: {'model': KMeans, 'silhouette': float, 'inertia': float, 'labels': np.ndarray}}
    """
    # Standardize profiles for clustering
    scaler = StandardScaler()
    profiles_scaled = scaler.fit_transform(profiles)

    results = {}

    for k in k_values:
        print(f"\n  k={k}:")
        km = KMeans(n_clusters=k, random_state=random_state, n_init=10, max_iter=300)
        labels = km.fit_predict(profiles_scaled)

        sil = silhouette_score(
            profiles_scaled, labels, sample_size=min(10000, len(labels))
        )
        inertia = km.inertia_

        # Cluster sizes
        unique, counts = np.unique(labels, return_counts=True)
        sizes = dict(zip(unique.tolist(), counts.tolist()))

        print(f"    silhouette={sil:.4f}, inertia={inertia:.0f}")
        print(f"    cluster sizes: {sizes}")

        # Transform centroids back to original scale
        centroids = scaler.inverse_transform(km.cluster_centers_)

        results[k] = {
            "model": km,
            "scaler": scaler,
            "silhouette": sil,
            "inertia": inertia,
            "labels": labels,
            "centroids": centroids,
        }

    return results


def save_results(results, labels_list, output_dir, best_k=None):
    """Save clustering results to JSON files.

    Parameters
    ----------
    results : dict from run_kmeans
    labels_list : list of str (field-year labels)
    output_dir : str
    best_k : int or None
        If None, selects k with highest silhouette score.
    """
    os.makedirs(output_dir, exist_ok=True)

    if best_k is None:
        best_k = max(results, key=lambda k: results[k]["silhouette"])

    # Summary CSV
    summary_rows = []
    for k, r in sorted(results.items()):
        unique, counts = np.unique(r["labels"], return_counts=True)
        summary_rows.append(
            {
                "k": k,
                "silhouette": r["silhouette"],
                "inertia": r["inertia"],
                "min_cluster_size": int(counts.min()),
                "max_cluster_size": int(counts.max()),
                "best": k == best_k,
            }
        )
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(
        os.path.join(output_dir, "tongue_cluster_summary.csv"), index=False
    )
    print("\nSummary -> tongue_cluster_summary.csv")

    for k, r in results.items():
        # Centroids
        centroid_path = os.path.join(output_dir, f"tongue_ndvi_centroids_k{k}.json")
        centroids_dict = {f"cluster_{i}": r["centroids"][i].tolist() for i in range(k)}
        with open(centroid_path, "w") as f:
            json.dump(centroids_dict, f, indent=2)

        # Cluster assignments
        assign_path = os.path.join(output_dir, f"tongue_ndvi_clusters_k{k}.json")
        assignments = {
            label: int(cluster_id)
            for label, cluster_id in zip(labels_list, r["labels"])
        }
        with open(assign_path, "w") as f:
            json.dump(assignments, f, indent=2)

        tag = " *BEST*" if k == best_k else ""
        print(f"  k={k}{tag}: centroids -> {centroid_path}")
        print(f"           assignments -> {assign_path}")

    return best_k


def main():
    parser = argparse.ArgumentParser(
        description="NDVI time series clustering for Tongue River Basin"
    )
    parser.add_argument(
        "--ndvi-dir",
        type=str,
        default="/nas/swim/examples/tongue_new/data/landsat/extracts/ndvi/irr",
        help="Directory with merged NDVI CSVs",
    )
    parser.add_argument(
        "--k",
        type=str,
        default="6,8,10,12",
        help="Comma-separated k values for k-means",
    )
    parser.add_argument(
        "--min-scenes",
        type=int,
        default=MIN_SCENES,
        help="Minimum growing-season scenes per field-year",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/nas/swim/examples/tongue_new/data/clustering",
        help="Output directory for results",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Comma-separated years to include (default: all available)",
    )
    args = parser.parse_args()

    k_values = [int(k) for k in args.k.split(",")]
    years = [int(y) for y in args.years.split(",")] if args.years else None

    print("=== Loading NDVI profiles ===")
    profiles, labels = extract_growing_season_profiles(
        args.ndvi_dir, years=years, min_scenes=args.min_scenes
    )

    if len(profiles) == 0:
        print("ERROR: No valid profiles found")
        return

    print("\n=== Running k-means clustering ===")
    results = run_kmeans(profiles, k_values)

    print("\n=== Saving results ===")
    best_k = save_results(results, labels, args.output_dir)
    print(f"\nBest k={best_k} (highest silhouette)")


if __name__ == "__main__":
    main()
