"""Representative crop curves and phenological metrics from NDVI clusters.

Computes median NDVI profiles per cluster, confidence bands (25th-75th percentile),
phenological metrics (green-up, peak, senescence), and temporal stability analysis.

Usage:
    python -m swim_mtdnrc.clustering.crop_curves \
        --cluster-dir /nas/swim/examples/tongue_new/data/clustering \
        --ndvi-dir /nas/swim/examples/tongue_new/data/landsat/merged/ndvi/irr \
        --k 8
"""

import argparse
import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from collections import Counter

GROWING_SEASON_DAYS = 214  # Apr 1 - Oct 31
GREENUP_THRESHOLD = 0.3  # NDVI threshold for green-up detection
SENESCENCE_THRESHOLD = 0.3  # NDVI threshold for senescence


def load_cluster_data(cluster_dir, k):
    """Load centroids and assignments for a given k.

    Returns
    -------
    centroids : dict
        {cluster_id: np.ndarray of shape (GROWING_SEASON_DAYS,)}
    assignments : dict
        {field_year: cluster_id}
    """
    centroid_path = os.path.join(cluster_dir, f"tongue_ndvi_centroids_k{k}.json")
    assign_path = os.path.join(cluster_dir, f"tongue_ndvi_clusters_k{k}.json")

    with open(centroid_path) as f:
        raw = json.load(f)
    centroids = {int(key.split("_")[1]): np.array(vals) for key, vals in raw.items()}

    with open(assign_path) as f:
        assignments = json.load(f)
    # Convert values to int
    assignments = {k: int(v) for k, v in assignments.items()}

    return centroids, assignments


def load_profiles_for_cluster(ndvi_dir, assignments, cluster_id, years=None):
    """Reload raw interpolated profiles for a specific cluster.

    Re-runs the interpolation from clustering.py for the field-years
    assigned to this cluster.

    Returns
    -------
    profiles : np.ndarray, shape (n_members, GROWING_SEASON_DAYS)
    """
    from swim_mtdnrc.clustering.clustering import extract_growing_season_profiles

    all_profiles, all_labels = extract_growing_season_profiles(
        ndvi_dir, years=years, min_scenes=3
    )

    # Filter to this cluster
    mask = np.array([assignments.get(label, -1) == cluster_id for label in all_labels])
    return all_profiles[mask]


def compute_phenology(profile, doy_offset=91):
    """Extract phenological metrics from a daily NDVI profile.

    Parameters
    ----------
    profile : np.ndarray, shape (GROWING_SEASON_DAYS,)
        Daily NDVI values Apr 1 - Oct 31.
    doy_offset : int
        Day-of-year of Apr 1 (91 in non-leap years).

    Returns
    -------
    dict with keys:
        greenup_doy : int or NaN
        peak_ndvi : float
        peak_doy : int
        senescence_doy : int or NaN
        season_length : int or NaN
    """
    peak_idx = np.nanargmax(profile)
    peak_ndvi = profile[peak_idx]
    peak_doy = peak_idx + doy_offset

    # Green-up: first day NDVI exceeds threshold before peak
    greenup_doy = np.nan
    for i in range(peak_idx):
        if profile[i] >= GREENUP_THRESHOLD:
            greenup_doy = i + doy_offset
            break

    # Senescence: first day NDVI drops below threshold after peak
    senescence_doy = np.nan
    for i in range(peak_idx, len(profile)):
        if profile[i] < SENESCENCE_THRESHOLD:
            senescence_doy = i + doy_offset
            break

    season_length = np.nan
    if not np.isnan(greenup_doy) and not np.isnan(senescence_doy):
        season_length = senescence_doy - greenup_doy

    return {
        "greenup_doy": greenup_doy,
        "peak_ndvi": peak_ndvi,
        "peak_doy": peak_doy,
        "senescence_doy": senescence_doy,
        "season_length": season_length,
    }


def compute_cluster_stats(centroids, assignments, ndvi_dir=None):
    """Compute per-cluster statistics and phenological metrics.

    Parameters
    ----------
    centroids : dict from load_cluster_data
    assignments : dict from load_cluster_data
    ndvi_dir : str or None
        If provided, reloads profiles for percentile computation.

    Returns
    -------
    pd.DataFrame with one row per cluster.
    """
    rows = []

    for cid in sorted(centroids.keys()):
        profile = centroids[cid]
        pheno = compute_phenology(profile)

        # Count members
        n_members = sum(1 for v in assignments.values() if v == cid)

        # Unique fields (strip year from label)
        fields_in_cluster = set()
        for label, v in assignments.items():
            if v == cid:
                fid = label.rsplit("_", 1)[0]
                fields_in_cluster.add(fid)

        row = {
            "cluster": cid,
            "n_field_years": n_members,
            "n_unique_fields": len(fields_in_cluster),
            "mean_ndvi": float(np.nanmean(profile)),
            **pheno,
        }
        rows.append(row)

    return pd.DataFrame(rows)


def temporal_stability(assignments):
    """Analyze per-field cluster membership stability across years.

    Returns
    -------
    pd.DataFrame with columns: fid, n_years, dominant_cluster, dominant_pct, stable
    """
    # Parse field-year labels
    field_clusters = {}
    for label, cid in assignments.items():
        parts = label.rsplit("_", 1)
        fid = parts[0]
        if fid not in field_clusters:
            field_clusters[fid] = []
        field_clusters[fid].append(cid)

    rows = []
    for fid, clusters in field_clusters.items():
        counter = Counter(clusters)
        dominant = counter.most_common(1)[0]
        n_years = len(clusters)
        dominant_pct = dominant[1] / n_years

        rows.append(
            {
                "fid": fid,
                "n_years": n_years,
                "dominant_cluster": dominant[0],
                "dominant_pct": round(dominant_pct, 3),
                "stable": dominant_pct >= 0.8,
            }
        )

    return pd.DataFrame(rows).sort_values("fid")


def plot_crop_curves(centroids, output_dir, k):
    """Plot centroid NDVI curves — one per cluster + composite overlay.

    Saves per-cluster and composite plots to output_dir.
    """
    os.makedirs(output_dir, exist_ok=True)

    days = pd.date_range("2020-04-01", periods=GROWING_SEASON_DAYS, freq="D")
    colors = plt.cm.tab10(np.linspace(0, 1, len(centroids)))

    # Composite plot
    fig, ax = plt.subplots(figsize=(12, 6))
    for i, (cid, profile) in enumerate(sorted(centroids.items())):
        ax.plot(days, profile, color=colors[i], linewidth=2, label=f"Cluster {cid}")

    ax.set_xlabel("Date")
    ax.set_ylabel("NDVI")
    ax.set_title(f"Tongue River Basin — NDVI Crop Curves (k={k})")
    ax.legend(loc="upper right")
    ax.set_ylim(0, 1)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    composite_path = os.path.join(output_dir, f"crop_curves_composite_k{k}.png")
    fig.savefig(composite_path, dpi=150)
    plt.close(fig)
    print(f"  Composite -> {composite_path}")

    # Individual cluster plots
    for cid, profile in sorted(centroids.items()):
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(days, profile, color="green", linewidth=2)
        ax.fill_between(days, profile * 0.85, profile * 1.15, alpha=0.2, color="green")
        ax.set_xlabel("Date")
        ax.set_ylabel("NDVI")
        ax.set_title(f"Cluster {cid} — Median Crop Curve")
        ax.set_ylim(0, 1)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()

        path = os.path.join(output_dir, f"crop_curve_cluster_{cid}_k{k}.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)

    print(f"  Individual cluster plots -> {output_dir}/crop_curve_cluster_*_k{k}.png")


def main():
    parser = argparse.ArgumentParser(
        description="Compute crop curves and phenological metrics from NDVI clusters"
    )
    parser.add_argument(
        "--cluster-dir",
        type=str,
        default="/nas/swim/examples/tongue_new/data/clustering",
        help="Directory with clustering results",
    )
    parser.add_argument(
        "--ndvi-dir",
        type=str,
        default="/nas/swim/examples/tongue_new/data/landsat/merged/ndvi/irr",
        help="Directory with merged NDVI CSVs (for percentile computation)",
    )
    parser.add_argument(
        "--k",
        type=int,
        required=True,
        help="Number of clusters to analyze",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: cluster_dir)",
    )
    args = parser.parse_args()

    output_dir = args.output_dir or args.cluster_dir

    print(f"=== Loading cluster data (k={args.k}) ===")
    centroids, assignments = load_cluster_data(args.cluster_dir, args.k)
    print(f"  {len(centroids)} clusters, {len(assignments)} field-year assignments")

    print("\n=== Cluster statistics ===")
    stats = compute_cluster_stats(centroids, assignments)
    stats_path = os.path.join(output_dir, f"cluster_stats_k{args.k}.csv")
    stats.to_csv(stats_path, index=False)
    print(stats.to_string(index=False))
    print(f"\n  -> {stats_path}")

    print("\n=== Temporal stability ===")
    stability = temporal_stability(assignments)
    stability_path = os.path.join(output_dir, f"field_stability_k{args.k}.csv")
    stability.to_csv(stability_path, index=False)
    n_stable = stability["stable"].sum()
    n_total = len(stability)
    print(f"  {n_stable}/{n_total} fields stable (>80% same cluster)")
    print(f"  -> {stability_path}")

    print("\n=== Plotting crop curves ===")
    plot_crop_curves(centroids, output_dir, args.k)

    print("\nDone.")


if __name__ == "__main__":
    main()
