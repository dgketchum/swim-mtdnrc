"""Regression analysis: NDVI cluster characteristics vs. meteorology & streamflow.

Methods:
- Random forest for cluster prediction + feature importance
- Linear regression for continuous metrics vs met/streamflow
- Correlation matrices

Usage:
    python -m swim_mtdnrc.analysis.regression \
        --cluster-dir /nas/swim/examples/tongue_new/data/clustering \
        --met-dir /nas/swim/examples/tongue_new/data/met_timeseries/gridmet \
        --streamflow /nas/swim/examples/tongue_new/data/streamflow/tongue_river_daily_discharge.csv \
        --k 8
"""

import argparse
import json
import os
import re

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score

GROWING_SEASON_MONTHS = list(range(4, 11))  # Apr-Oct


def compute_field_phenology(ndvi_dir, years=None):
    """Compute per-field phenology from raw NDVI profiles.

    Parameters
    ----------
    ndvi_dir : str
        Directory containing ndvi_YYYY_irr.csv files.
    years : list[int] or None

    Returns
    -------
    pd.DataFrame
        Columns: fid, year, peak_ndvi, greenup_doy, senescence_doy, season_length
    """
    from swim_mtdnrc.clustering.clustering import extract_growing_season_profiles
    from swim_mtdnrc.clustering.crop_curves import compute_phenology

    profiles, labels = extract_growing_season_profiles(
        ndvi_dir, years=years, min_scenes=3
    )

    rows = []
    for i, label in enumerate(labels):
        parts = label.rsplit("_", 1)
        fid = int(parts[0])
        year = int(parts[1])
        pheno = compute_phenology(profiles[i])
        rows.append(
            {
                "fid": fid,
                "year": year,
                "peak_ndvi": pheno["peak_ndvi"],
                "greenup_doy": pheno["greenup_doy"],
                "senescence_doy": pheno["senescence_doy"],
                "season_length": pheno["season_length"],
            }
        )

    df = pd.DataFrame(rows)
    print(f"  Field phenology: {len(df)} rows, {df['fid'].nunique()} unique fields")
    return df


def load_gridmet_growing_season(met_dir, years):
    """Load per-GFID GridMET parquets and compute growing-season summaries.

    Parameters
    ----------
    met_dir : str
        Directory with gridmet_historical_{GFID}.parquet files.
    years : list[int]

    Returns
    -------
    pd.DataFrame
        Columns: gfid, year, gs_precip_mm, gs_mean_temp_c, gs_eto_mm, gs_gdd
    """
    files = [f for f in os.listdir(met_dir) if f.endswith(".parquet")]
    if not files:
        print(f"  WARNING: No parquet files in {met_dir}")
        return pd.DataFrame()

    years_set = set(years)
    result_rows = []

    for fname in files:
        # Extract GFID from filename like gridmet_historical_100247.parquet
        match = re.search(r"gridmet_historical_(\d+)\.parquet", fname)
        if not match:
            continue
        gfid = int(match.group(1))

        df = pd.read_parquet(os.path.join(met_dir, fname))
        if df.index.name == "date" or isinstance(df.index, pd.DatetimeIndex):
            df = df.reset_index()

        if "year" not in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df["year"] = df["date"].dt.year
        if "month" not in df.columns:
            df["month"] = (
                df["date"].dt.month
                if "date" in df.columns
                else pd.to_datetime(df.index).month
            )

        gs = df[df["month"].isin(GROWING_SEASON_MONTHS) & df["year"].isin(years_set)]

        precip_col = _find_col(gs, ["prcp_mm", "pr", "precip", "ppt", "prcp"])
        tmin_col = _find_col(gs, ["tmin_c", "tmmn", "tmin"])
        tmax_col = _find_col(gs, ["tmax_c", "tmmx", "tmax"])
        eto_col = _find_col(gs, ["eto_mm", "eto", "etr", "pet"])

        for year, yr_data in gs.groupby("year"):
            if len(yr_data) == 0:
                continue

            row = {"gfid": gfid, "year": int(year)}

            if precip_col:
                row["gs_precip_mm"] = yr_data[precip_col].sum()
            if tmin_col and tmax_col:
                mean_temp = (yr_data[tmin_col] + yr_data[tmax_col]) / 2
                row["gs_mean_temp_c"] = mean_temp.mean()
                row["gs_gdd"] = np.maximum(mean_temp - 10, 0).sum()
            if eto_col:
                row["gs_eto_mm"] = yr_data[eto_col].sum()

            result_rows.append(row)

    met_df = pd.DataFrame(result_rows)
    print(f"  GridMET: {len(met_df)} rows, {met_df['gfid'].nunique()} unique GFIDs")
    return met_df


def _find_col(df, candidates):
    """Find first matching column name (case-insensitive)."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def load_streamflow_growing_season(streamflow_path, years):
    """Load streamflow CSV and compute growing-season summaries.

    Returns
    -------
    pd.DataFrame
        Columns: year, gs_mean_discharge_cfs, gs_total_discharge_cfs, peak_flow_doy
    """
    df = pd.read_csv(streamflow_path, index_col=0, parse_dates=True)
    df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)

    rows = []
    for year in years:
        yr_data = df[df.index.year == year]
        gs_data = yr_data[
            (yr_data.index.month >= GROWING_SEASON_MONTHS[0])
            & (yr_data.index.month <= GROWING_SEASON_MONTHS[-1])
        ]

        for col in df.columns:
            row = {"year": year, "gage": col}
            if len(gs_data) > 0 and col in gs_data.columns:
                vals = gs_data[col].dropna()
                if len(vals) > 0:
                    row["gs_mean_discharge_cfs"] = vals.mean()
                    row["gs_total_discharge_cfs"] = vals.sum()
                    peak_idx = vals.idxmax()
                    row["peak_flow_doy"] = peak_idx.dayofyear
            rows.append(row)

    return pd.DataFrame(rows)


def build_feature_table(
    cluster_dir,
    k,
    met_dir=None,
    streamflow_path=None,
    ndvi_dir=None,
    shapefile_path=None,
):
    """Build a feature table for regression: one row per field-year.

    Columns: fid, year, cluster, peak_ndvi, greenup_doy, season_length,
             + per-GFID meteorological variables, + streamflow variables.
    """
    # Load cluster assignments
    assign_path = os.path.join(cluster_dir, f"tongue_ndvi_clusters_k{k}.json")
    with open(assign_path) as f:
        assignments = json.load(f)

    # Build base table from assignments
    rows = []
    for label, cid in assignments.items():
        parts = label.rsplit("_", 1)
        fid = int(parts[0])
        year = int(parts[1])
        rows.append({"fid": fid, "year": year, "cluster": int(cid)})

    df = pd.DataFrame(rows)
    years = sorted(df["year"].unique())

    # Merge field-level phenology from raw NDVI profiles
    if ndvi_dir and os.path.isdir(ndvi_dir):
        print("  Computing field-level phenology...")
        pheno_df = compute_field_phenology(ndvi_dir, years=years)
        df = df.merge(pheno_df, on=["fid", "year"], how="left")
    else:
        print("  WARNING: No ndvi_dir provided, skipping field-level phenology")

    # Load FID -> GFID mapping from shapefile
    fid_to_gfid = None
    if shapefile_path and os.path.exists(shapefile_path):
        gdf = gpd.read_file(shapefile_path, engine="fiona")
        fid_to_gfid = dict(zip(gdf["FID"].astype(int), gdf["GFID"].astype(int)))
        df["gfid"] = df["fid"].map(fid_to_gfid)
        n_mapped = df["gfid"].notna().sum()
        print(f"  FID->GFID mapping: {n_mapped}/{len(df)} rows mapped")

    # Merge per-GFID meteorological data
    if met_dir and os.path.isdir(met_dir) and fid_to_gfid is not None:
        met_df = load_gridmet_growing_season(met_dir, years)
        if len(met_df) > 0:
            df = df.merge(met_df, on=["gfid", "year"], how="left")

    # Merge streamflow (basin-wide — merge on year only)
    if streamflow_path and os.path.exists(streamflow_path):
        flow_df = load_streamflow_growing_season(streamflow_path, years)
        if len(flow_df) > 0:
            for gage_name in flow_df["gage"].unique():
                gage_df = flow_df[flow_df["gage"] == gage_name].drop(columns=["gage"])
                suffix = gage_name.split("_", 1)[-1] if "_" in gage_name else gage_name
                gage_df = gage_df.rename(
                    columns={c: f"{c}_{suffix}" for c in gage_df.columns if c != "year"}
                )
                df = df.merge(gage_df, on="year", how="left")

    # Add trend variable
    df["year_trend"] = df["year"] - df["year"].min()

    return df


def run_cluster_prediction(df, k):
    """Random forest to predict cluster assignment from met + streamflow features.

    Returns feature importance ranking.
    """
    feature_cols = [
        c
        for c in df.columns
        if c
        not in [
            "fid",
            "year",
            "cluster",
            "gfid",
            "peak_ndvi",
            "greenup_doy",
            "senescence_doy",
            "season_length",
        ]
    ]
    feature_cols = [c for c in feature_cols if df[c].notna().sum() > len(df) * 0.5]

    if not feature_cols:
        print("  No features available for cluster prediction")
        return None

    X = df[feature_cols].fillna(df[feature_cols].median())
    y = df["cluster"]

    rf = RandomForestClassifier(n_estimators=200, random_state=42, n_jobs=-1)
    scores = cross_val_score(rf, X, y, cv=5, scoring="accuracy")
    rf.fit(X, y)

    importance = pd.DataFrame(
        {
            "feature": feature_cols,
            "importance": rf.feature_importances_,
        }
    ).sort_values("importance", ascending=False)

    print(f"\n  Random Forest cluster prediction (k={k}):")
    print(f"    5-fold CV accuracy: {scores.mean():.3f} +/- {scores.std():.3f}")
    print("    Feature importance:")
    for _, row in importance.iterrows():
        print(f"      {row['feature']:30s} {row['importance']:.4f}")

    return importance


def run_continuous_regressions(df, output_dir):
    """Linear regression for continuous NDVI metrics vs met/streamflow.

    Targets: peak_ndvi, greenup_doy, season_length
    """
    targets = ["peak_ndvi", "greenup_doy", "season_length"]
    available_targets = [
        t for t in targets if t in df.columns and df[t].notna().sum() > 10
    ]

    predictor_cols = [
        c
        for c in df.columns
        if c
        not in [
            "fid",
            "year",
            "cluster",
            "gfid",
            "peak_ndvi",
            "greenup_doy",
            "senescence_doy",
            "season_length",
        ]
    ]
    predictor_cols = [c for c in predictor_cols if df[c].notna().sum() > len(df) * 0.5]

    if not predictor_cols or not available_targets:
        print("  Insufficient data for continuous regressions")
        return

    os.makedirs(output_dir, exist_ok=True)

    for target in available_targets:
        valid = df[target].notna()
        X = df.loc[valid, predictor_cols].fillna(df[predictor_cols].median())
        y = df.loc[valid, target]

        if len(y) < 20:
            continue

        # Random forest regression
        rf = RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1)
        scores = cross_val_score(rf, X, y, cv=5, scoring="r2")
        rf.fit(X, y)

        print(f"\n  {target} regression:")
        print(f"    RF R2 (5-fold CV): {scores.mean():.3f} +/- {scores.std():.3f}")

        # Simple linear regression for each predictor
        for pred in predictor_cols:
            x_single = X[[pred]].values
            lr = LinearRegression().fit(x_single, y)
            r2 = lr.score(x_single, y)
            if abs(r2) > 0.05:
                print(f"    {pred:30s} linear R2={r2:.3f}, slope={lr.coef_[0]:.4f}")


def plot_correlation_matrix(df, output_dir, k):
    """Plot correlation matrix of numeric features."""
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    exclude = ["fid", "gfid"]
    numeric_cols = [c for c in numeric_cols if c not in exclude]

    if len(numeric_cols) < 3:
        print("  Not enough numeric columns for correlation matrix")
        return

    corr = df[numeric_cols].corr()

    fig = plt.figure(figsize=(12, 10))
    ax = fig.add_subplot(1, 1, 1)
    im = ax.imshow(corr, cmap="RdBu_r", vmin=-1, vmax=1, aspect="auto")
    ax.set_xticks(range(len(numeric_cols)))
    ax.set_yticks(range(len(numeric_cols)))
    ax.set_xticklabels(numeric_cols, rotation=45, ha="right", fontsize=8)
    ax.set_yticklabels(numeric_cols, fontsize=8)
    fig.colorbar(im, ax=ax, shrink=0.8)
    ax.set_title(f"Feature Correlation Matrix (k={k})")
    fig.tight_layout()

    path = os.path.join(output_dir, f"correlation_matrix_k{k}.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"\n  Correlation matrix -> {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Regression analysis: NDVI clusters vs met/streamflow"
    )
    parser.add_argument(
        "--cluster-dir",
        type=str,
        default="/nas/swim/examples/tongue_new/data/clustering",
    )
    parser.add_argument("--k", type=int, required=True)
    parser.add_argument(
        "--met-dir",
        type=str,
        default="/nas/swim/examples/tongue_new/data/met_timeseries/gridmet",
    )
    parser.add_argument(
        "--streamflow",
        type=str,
        default="/nas/swim/examples/tongue_new/data/streamflow/tongue_river_daily_discharge.csv",
    )
    parser.add_argument(
        "--ndvi-dir",
        type=str,
        default="/nas/swim/examples/tongue_new/data/landsat/extracts/ndvi/irr",
    )
    parser.add_argument(
        "--shapefile",
        type=str,
        default="/nas/swim/examples/tongue_new/data/gis/tongue_fields_gfid.shp",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/nas/swim/examples/tongue_new/data/clustering",
    )
    args = parser.parse_args()

    print(f"=== Building feature table (k={args.k}) ===")
    df = build_feature_table(
        args.cluster_dir,
        args.k,
        met_dir=args.met_dir,
        streamflow_path=args.streamflow,
        ndvi_dir=args.ndvi_dir,
        shapefile_path=args.shapefile,
    )
    print(f"  {len(df)} rows, {len(df.columns)} columns")
    print(f"  Columns: {list(df.columns)}")

    # Save feature table
    table_path = os.path.join(args.output_dir, f"feature_table_k{args.k}.csv")
    df.to_csv(table_path, index=False)
    print(f"  -> {table_path}")

    print("\n=== Cluster prediction (Random Forest) ===")
    importance = run_cluster_prediction(df, args.k)
    if importance is not None:
        imp_path = os.path.join(args.output_dir, f"feature_importance_k{args.k}.csv")
        importance.to_csv(imp_path, index=False)

    print("\n=== Continuous regressions ===")
    run_continuous_regressions(df, args.output_dir)

    print("\n=== Correlation matrix ===")
    plot_correlation_matrix(df, args.output_dir, args.k)

    print("\nDone.")


if __name__ == "__main__":
    main()
