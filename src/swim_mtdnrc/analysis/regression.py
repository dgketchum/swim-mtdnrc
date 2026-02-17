"""Regression analysis: NDVI cluster characteristics vs. meteorology & streamflow.

Methods:
- Random forest for cluster prediction + feature importance
- Linear regression for continuous metrics vs met/streamflow
- Correlation matrices

Usage:
    python -m swim_mtdnrc.analysis.regression \
        --cluster-dir /nas/swim/examples/tongue_new/data/clustering \
        --met-dir /nas/swim/examples/tongue_new/data/met_timeseries \
        --streamflow /nas/swim/examples/tongue_new/data/streamflow/tongue_river_daily_discharge.csv \
        --k 8
"""

import argparse
import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression

GROWING_SEASON_MONTHS = list(range(4, 11))  # Apr-Oct


def load_gridmet_growing_season(met_dir, years):
    """Load GridMET parquet files and compute growing-season summaries.

    Parameters
    ----------
    met_dir : str
        Directory with GridMET parquet files (one per field).
    years : list[int]

    Returns
    -------
    pd.DataFrame
        Columns: year, gs_precip_mm, gs_mean_temp_c, gs_eto_mm, gs_gdd
    """
    # GridMET data is in parquet files per field, or one combined file.
    # Try combined first, then per-field.
    combined = os.path.join(met_dir, "gridmet_combined.parquet")
    if os.path.exists(combined):
        df = pd.read_parquet(combined)
    else:
        # Look for individual parquet files
        files = [f for f in os.listdir(met_dir) if f.endswith(".parquet")]
        if not files:
            print(f"  WARNING: No parquet files in {met_dir}")
            return pd.DataFrame()
        dfs = [pd.read_parquet(os.path.join(met_dir, f)) for f in files]
        df = pd.concat(dfs, ignore_index=True)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month
    elif df.index.name == "date" or isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index()
        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

    gs = df[df["month"].isin(GROWING_SEASON_MONTHS) & df["year"].isin(years)]

    # Identify available met columns (different naming conventions)
    precip_col = _find_col(gs, ["pr", "precip", "ppt", "prcp"])
    tmin_col = _find_col(gs, ["tmmn", "tmin"])
    tmax_col = _find_col(gs, ["tmmx", "tmax"])
    eto_col = _find_col(gs, ["eto", "etr", "pet"])

    result_rows = []
    for year in years:
        yr_data = gs[gs["year"] == year]
        if len(yr_data) == 0:
            continue

        row = {"year": year}

        if precip_col:
            row["gs_precip_mm"] = yr_data[precip_col].sum()
        if tmin_col and tmax_col:
            mean_temp = (yr_data[tmin_col] + yr_data[tmax_col]) / 2
            row["gs_mean_temp_c"] = mean_temp.mean()
            # GDD base 10C
            row["gs_gdd"] = np.maximum(mean_temp - 10, 0).sum()
        if eto_col:
            row["gs_eto_mm"] = yr_data[eto_col].sum()

        result_rows.append(row)

    return pd.DataFrame(result_rows)


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

    rows = []
    for year in years:
        yr_data = df[str(year)]
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
                    # Peak flow day of year
                    peak_idx = vals.idxmax()
                    row["peak_flow_doy"] = peak_idx.dayofyear
            rows.append(row)

    return pd.DataFrame(rows)


def build_feature_table(cluster_dir, k, met_dir=None, streamflow_path=None):
    """Build a feature table for regression: one row per field-year.

    Columns: fid, year, cluster, peak_ndvi, greenup_doy, season_length,
             + meteorological variables, + streamflow variables.
    """
    # Load cluster assignments
    assign_path = os.path.join(cluster_dir, f"tongue_ndvi_clusters_k{k}.json")
    with open(assign_path) as f:
        assignments = json.load(f)

    # Load phenology stats if available
    stats_path = os.path.join(cluster_dir, f"cluster_stats_k{k}.csv")
    if os.path.exists(stats_path):
        cluster_stats = pd.read_csv(stats_path)
    else:
        cluster_stats = None

    # Build base table from assignments
    rows = []
    for label, cid in assignments.items():
        parts = label.rsplit("_", 1)
        fid = parts[0]
        year = int(parts[1])
        rows.append({"fid": fid, "year": year, "cluster": int(cid)})

    df = pd.DataFrame(rows)
    years = sorted(df["year"].unique())

    # Merge phenology from cluster centroids
    if cluster_stats is not None:
        pheno_cols = [
            "cluster",
            "peak_ndvi",
            "greenup_doy",
            "senescence_doy",
            "season_length",
        ]
        available = [c for c in pheno_cols if c in cluster_stats.columns]
        df = df.merge(cluster_stats[available], on="cluster", how="left")

    # Merge meteorological data
    if met_dir and os.path.isdir(met_dir):
        met_df = load_gridmet_growing_season(met_dir, years)
        if len(met_df) > 0:
            df = df.merge(met_df, on="year", how="left")

    # Merge streamflow
    if streamflow_path and os.path.exists(streamflow_path):
        flow_df = load_streamflow_growing_season(streamflow_path, years)
        if len(flow_df) > 0:
            # Use first gage for simplicity; pivot if multiple
            first_gage = flow_df["gage"].iloc[0]
            flow_single = flow_df[flow_df["gage"] == first_gage].drop(columns=["gage"])
            df = df.merge(flow_single, on="year", how="left")

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
    exclude = ["fid"]
    numeric_cols = [c for c in numeric_cols if c not in exclude]

    if len(numeric_cols) < 3:
        print("  Not enough numeric columns for correlation matrix")
        return

    corr = df[numeric_cols].corr()

    fig, ax = plt.subplots(figsize=(12, 10))
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
        default="/nas/swim/examples/tongue_new/data/met_timeseries",
    )
    parser.add_argument(
        "--streamflow",
        type=str,
        default="/nas/swim/examples/tongue_new/data/streamflow/tongue_river_daily_discharge.csv",
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
