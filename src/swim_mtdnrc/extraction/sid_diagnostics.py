"""SID data diagnostics — observation frequency, missingness, and irrigation detection QC.

Standalone script (no SwimContainer dependency) that reads a SID netCDF and
produces console stats + plotly figures for one or more fields.

Usage:
    python scripts/sid_diagnostics.py --county 099 --field 099_001434 \
        [--root /nas/swim/sid] [--out-dir ~/Downloads/figures/sid_qc]
"""

import argparse
import os
import re
from glob import glob

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import xarray as xr
from plotly.subplots import make_subplots


def read_irr_csv(csv_path, feature_id="FID"):
    """Read IrrMapper CSV and return DataFrame with fid index, year columns."""
    df = pd.read_csv(csv_path)
    df.index = df[feature_id]
    irr_cols = sorted([c for c in df.columns if re.match(r"irr_\d{4}$", c)])
    return df[irr_cols]


def load_data(root, county):
    """Load netCDF and IrrMapper properties for a county.

    Returns (xr.Dataset, pd.DataFrame or None).
    """
    nc_path = os.path.join(root, f"{county}_remote_sensing_ndvi.nc")
    if not os.path.exists(nc_path):
        raise FileNotFoundError(f"NetCDF not found: {nc_path}")
    ds = xr.open_dataset(nc_path)

    irr_files = glob(os.path.join(root, county, "properties", "irr_*.csv"))
    irr_df = read_irr_csv(irr_files[0]) if irr_files else None

    return ds, irr_df


def print_stats(ds, irr_df, field):
    """Print per-year observation counts, coverage, and irrigation stats."""
    ct = ds["ndvi_irr_ct"].sel(fid=field).values.astype(bool)
    dates = pd.to_datetime(ds["date"].values)

    irr_days = None
    if "irr_days" in ds:
        irr_days = ds["irr_days"].sel(fid=field).values.astype(bool)

    print(f"\n{'=' * 60}")
    print(f"  Field: {field}")
    print(f"  Date range: {dates[0].date()} to {dates[-1].date()}")
    print(f"  Total days: {len(dates)}")
    print(f"  Total observations: {ct.sum()}")
    print(f"  Overall coverage: {ct.sum() / len(dates) * 100:.1f}%")
    print(f"{'=' * 60}")

    print(f"\n{'Year':>6}  {'Obs':>5}  {'Irr Days':>9}  {'IrrMapper Prob':>15}")
    print(f"{'----':>6}  {'---':>5}  {'--------':>9}  {'--------------':>15}")

    years = sorted(set(dates.year))
    for yr in years:
        yr_mask = dates.year == yr
        yr_obs = ct[yr_mask].sum()

        yr_irr = ""
        if irr_days is not None:
            yr_irr = str(irr_days[yr_mask].sum())

        irr_prob = ""
        if irr_df is not None:
            col = f"irr_{yr}"
            if col in irr_df.columns and field in irr_df.index:
                irr_prob = f"{irr_df.at[field, col]:.2f}"

        print(f"{yr:>6}  {yr_obs:>5}  {yr_irr:>9}  {irr_prob:>15}")

    print()


def coverage_figure(ds, irr_df, field, out_dir=None):
    """Multi-year bar chart of observation counts with irrigation day overlay."""
    ct = ds["ndvi_irr_ct"].sel(fid=field).values.astype(bool)
    dates = pd.to_datetime(ds["date"].values)
    years = sorted(set(dates.year))

    obs_counts = []
    irr_counts = []
    has_irr = "irr_days" in ds

    if has_irr:
        irr_vals = ds["irr_days"].sel(fid=field).values.astype(bool)

    for yr in years:
        yr_mask = dates.year == yr
        obs_counts.append(int(ct[yr_mask].sum()))
        if has_irr:
            irr_counts.append(int(irr_vals[yr_mask].sum()))

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(
            x=years,
            y=obs_counts,
            name="Landsat Observations",
            marker_color="steelblue",
            opacity=0.7,
        ),
        secondary_y=False,
    )

    if has_irr and any(c > 0 for c in irr_counts):
        fig.add_trace(
            go.Scatter(
                x=years,
                y=irr_counts,
                mode="lines+markers",
                name="Irrigation Days",
                line=dict(color="firebrick", width=2),
                marker=dict(size=5),
            ),
            secondary_y=True,
        )

    fig.update_layout(
        title=f"{field} — Observation Density & Irrigation Activity",
        xaxis_title="Year",
        legend=dict(x=0.01, y=0.99),
        template="plotly_white",
        width=1200,
        height=500,
    )
    fig.update_yaxes(title_text="Observations / Year", secondary_y=False)
    fig.update_yaxes(title_text="Irrigation Days / Year", secondary_y=True)

    _save_or_show(fig, out_dir, f"{field}_coverage.png")


def yearly_figures(ds, irr_df, field, out_dir=None):
    """Per-year NDVI timeseries with raw observations and irrigation markers."""
    ndvi = ds["ndvi_irr"].sel(fid=field).values.astype(float)
    ct = ds["ndvi_irr_ct"].sel(fid=field).values.astype(bool)
    dates = pd.to_datetime(ds["date"].values)

    has_irr = "irr_days" in ds
    if has_irr:
        irr_vals = ds["irr_days"].sel(fid=field).values.astype(bool)

    years = sorted(set(dates.year))

    for yr in years:
        yr_mask = dates.year == yr
        yr_dates = dates[yr_mask]
        yr_ndvi = ndvi[yr_mask]
        yr_ct = ct[yr_mask]

        if yr_ct.sum() == 0:
            continue

        # 10-day rolling mean (matches detect_cuttings_nc smoothing)
        ndvi_series = pd.Series(yr_ndvi, index=yr_dates)
        ndvi_smooth = ndvi_series.rolling(window=10, center=True).mean()

        # Raw observation dates/values
        obs_dates = yr_dates[yr_ct]
        obs_values = yr_ndvi[yr_ct]

        # IrrMapper probability for title
        irr_prob_str = ""
        if irr_df is not None:
            col = f"irr_{yr}"
            if col in irr_df.columns and field in irr_df.index:
                irr_prob_str = f" | IrrMapper P={irr_df.at[field, col]:.2f}"

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=yr_dates,
                y=ndvi_smooth,
                mode="lines",
                name="NDVI (10-day mean)",
                line=dict(color="green", width=2),
            )
        )

        fig.add_trace(
            go.Scatter(
                x=obs_dates,
                y=obs_values,
                mode="markers",
                name="Raw Observations",
                marker=dict(size=6, color="darkgreen", symbol="circle"),
            )
        )

        if has_irr:
            yr_irr = irr_vals[yr_mask]
            irr_dates = yr_dates[yr_irr]
            if len(irr_dates) > 0:
                irr_ndvi = ndvi_smooth.reindex(irr_dates)
                fig.add_trace(
                    go.Scatter(
                        x=irr_dates,
                        y=irr_ndvi,
                        mode="markers",
                        name="Irrigation Days",
                        marker=dict(
                            size=10,
                            color="blue",
                            symbol="circle-open",
                            line=dict(width=2),
                        ),
                    )
                )

        fig.update_layout(
            title=f"{field} — {yr}{irr_prob_str}",
            xaxis_title="Date",
            yaxis_title="NDVI",
            template="plotly_white",
            width=1200,
            height=500,
            legend=dict(x=0.01, y=0.99),
        )

        _save_or_show(fig, out_dir, f"{field}_{yr}.png")


def capture_histogram(ds, county, out_dir=None):
    """Histogram of mean annual Landsat captures per field for a county.

    For each field, computes the observation count per year then averages
    across years.  Stacks irrigated and non-irrigated (inv_irr) captures.
    Vertical lines mark the 5th and 95th percentiles of total captures.
    """
    dates = pd.to_datetime(ds["date"].values)
    fids = ds["fid"].values
    years = sorted(set(dates.year))
    n_years = len(years)

    masks = [("ndvi_irr_ct", "Irrigated", "steelblue")]
    if "ndvi_inv_irr_ct" in ds:
        masks.append(("ndvi_inv_irr_ct", "Non-Irrigated", "goldenrod"))

    mean_by_var = {}
    for var, label, _ in masks:
        ct = ds[var].values.astype(bool)
        annual = np.zeros((len(fids), n_years))
        for i, yr in enumerate(years):
            yr_mask = dates.year == yr
            annual[:, i] = ct[yr_mask, :].sum(axis=0)
        mean_by_var[label] = annual.mean(axis=1)

    total = sum(mean_by_var.values())
    p5 = np.percentile(total, 5)
    p95 = np.percentile(total, 95)
    median = np.median(total)

    print(f"\n=== County {county} capture histogram ===")
    print(f"  Fields: {len(fids)}")
    print(f"  Years:  {years[0]}-{years[-1]} ({n_years} yrs)")
    for label, vals in mean_by_var.items():
        print(
            f"  {label} — min: {vals.min():.1f}, max: {vals.max():.1f}, "
            f"mean: {vals.mean():.1f}"
        )
    print(
        f"  Total — min: {total.min():.1f}, max: {total.max():.1f}, "
        f"mean: {total.mean():.1f}"
    )
    print(f"  5th pctl: {p5:.1f}, median: {median:.1f}, 95th pctl: {p95:.1f}")

    # Bin fields by total captures, split each bar by irr/inv_irr proportion
    bin_edges = np.linspace(total.min(), total.max(), 41)
    bin_idx = np.clip(np.digitize(total, bin_edges) - 1, 0, len(bin_edges) - 2)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    n_bins = len(bin_centers)

    bar_data = {}
    for _, label, _ in masks:
        bar_data[label] = np.zeros(n_bins)

    for b in range(n_bins):
        bmask = bin_idx == b
        n = bmask.sum()
        if n == 0:
            continue
        bin_total = total[bmask].sum()
        for _, label, _ in masks:
            bar_data[label][b] = n * mean_by_var[label][bmask].sum() / bin_total

    fig = go.Figure()

    for _, label, color in masks:
        fig.add_trace(
            go.Bar(
                x=bin_centers,
                y=bar_data[label],
                marker_color=color,
                opacity=0.8,
                name=label,
                width=bin_edges[1] - bin_edges[0],
            )
        )

    for pval, plabel, color in [
        (p5, "5th", "firebrick"),
        (p95, "95th", "firebrick"),
        (median, "Median", "black"),
    ]:
        fig.add_vline(
            x=pval,
            line_dash="dash",
            line_color=color,
            line_width=2,
            annotation_text=f"{plabel}: {pval:.1f}",
            annotation_position="top",
        )

    fig.update_layout(
        title=f"County {county} — Mean Annual Landsat Captures per Field "
        f"(n={len(fids)}, {years[0]}-{years[-1]})",
        xaxis_title="Mean Annual Captures",
        yaxis_title="Number of Fields",
        barmode="stack",
        template="plotly_white",
        width=900,
        height=500,
    )

    _save_or_show(fig, out_dir, f"{county}_capture_histogram.png")


def _save_or_show(fig, out_dir, filename):
    """Write figure to PNG or show interactively."""
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, filename)
        fig.write_image(path)
        print(f"  Wrote {path}")
    else:
        fig.show()


def main():
    parser = argparse.ArgumentParser(
        description="SID data diagnostics — observation frequency, missingness, irrigation QC"
    )
    parser.add_argument(
        "--county",
        required=True,
        help="County code(s), comma-separated (e.g. 099 or 001,003,099)",
    )
    parser.add_argument(
        "--field",
        default=None,
        help="Field ID (e.g. 099_001434). Comma-separated for multiple.",
    )
    parser.add_argument("--root", default="/nas/swim/sid", help="SID root directory")
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory for figures (default: show interactively)",
    )
    parser.add_argument(
        "--histogram",
        action="store_true",
        help="Plot histogram of mean annual captures per field for each county",
    )
    args = parser.parse_args()

    if not args.histogram and not args.field:
        parser.error("--field is required unless --histogram is used")

    out_dir = os.path.expanduser(args.out_dir) if args.out_dir else None
    counties = [c.strip() for c in args.county.split(",")]

    for county in counties:
        try:
            ds, irr_df = load_data(args.root, county)
        except FileNotFoundError as e:
            print(f"WARNING: {e}, skipping county {county}")
            continue

        if args.histogram:
            capture_histogram(ds, county, out_dir=out_dir)

        if args.field:
            fields = [f.strip() for f in args.field.split(",")]
            for field in fields:
                if field not in ds["fid"].values:
                    print(f"WARNING: field {field} not found in dataset, skipping")
                    continue
                print_stats(ds, irr_df, field)
                coverage_figure(ds, irr_df, field, out_dir=out_dir)
                yearly_figures(ds, irr_df, field, out_dir=out_dir)

        ds.close()


if __name__ == "__main__":
    main()

# ========================= EOF ====================================================================
