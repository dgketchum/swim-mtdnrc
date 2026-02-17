"""Download USGS Tongue River daily streamflow using dataretrieval.

Gages:
    06308500 — Tongue River at Miles City, MT
    06306300 — Tongue River at Tongue River Dam, MT
    06307500 — Tongue River at Birney Day, MT

Usage:
    python -m swim_mtdnrc.analysis.streamflow
"""

import argparse
import os

import pandas as pd

try:
    import dataretrieval.nwis as nwis
except ImportError:
    raise ImportError("dataretrieval is required: uv add dataretrieval")

GAGES = {
    "06308500": "Miles City",
    "06306300": "Tongue River Dam",
    "06307500": "Birney",
}

PARAM_CODE = "00060"  # daily mean discharge (cfs)
OUTPUT_DIR = "/nas/swim/examples/tongue_new/data/streamflow"
DEFAULT_START = "1987-01-01"
DEFAULT_END = "2024-12-31"


def download_discharge(sites=None, start_date=None, end_date=None):
    """Download daily mean discharge for Tongue River gages.

    Parameters
    ----------
    sites : dict or None
        {site_no: name} mapping. Defaults to GAGES.
    start_date : str
        Start date (YYYY-MM-DD). Defaults to 1987-01-01.
    end_date : str
        End date (YYYY-MM-DD). Defaults to 2024-12-31.

    Returns
    -------
    pd.DataFrame
        DataFrame with DatetimeIndex and one column per gage (discharge in cfs).
    """
    if sites is None:
        sites = GAGES
    if start_date is None:
        start_date = DEFAULT_START
    if end_date is None:
        end_date = DEFAULT_END

    site_list = list(sites.keys())

    print(f"Downloading discharge for {len(site_list)} gages: {site_list}")
    print(f"  Period: {start_date} to {end_date}")
    print(f"  Parameter: {PARAM_CODE} (daily mean discharge, cfs)")

    df_all, _ = nwis.get_dv(
        sites=site_list,
        parameterCd=PARAM_CODE,
        start=start_date,
        end=end_date,
    )

    # dataretrieval returns MultiIndex (datetime, site_no) or flat depending on version.
    # Normalize to DatetimeIndex with columns named by site.
    if isinstance(df_all.index, pd.MultiIndex):
        # Pivot from long format
        df_all = df_all.reset_index()
        date_col = [c for c in df_all.columns if "date" in c.lower()][0]
        site_col = [c for c in df_all.columns if "site" in c.lower()][0]
        val_col = [c for c in df_all.columns if "00060" in c and "mean" in c.lower()]
        if not val_col:
            val_col = [c for c in df_all.columns if "00060" in c]
        val_col = val_col[0]

        result = df_all.pivot(index=date_col, columns=site_col, values=val_col)
        result.index = pd.to_datetime(result.index)
        result.index.name = "date"
    else:
        # Single-site or already flat
        result = df_all.copy()
        result.index = pd.to_datetime(result.index)
        result.index.name = "date"
        # Rename columns to site numbers
        discharge_cols = [c for c in result.columns if "00060" in c]
        if len(discharge_cols) == 1 and len(site_list) == 1:
            result = result[discharge_cols].rename(
                columns={discharge_cols[0]: site_list[0]}
            )
        else:
            result = result[discharge_cols]

    # Rename columns to include gage name
    rename_map = {}
    for col in result.columns:
        site_no = str(col).strip()
        if site_no in sites:
            rename_map[col] = f"{site_no}_{sites[site_no].replace(' ', '_')}"
    if rename_map:
        result = result.rename(columns=rename_map)

    for col in result.columns:
        n_obs = result[col].notna().sum()
        n_total = len(result)
        print(f"  {col}: {n_obs}/{n_total} days ({n_obs / n_total * 100:.1f}%)")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Download Tongue River daily streamflow from USGS"
    )
    parser.add_argument(
        "--start-date", type=str, default=DEFAULT_START, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=str, default=DEFAULT_END, help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--output-dir", type=str, default=OUTPUT_DIR, help="Output directory"
    )
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    df = download_discharge(start_date=args.start_date, end_date=args.end_date)

    out_path = os.path.join(args.output_dir, "tongue_river_daily_discharge.csv")
    df.to_csv(out_path)
    print(f"\nWrote {len(df)} days to {out_path}")


if __name__ == "__main__":
    main()
