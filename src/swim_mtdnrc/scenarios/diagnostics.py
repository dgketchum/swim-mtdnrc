"""Diagnostic plots and reports for crop-type substitution scenarios.

Compares NDVI before and after substitution, and summarizes the phenological
changes introduced by the scenario.
"""

from __future__ import annotations

import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from swim_mtdnrc.clustering.crop_curves import compute_phenology
from swim_mtdnrc.clustering.crop_library import (
    DOY_START,
    FULL_YEAR_DAYS,
    GROWING_SEASON_DAYS,
)


def _extract_field_ndvi_climatology(container, field_idx, mask="irr"):
    """Extract per-DOY median NDVI for one field from a container.

    Returns
    -------
    np.ndarray, shape (366,)
    """
    path = f"derived/merged_ndvi/{mask}"
    arr = container._root[path]
    time_index = container._time_index
    doys = time_index.dayofyear.values

    field_ndvi = arr[:, field_idx]
    if hasattr(field_ndvi, "__array__"):
        field_ndvi = np.asarray(field_ndvi, dtype=np.float64)

    clim = np.full(FULL_YEAR_DAYS, np.nan)
    for doy in range(1, FULL_YEAR_DAYS + 1):
        mask_doy = doys == doy
        vals = field_ndvi[mask_doy]
        finite = vals[np.isfinite(vals)]
        if len(finite) > 0:
            clim[doy - 1] = np.nanmedian(finite)

    # Fill any remaining NaN with neighbors
    s = pd.Series(clim)
    s = s.interpolate(limit_direction="both")
    return s.values


def scenario_report(
    source_path,
    scenario_path,
    spec,
    library,
    output_dir,
    mask="irr",
):
    """Generate diagnostic plots and summary CSV for a scenario.

    Parameters
    ----------
    source_path : str
        Path to the source (baseline) container.
    scenario_path : str
        Path to the scenario container.
    spec : ScenarioSpec
        The scenario specification.
    library : dict
        Loaded crop library.
    output_dir : str
        Directory for output plots and CSV.
    mask : str
        NDVI mask to compare ('irr' or 'inv_irr').
    """
    from swimrs.container import SwimContainer

    os.makedirs(output_dir, exist_ok=True)

    source = SwimContainer.open(source_path, mode="r")
    scenario = SwimContainer.open(scenario_path, mode="r")

    try:
        # Build UID -> index mapping
        uids = list(source._root["geometry/uid"][:])
        if uids and isinstance(uids[0], bytes):
            uids = [u.decode("utf-8") for u in uids]
        uid_to_idx = {uid: i for i, uid in enumerate(uids)}

        # Collect per-field comparison data
        rows = []
        before_curves = {}
        after_curves = {}

        for sub in spec.substitutions:
            if sub.fid not in uid_to_idx:
                continue

            field_idx = uid_to_idx[sub.fid]

            before = _extract_field_ndvi_climatology(source, field_idx, mask)
            after = _extract_field_ndvi_climatology(scenario, field_idx, mask)

            before_curves[sub.fid] = before
            after_curves[sub.fid] = after

            # Growing-season slices for phenology
            before_gs = before[DOY_START - 1 : DOY_START - 1 + GROWING_SEASON_DAYS]
            after_gs = after[DOY_START - 1 : DOY_START - 1 + GROWING_SEASON_DAYS]

            pheno_before = compute_phenology(before_gs)
            pheno_after = compute_phenology(after_gs)

            rows.append(
                {
                    "fid": sub.fid,
                    "target_crop": sub.target_crop,
                    "before_peak_ndvi": pheno_before["peak_ndvi"],
                    "after_peak_ndvi": pheno_after["peak_ndvi"],
                    "peak_ndvi_change": pheno_after["peak_ndvi"]
                    - pheno_before["peak_ndvi"],
                    "before_peak_doy": pheno_before["peak_doy"],
                    "after_peak_doy": pheno_after["peak_doy"],
                    "before_greenup_doy": pheno_before["greenup_doy"],
                    "after_greenup_doy": pheno_after["greenup_doy"],
                    "before_season_length": pheno_before["season_length"],
                    "after_season_length": pheno_after["season_length"],
                }
            )

        # Write summary CSV
        summary_df = pd.DataFrame(rows)
        summary_path = os.path.join(output_dir, "scenario_summary.csv")
        summary_df.to_csv(summary_path, index=False)
        print(f"Summary written: {summary_path}")

        # Plot 1: Library curves overview
        _plot_library_curves(library, output_dir)

        # Plot 2: Before/after comparison (small multiples)
        if before_curves:
            _plot_before_after(before_curves, after_curves, spec, output_dir)

    finally:
        source.close()
        scenario.close()


def _plot_library_curves(library, output_dir):
    """Plot all crop library curves with confidence bands."""
    fig, ax = plt.subplots(figsize=(10, 5))
    doys = np.arange(1, FULL_YEAR_DAYS + 1)

    colors = plt.cm.tab10(np.linspace(0, 1, len(library)))

    for i, (crop_name, entry) in enumerate(sorted(library.items())):
        curve = np.array(entry["curve_366"])
        p25 = np.array(entry["p25_366"])
        p75 = np.array(entry["p75_366"])

        ax.plot(
            doys, curve, label=f"{crop_name} (n={entry['n_profiles']})", color=colors[i]
        )
        ax.fill_between(doys, p25, p75, alpha=0.15, color=colors[i])

    ax.set_xlabel("Day of Year")
    ax.set_ylabel("NDVI")
    ax.set_title("Crop Library Curves (median + 25th-75th percentile)")
    ax.legend(loc="upper right", fontsize=8)
    ax.set_xlim(1, 366)
    ax.set_ylim(0, 1)
    ax.axvline(
        DOY_START, color="gray", linestyle="--", alpha=0.3, label="Growing season"
    )
    ax.axvline(DOY_START + GROWING_SEASON_DAYS, color="gray", linestyle="--", alpha=0.3)

    fig.tight_layout()
    path = os.path.join(output_dir, "crop_library_curves.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"Library plot: {path}")


def _plot_before_after(before_curves, after_curves, spec, output_dir):
    """Plot before/after NDVI for substituted fields (small multiples)."""
    fids = list(before_curves.keys())
    n = len(fids)
    ncols = min(4, n)
    nrows = (n + ncols - 1) // ncols

    fig, axes = plt.subplots(
        nrows, ncols, figsize=(3.5 * ncols, 2.5 * nrows), squeeze=False
    )
    doys = np.arange(1, FULL_YEAR_DAYS + 1)

    # Build FID -> crop lookup
    fid_to_crop = {s.fid: s.target_crop for s in spec.substitutions}

    for i, fid in enumerate(fids):
        row, col = divmod(i, ncols)
        ax = axes[row, col]
        ax.plot(
            doys, before_curves[fid], label="before", color="steelblue", linewidth=0.8
        )
        ax.plot(
            doys, after_curves[fid], label="after", color="orangered", linewidth=0.8
        )
        ax.set_title(f"FID {fid} → {fid_to_crop.get(fid, '?')}", fontsize=8)
        ax.set_xlim(60, 330)
        ax.set_ylim(0, 0.9)
        ax.tick_params(labelsize=6)
        if i == 0:
            ax.legend(fontsize=6)

    # Hide unused axes
    for i in range(n, nrows * ncols):
        row, col = divmod(i, ncols)
        axes[row, col].set_visible(False)

    fig.suptitle(f"Scenario: {spec.name}", fontsize=10)
    fig.tight_layout()
    path = os.path.join(output_dir, "scenario_before_after.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"Before/after plot: {path}")
