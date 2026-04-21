"""Build and load a crop-type NDVI curve library from CDL-labeled field-year profiles.

The library maps each crop group to a representative 366-day NDVI curve (per-DOY
median) built from historical Landsat profiles filtered by CDL crop classification.
These curves can be used to substitute NDVI in scenario containers without
recalibrating the model.

Usage:
    python -m swim_mtdnrc.clustering.crop_library \
        --ndvi-dir /nas/swim/examples/tongue/data/landsat/extracts/ndvi/irr \
        --cdl-csv /nas/swim/examples/tongue/data/landsat/extracts/cdl/cdl_crop_type_2008_2024.csv \
        --output /nas/swim/examples/tongue/data/crop_library/tongue_crop_library.json
"""

import argparse
import json
import os

import numpy as np
from scipy.ndimage import gaussian_filter1d

from swim_mtdnrc.clustering.cdl_crosstab import CDL_NAMES, load_cdl
from swim_mtdnrc.clustering.clustering import extract_growing_season_profiles
from swim_mtdnrc.clustering.crop_curves import compute_phenology

GROWING_SEASON_DAYS = 214
DOY_START = 91  # Apr 1
DOY_END = 304  # Oct 31
FULL_YEAR_DAYS = 366

DEFAULT_CROP_GROUPS = {
    "alfalfa": [36],
    "grass_pasture": [171, 176],
    "other_hay": [37],
    "corn": [1],
    "small_grains": [21, 23, 24, 25, 27, 28, 29],
}

MIN_PROFILES_DEFAULT = 30


def _extend_to_full_year(profile_214, smooth_window=7):
    """Extend a 214-day growing-season profile to a 366-day full year.

    The growing season (Apr 1 – Oct 31) occupies DOY 91-304.  Winter
    days are padded with the boundary values (Apr 1 value for Jan-Mar,
    Oct 31 value for Nov-Dec) and a Gaussian smooth is applied at the
    two seams to avoid discontinuities.

    Parameters
    ----------
    profile_214 : np.ndarray, shape (214,)
        Daily NDVI values for the growing season.
    smooth_window : int
        Gaussian sigma (days) for transition smoothing.

    Returns
    -------
    np.ndarray, shape (366,)
        Full-year daily NDVI profile indexed by DOY (1-based DOY maps to
        0-based index).
    """
    full = np.empty(FULL_YEAR_DAYS, dtype=np.float64)

    start_val = profile_214[0]
    end_val = profile_214[-1]

    # DOY 1-90 (index 0-89): winter/early spring — pad with Apr 1 value
    full[: DOY_START - 1] = start_val
    # DOY 91-304 (index 90-303): growing season
    full[DOY_START - 1 : DOY_END] = profile_214
    # DOY 305-366 (index 304-365): late fall/winter — pad with Oct 31 value
    full[DOY_END:] = end_val

    # Smooth the two seam regions to remove discontinuities
    if smooth_window > 0:
        smoothed = gaussian_filter1d(full, sigma=smooth_window)
        # Only apply smoothing at the seam neighborhoods, keep the
        # growing-season core and winter plateaus intact
        seam_width = smooth_window * 3
        # Spring seam: around DOY 91 (index 90)
        s1_lo = max(0, DOY_START - 1 - seam_width)
        s1_hi = min(FULL_YEAR_DAYS, DOY_START - 1 + seam_width)
        full[s1_lo:s1_hi] = smoothed[s1_lo:s1_hi]
        # Fall seam: around DOY 304 (index 303)
        s2_lo = max(0, DOY_END - seam_width)
        s2_hi = min(FULL_YEAR_DAYS, DOY_END + seam_width)
        full[s2_lo:s2_hi] = smoothed[s2_lo:s2_hi]

    return full


def _filter_profiles_by_cdl(profiles, labels, cdl_long, cdl_codes):
    """Filter interpolated NDVI profiles to those whose CDL label matches codes.

    Parameters
    ----------
    profiles : np.ndarray, shape (n, 214)
        Interpolated growing-season profiles.
    labels : list of str
        Field-year labels like "123_2015".
    cdl_long : pd.DataFrame
        Long-format CDL table with columns [fid_year, FID, year, crop_code].
    cdl_codes : list of int
        CDL codes to match.

    Returns
    -------
    np.ndarray, shape (m, 214)
        Filtered profiles where the field-year's CDL code is in ``cdl_codes``.
    """
    cdl_set = set(cdl_codes)
    matching_fid_years = set(
        cdl_long.loc[cdl_long["crop_code"].isin(cdl_set), "fid_year"]
    )

    mask = np.array([label in matching_fid_years for label in labels])
    return profiles[mask]


def build_crop_library(
    ndvi_dir,
    cdl_csv,
    output_path,
    crop_groups=None,
    min_profiles=MIN_PROFILES_DEFAULT,
    years=None,
    min_scenes=3,
    smooth_window=7,
):
    """Build per-crop-type representative NDVI curves from CDL-labeled profiles.

    Parameters
    ----------
    ndvi_dir : str
        Directory containing annual ndvi_YYYY_irr.csv files.
    cdl_csv : str
        Path to CDL crop type CSV (columns: FID, crop_YYYY, ...).
    output_path : str
        Path to write the JSON library file.
    crop_groups : dict or None
        Mapping of crop name to list of CDL codes.  Defaults to
        ``DEFAULT_CROP_GROUPS``.
    min_profiles : int
        Minimum field-year profiles required to include a crop group.
    years : list[int] or None
        Years to process; auto-detected if None.
    min_scenes : int
        Minimum Landsat scenes per field-year for profile inclusion.
    smooth_window : int
        Gaussian sigma for full-year extension smoothing.

    Returns
    -------
    dict
        Library mapping crop name to curve data, phenology, and metadata.
    """
    if crop_groups is None:
        crop_groups = DEFAULT_CROP_GROUPS

    print("Loading growing-season profiles...")
    profiles, labels = extract_growing_season_profiles(
        ndvi_dir, years=years, min_scenes=min_scenes
    )

    print("Loading CDL data...")
    cdl_long = load_cdl(cdl_csv)

    library = {}

    for crop_name, cdl_codes in crop_groups.items():
        filtered = _filter_profiles_by_cdl(profiles, labels, cdl_long, cdl_codes)
        n = len(filtered)

        code_names = [CDL_NAMES.get(c, f"CDL_{c}") for c in cdl_codes]
        print(f"\n{crop_name}: {n} field-year profiles ({', '.join(code_names)})")

        if n < min_profiles:
            print(f"  SKIP: below minimum threshold ({min_profiles})")
            continue

        # Compute growing-season statistics
        median_214 = np.nanmedian(filtered, axis=0)
        p25_214 = np.nanpercentile(filtered, 25, axis=0)
        p75_214 = np.nanpercentile(filtered, 75, axis=0)

        # Extend to full year
        median_366 = _extend_to_full_year(median_214, smooth_window=smooth_window)
        p25_366 = _extend_to_full_year(p25_214, smooth_window=smooth_window)
        p75_366 = _extend_to_full_year(p75_214, smooth_window=smooth_window)

        # Phenology from the growing-season median
        phenology = compute_phenology(median_214)

        library[crop_name] = {
            "curve_366": median_366.tolist(),
            "p25_366": p25_366.tolist(),
            "p75_366": p75_366.tolist(),
            "n_profiles": n,
            "cdl_codes": cdl_codes,
            "phenology": {
                k: (float(v) if not np.isnan(v) else None) for k, v in phenology.items()
            },
        }
        print(
            f"  peak_ndvi={phenology['peak_ndvi']:.3f}  "
            f"peak_doy={phenology['peak_doy']}  "
            f"greenup_doy={phenology.get('greenup_doy', 'N/A')}"
        )

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(library, f, indent=2)
        print(f"\nLibrary written: {output_path} ({len(library)} crop groups)")

    return library


def load_crop_library(path):
    """Load a saved crop curve library JSON file.

    Parameters
    ----------
    path : str
        Path to the JSON library file.

    Returns
    -------
    dict
        Library mapping crop name to curve data.  The ``curve_366``,
        ``p25_366``, and ``p75_366`` values are converted to numpy arrays.
    """
    with open(path) as f:
        raw = json.load(f)

    library = {}
    for crop_name, entry in raw.items():
        library[crop_name] = {
            "curve_366": np.array(entry["curve_366"], dtype=np.float64),
            "p25_366": np.array(entry["p25_366"], dtype=np.float64),
            "p75_366": np.array(entry["p75_366"], dtype=np.float64),
            "n_profiles": entry["n_profiles"],
            "cdl_codes": entry["cdl_codes"],
            "phenology": entry["phenology"],
        }

    return library


def main():
    parser = argparse.ArgumentParser(
        description="Build crop-type NDVI curve library from CDL-labeled profiles"
    )
    parser.add_argument(
        "--ndvi-dir",
        required=True,
        help="Directory with annual ndvi_YYYY_irr.csv files",
    )
    parser.add_argument(
        "--cdl-csv",
        required=True,
        help="CDL crop type CSV (FID, crop_YYYY columns)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output JSON path for crop library",
    )
    parser.add_argument(
        "--min-profiles",
        type=int,
        default=MIN_PROFILES_DEFAULT,
        help="Minimum field-year profiles per crop group",
    )
    parser.add_argument(
        "--min-scenes",
        type=int,
        default=3,
        help="Minimum Landsat scenes per field-year",
    )
    args = parser.parse_args()

    build_crop_library(
        ndvi_dir=args.ndvi_dir,
        cdl_csv=args.cdl_csv,
        output_path=args.output,
        min_profiles=args.min_profiles,
        min_scenes=args.min_scenes,
    )


if __name__ == "__main__":
    main()
