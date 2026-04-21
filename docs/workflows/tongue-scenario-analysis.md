# Tongue Scenario Analysis

## Purpose

This workflow substitutes crop-type NDVI curves on selected fields to simulate
alternative cropping patterns without recalibrating the model.  It answers
questions like "what would basin water demand look like if 50 alfalfa fields
switched to corn?"

## When To Use This Workflow

Use this workflow when you need to:

- simulate water demand under a different crop mix
- compare irrigation requirements between crop types
- evaluate the ET impact of land-use change at the field scale

## Prerequisites

- A completed calibration and hindcast container (see
  [Tongue Calibration](tongue-calibration.md))
- A crop curve library built from historical NDVI and CDL data (built once,
  reused across scenarios)

## Primary Entry Points

| Entry point | Purpose |
|-------------|---------|
| `uv run python /home/dgketchum/code/swim-mtdnrc/scripts/run_build_library.py` | build the crop curve library from CDL-labeled NDVI profiles |
| `uv run python /home/dgketchum/code/swim-mtdnrc/scripts/run_scenario.py` | build a scenario container and generate diagnostics |

## Workflow Shape

### 1. Build the crop curve library (one-time)

The library answers: "what does a typical NDVI season look like for each crop
type in the Tongue basin?"  It is built once from historical data and reused
across any number of scenarios.

The build process works in three stages:

1. **Load profiles.** Every irrigated field in every year (1987-2024) has a
   214-day growing-season NDVI time series interpolated from Landsat scenes.
   The build loads all of these — 55,451 field-year profiles for the Tongue
   basin.

2. **Filter by CDL crop label.** The USDA Cropland Data Layer classifies each
   field's crop type annually (2008-2024).  For each crop group (e.g., all
   CDL code 36 = alfalfa), the matching field-year profiles are pulled out.
   This gives crop-specific subsets: 8,489 alfalfa profiles, 507 corn
   profiles, etc.

3. **Compute representative curves.** For each crop group, the per-DOY median
   across all matching profiles becomes the representative curve.  The
   214-day growing season (Apr 1 - Oct 31) is extended to a full 366-day year
   by padding winter months with the boundary values and smoothing the seams
   with a Gaussian filter.  Percentile bands (25th-75th) capture the
   year-to-year spread.

```
uv run python scripts/run_build_library.py \
    --ndvi-dir /nas/swim/examples/tongue/data/landsat/extracts/ndvi/irr \
    --cdl-csv /nas/swim/examples/tongue/data/landsat/extracts/cdl/cdl_crop_type_2008_2024.csv \
    --output /nas/swim/examples/tongue/data/crop_library/tongue_crop_library.json
```

The output is a JSON file with one entry per crop group.  The Tongue build
produced five groups from 55,451 profiles:

| Crop Group | Profiles | Peak NDVI | Peak DOY | Greenup DOY |
|------------|----------|-----------|----------|-------------|
| alfalfa | 8,489 | 0.637 | 156 (Jun 5) | 105 (Apr 15) |
| grass_pasture | 5,805 | 0.680 | 173 (Jun 22) | 99 (Apr 9) |
| other_hay | 4,421 | 0.709 | 165 (Jun 14) | 94 (Apr 4) |
| corn | 507 | 0.718 | 225 (Aug 13) | 172 (Jun 21) |
| small_grains | 413 | 0.614 | 179 (Jun 28) | 143 (May 23) |

The phenology reflects real agronomic differences: corn has the latest greenup
and peak (warm-season annual), alfalfa is early with a moderate peak (perennial,
multiple cuttings flatten the curve), and small grains peak in late June then
senesce quickly.

### 2. Write a scenario specification

Create a TOML file describing which fields to change and what crop to assign:

```toml
[scenario]
name = "corn_expansion"
description = "Convert 50 alfalfa fields in the lower basin to corn"
crop_library = "/nas/swim/examples/tongue/data/crop_library/tongue_crop_library.json"
source_container = "/nas/swim/examples/tongue_ensemble/data/tongue_hindcast.swim"

[[scenario.substitutions]]
fids = [101, 102, 103, 104, 105]
crop = "corn"

[[scenario.substitutions]]
fids = [200, 201]
crop = "small_grains"
```

Alternatively, use a two-column CSV (`FID,crop`) for bulk substitutions.

### 3. Build the scenario container

The scenario container is a copy of the hindcast with targeted fields' NDVI
replaced.  The builder:

1. **Creates a fresh container** with the same fields, date range, and
   coordinate reference as the source hindcast.
2. **Copies all data** — meteorology, soil properties, calibration parameters,
   and baseline NDVI — from the source.  Simulation runs and restart state
   from the source are intentionally excluded so the model starts fresh.
3. **Overwrites NDVI** for each substituted field.  The crop library's 366-day
   curve is tiled across the container's full time axis by day-of-year,
   replacing both the irrigated and non-irrigated NDVI masks.
4. **Recomputes irrigation windows** (`irr_data`) from the modified NDVI using
   slope-based detection, so the model sees irrigation timing consistent with
   the new crop's phenology.

```
uv run python scripts/run_scenario.py \
    --scenario /path/to/scenario.toml \
    --output /nas/swim/examples/tongue_ensemble/data/tongue_corn_expansion.swim \
    --report-dir /nas/swim/examples/tongue_ensemble/reports/corn_expansion/
```

The result is a standard `.swim` container that works with all existing
`swim run` options — no special flags needed.

### 4. Review the diagnostics report

The report directory contains:

- `scenario_summary.csv` — per-field phenology changes (peak NDVI, greenup, season length)
- `crop_library_curves.png` — all library curves overlaid with confidence bands
- `scenario_before_after.png` — before/after NDVI for each substituted field

Review these before running the model to verify the substitutions are reasonable.

### 5. Run the model

```
swim run tongue_ensemble.toml \
    --container /nas/swim/examples/tongue_ensemble/data/tongue_corn_expansion.swim \
    --run-id corn_expansion --profile core --ndvi-mode observed
```

The scenario container is a regular SWIM container.  All existing `swim run`
options work unchanged.

### 6. Compare outputs to baseline

Compare the scenario run's `eta`, `irr_sim`, and other outputs against the
baseline hindcast to quantify the impact of the crop change.

## Crop Library Reference

The Tongue River library contains five crop groups built from 55,451 field-year
NDVI profiles (1987-2024) filtered by CDL crop labels (2008-2024):

| Crop Group | CDL Codes | Profiles | Peak NDVI | Season |
|------------|-----------|----------|-----------|--------|
| `alfalfa` | 36 | 8,489 | 0.637 | Apr 15 - Oct, multiple cuts |
| `grass_pasture` | 171, 176 | 5,805 | 0.680 | Apr 9 - Oct, single broad peak |
| `other_hay` | 37 | 4,421 | 0.709 | Apr 4 - Oct |
| `corn` | 1 | 507 | 0.718 | Jun 21 - Oct, late sharp peak Aug 13 |
| `small_grains` | 21, 23, 24, 25, 27, 28, 29 | 413 | 0.614 | May 23 - Sep, early peak Jun 28 |

## Main Outputs

| Output | Role |
|--------|------|
| crop library JSON | per-crop 366-day NDVI curves with percentile bands |
| scenario container (.swim) | model-ready container with modified NDVI |
| diagnostics report | before/after comparison plots and phenology summary |
| model outputs | daily ET, irrigation, and water balance under the scenario |

## Relationship To Calibration

Scenarios reuse calibrated parameters.  The sigmoid NDVI-to-Kcb transform
(ndvi_k, ndvi_0) was fitted during calibration and stays fixed — only the NDVI
input changes.  This means the model applies the same physical relationships
to the new crop's phenology without needing to recalibrate.

## Caveats

- Crop curves are basin-specific, derived from Tongue River CDL and NDVI history.
- The small-grains curve aggregates barley, wheat, and oats due to limited
  individual sample sizes in the basin.
- Substituting a crop with very different water demand (e.g., replacing
  grass/pasture with corn) may push the model into parameter ranges that were
  not well explored during calibration.  Review diagnostics before trusting
  results from large phenological shifts.
- Both the `irr` and `inv_irr` NDVI masks are overwritten with the same crop
  curve.  This represents "this field grows crop X under irrigation" uniformly.
