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

`clustering.crop_library` loads historical growing-season NDVI profiles, filters
them by CDL crop classification, computes per-crop-type median curves (with
25th-75th percentile bands), and extends them to a full 366-day year.

```
uv run python scripts/run_build_library.py \
    --ndvi-dir /nas/swim/examples/tongue/data/landsat/extracts/ndvi/irr \
    --cdl-csv /nas/swim/examples/tongue/data/landsat/extracts/cdl/cdl_crop_type_2008_2024.csv \
    --output /nas/swim/examples/tongue/data/crop_library/tongue_crop_library.json
```

The library is a JSON file containing one entry per crop group.

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

```
uv run python scripts/run_scenario.py \
    --scenario /path/to/scenario.toml \
    --output /nas/swim/examples/tongue_ensemble/data/tongue_corn_expansion.swim \
    --report-dir /nas/swim/examples/tongue_ensemble/reports/corn_expansion/
```

This clones the hindcast container, overwrites the targeted fields' NDVI with
the crop library curves, and recomputes irrigation windows.

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

The Tongue River library contains five crop groups built from 2008-2021
CDL-labeled NDVI profiles:

| Crop Group | CDL Codes | Description |
|------------|-----------|-------------|
| `alfalfa` | 36 | Dominant irrigated crop in the basin |
| `grass_pasture` | 171, 176 | Rangeland and pasture |
| `other_hay` | 37 | Non-alfalfa hay |
| `corn` | 1 | Irrigated corn (concentrated in cluster 0) |
| `small_grains` | 21, 23, 24, 25, 27, 28, 29 | Barley, wheat, oats, and other small grains |

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
