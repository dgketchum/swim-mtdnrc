# Tongue Clustering and Analysis

## Purpose

This workflow uses historical NDVI behavior, crop-type context, meteorology,
and streamflow to describe crop-pattern structure in the Tongue project and to
support interpretation of the calibrated model products.

## When To Use This Workflow

Use this workflow when you need to:

- summarize historical Tongue NDVI behavior at the field-year level
- derive representative crop curves
- connect cluster behavior to CDL crop classes
- relate field behavior to streamflow and climate summaries

## Primary Entry Points

| Entry point | Purpose |
|-------------|---------|
| `uv run python /home/dgketchum/code/swim-mtdnrc/scripts/run_merge.py` | merge Tongue and annex extract CSVs |
| `uv run python /home/dgketchum/code/swim-mtdnrc/scripts/run_clustering.py` | NDVI clustering |
| `uv run python -m swim_mtdnrc.clustering.crop_curves ...` | crop-curve and phenology summaries |
| `uv run python -m swim_mtdnrc.clustering.cdl_crosstab ...` | cluster x crop-type cross-tab |
| `uv run python /home/dgketchum/code/swim-mtdnrc/scripts/run_streamflow.py` | USGS Tongue streamflow download |
| `uv run python /home/dgketchum/code/swim-mtdnrc/scripts/run_regression.py --k ...` | feature-table assembly and regression analyses |

## Workflow Shape

### 1. Merge the field extracts

`clustering.merge_extracts` combines Tongue and annex CSVs into one annual
dataset keyed by Tongue `FID`.

### 2. Build growing-season NDVI profiles

`clustering.clustering`:

- loads annual NDVI CSVs
- extracts Apr-Oct profiles
- interpolates each field-year to a daily grid
- clusters the resulting profiles with k-means

The output is a cluster assignment and centroid set for each chosen `k`.

### 3. Derive crop curves and phenology

`clustering.crop_curves` computes:

- representative cluster curves
- percentile envelopes
- phenology metrics
- temporal stability summaries

### 4. Add crop-type context

`clustering.cdl_crosstab` joins cluster labels to extracted CDL modal crop
classes so the cluster interpretation is grounded in crop evidence.

### 5. Add hydrologic context

`analysis.streamflow` downloads Tongue River gage discharge. `analysis.regression`
then combines:

- cluster labels
- field-level phenology
- per-GFID GridMET summaries
- streamflow summaries by year

to create a feature table and run predictive or explanatory regressions.

## Main Outputs

| Output | Role |
|--------|------|
| merged NDVI extract CSVs | unified field-year input source |
| cluster summary CSVs and JSON | cluster structure and assignments |
| centroid files | representative NDVI time series |
| crop-curve figures and stats | cluster interpretation products |
| CDL cross-tabs | crop-type interpretation support |
| streamflow CSV | basin hydrologic context |
| feature tables and regression outputs | explanatory analysis outputs |

## Relationship To Calibration

This workflow does not replace calibration. It complements calibration by:

- describing historical vegetation behavior
- providing cluster-level interpretation
- contextualizing basin response against met and streamflow variability

## Notebook Demo

Use this only after reading this workflow page:

- [04 Tongue Clustering Workflow](../notebooks/04_tongue_clustering_workflow.ipynb)

## Next Step: Scenario Analysis

The clustering outputs — especially the CDL cross-tabulation and cluster
assignments — feed directly into the crop curve library used by the scenario
workflow.  See [Tongue Scenario Analysis](tongue-scenario-analysis.md) for
building crop-type substitution scenarios from these results.

## Caveats

- This workflow is partly interpretive, not purely operational.
- Cluster choice should be documented with both statistical and domain reasons.
- Outputs here are best used as analysis products and workflow demonstrations,
  not as substitutes for the calibrated container.
