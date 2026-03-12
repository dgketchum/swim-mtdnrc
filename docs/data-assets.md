# Data Assets

## Purpose

Most of the operational artifacts used by `swim-mtdnrc` live outside the repo
under `/nas/`. This page summarizes the active path conventions and what each
tree is used for.

## Core Project Roots

| Path | Role |
|------|------|
| `/nas/swim/examples/tongue/` | Legacy Tongue project root |
| `/nas/swim/examples/tongue_annex/` | Legacy annex extraction root |
| `/nas/swim/examples/tongue_new/` | Active merged Tongue output root |

## What Lives Under Each Root

### `/nas/swim/examples/tongue/`

Legacy source material and earlier project outputs, including:

- original Tongue shapefiles
- legacy Landsat extracts
- GridMET parquet files
- SNODAS extracts
- properties and prior container artifacts

This tree is still used as an upstream source for some merge and prep steps.

### `/nas/swim/examples/tongue_annex/`

Supplemental extraction source for the annex fields that were merged into the
broader Tongue workflow. Its main relevance is to older NDVI and ETf merge
operations.

### `/nas/swim/examples/tongue_new/`

The current merged project root for collaborator-facing outputs. This is the
main location to document for external users.

Representative subtrees:

| Subtree | Typical contents |
|---------|------------------|
| `data/gis/` | Tongue field shapefiles and crosswalks |
| `data/landsat/extracts/` | NDVI and ETf CSV extracts by model and mask |
| `data/met_timeseries/` | GridMET parquet inputs |
| `data/snow/` | SNODAS extract CSVs |
| `data/properties/` | soils, irrigation, and related properties |
| `data/*.swim` | built container artifacts |
| `data/clustering/` | cluster assignments, centroids, crop-curve outputs, regression outputs |
| `pestrun/` or similar run roots | batch calibration manifests, logs, and reports |

## Artifact Types

### Remote sensing extracts

Usually CSV files organized by:

- variable or model
- irrigation mask
- year

Examples:

- `ndvi/irr/`
- `ndvi/inv_irr/`
- `ensemble_etf/irr/`
- `ssebop_etf/inv_irr/`

### Containers

The core model deliverable is a `.swim` container built with `swim-rs`.

This artifact holds:

- fields and geometry metadata
- properties
- remote sensing time series
- meteorology
- snow inputs
- derived arrays
- calibrated parameters
- health and run metadata

### Calibration artifacts

Batch calibration outputs typically include:

- `batch_manifest.csv`
- `batch_log.json`
- per-batch working directories
- health reports
- calibration reports

### Analytical outputs

Under clustering and analysis directories, expect:

- cluster assignments
- centroid time series
- crop-curve figures and stats
- CDL cross-tabs
- feature tables
- regression summaries

## Documentation Rule of Thumb

When documenting workflows, describe:

- which repo code produces an artifact
- which `/nas/` path receives it
- whether the artifact is an upstream input, an intermediate product, or a
  collaborator-facing deliverable
