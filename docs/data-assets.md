# Data Assets

These docs assume collaborators are working from the shipped Tongue project
directory:

- `tongue_ensemble/`

Older assembly roots are intentionally out of scope for the collaborator docs.

## Project Directory Layout

`tongue_ensemble/` is the public project root for the Tongue delivery.

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

### Calibration container

The calibration container is the observed-period `.swim` container used for the
inverse run. It is the container that:

- ingests the extracted remote-sensing and meteorology inputs
- carries the ETf targets used for calibration
- stores the calibrated parameter results
- preserves health and calibration metadata

Typical contents include:

- fields and geometry metadata
- properties
- remote-sensing time series
- meteorology
- snow inputs
- derived arrays
- calibrated parameters
- health and run metadata

### Forward run container

A forward run container is a container prepared for evaluation, hindcast, or
projection after calibration. It keeps the calibrated parameterization but is
oriented toward running the model forward rather than solving an inverse
problem.

Relative to the calibration container, a forward run container typically:

- reuses the calibrated parameter state
- swaps in run-specific forcings or scenario inputs as needed
- carries forward-run outputs and reports rather than inversion artifacts

The important distinction is:

- calibration container = inverse-run source of truth
- forward run container = run-ready delivery for hindcast, evaluation, or projection

### Calibration artifacts

Batch calibration outputs typically include:

- `batch_manifest.csv`
- `batch_log.json`
- per-batch working directories
- health reports
- calibration reports

### Crop curve library

A JSON file containing per-crop-type representative 366-day NDVI curves built
from CDL-labeled historical profiles.  Used by the scenario workflow to
substitute crop phenology on selected fields.

- `data/crop_library/tongue_crop_library.json`

### Scenario containers

Scenario containers are clones of the hindcast container with specific fields'
NDVI replaced by crop-library curves.  They are runnable with `swim run` using
the same calibrated parameters.

- `data/tongue_<scenario_name>.swim`

### Analytical outputs

Under clustering and analysis directories, expect:

- cluster assignments
- centroid time series
- crop-curve figures and stats
- CDL cross-tabs
- feature tables
- regression summaries
