# Tongue Ensemble Calibration

Calibrated SWIM for 1,999 fields in the Tongue River Basin against a 6-model
OpenET ETf ensemble (SSEBop, SIMS, geeSEBAL, eeMETRIC, PT-JPL, DisALEXI),
2010-2025. This document describes the full workflow from raw data through
calibrated container, including the tools used, the problems we hit, and the
decisions we made.

## Project Layout

```
tongue_ensemble/
  tongue_ensemble.toml          # project config
  container_prep.py             # builds the .swim container
  data/
    tongue_ensemble.swim        # calibrated container (1999 fields, 2010-2025)
    gis/tongue_fields_gfid.shp  # field geometries (FID 1-2000)
    remote_sensing/             # raw ETf + NDVI extracts by model/mask
    meteorology/gridmet/        # daily GridMET parquet
    snow/snodas/                # SNODAS SWE extracts
    properties/                 # SSURGO soils, IrrMapper, CDL, MODIS LC
  pestrun/
    batch_log.json              # calibration run log
    batch_manifest.csv          # field-to-batch mapping
    health/<timestamp>/         # pre-calibration health report artifacts
    calibration.html            # post-calibration parameter report
    calibration.png             # parameter distribution histograms
```

## 1. Field Geometries and the SID Crosswalk

Montana Tongue fields come from the Montana Statewide Irrigation Dataset (SID). The
SID shapefile uses county-coded string FIDs (`017_000042`) while the Tongue
shapefile uses integer FIDs (1-2000). A spatial join crosswalk links the two.

**Tool:** `swim_mtdnrc.calibration.crosswalk`

The crosswalk lets us pull OpenET extracts keyed by SID FID and map them back
to Tongue container indices. 639 of 2000 fields have direct SID polygon
overlap; the remainder are non-SID, Wyoming fields.

## 2. Remote Sensing Extraction

ETf and NDVI extracts were pulled from Google Earth Engine via OpenET's
per-model image collections for each field polygon, under two irrigation masks:

- **irr**: pixels classified irrigated by IrrMapper in that year
- **inv_irr**: the complement (non-irrigated pixels within the field)

The model selects the appropriate mask per field per year based on the
IrrMapper classification.

### ETf (7 models, 2016-2025)

**Tool:** `swim_mtdnrc.extraction.openet_etf` (Tongue-specific),
`swim_mtdnrc.extraction.sid_etf` (SID county-level)

Seven OpenET models extracted: SSEBop, SIMS, geeSEBAL, eeMETRIC, PT-JPL,
DisALEXI, and the OpenET ensemble mean. Each model produces ETf on Landsat
overpass dates (~8-day revisit from Landsat 8+9 combined). Output: one CSV per
model per mask per year, columns are field FIDs, rows are dates.

One known gap: SIMS only runs on CDL crop-type fields. FID 694 (non-crop) has
zero SIMS observations but valid data from all other models. This is expected
behavior, not a data error.

### NDVI (Landsat 1987-2025, Sentinel 2018-2024)

**Tool:** `swim_mtdnrc.extraction.tongue_extract_ndvi`, plus SID-level
`swim_mtdnrc.extraction.sid_ndvi`

Landsat NDVI provides the long record; Sentinel fills gaps in the recent
period. Both are fused in the container into `derived/merged_ndvi/` during
the build step.

### Meteorology (GridMET, 2010-2025)

Extracted via `swim extract` (swim-rs CLI). Daily precipitation, temperature
(min/max), solar radiation, and grass reference ET (ETo) from GridMET's
4km grid, with field-to-grid mapping handled by the container ingestor.

### Snow (SNODAS, 2010-2025)

**Tool:** `swim_mtdnrc.extraction.tongue_extract_snodas`

Daily SWE from NOAA's SNODAS 1km product, extracted per field centroid.

### Properties

- **Soils (SSURGO):** AWC, sand, clay, Ksat — from NRCS Web Soil Survey
- **Irrigation (IrrMapper):** per-year irrigated fraction, 1987-2025
- **Land cover (MODIS):** IGBP classification for rooting depth assignment
- **CDL:** Cropland Data Layer for crop type context

## 3. Container Build

**Tool:** `container_prep.py` (project-level script that calls swimrs ingestors)

The container build takes the raw extract CSVs and properties and writes them
into a single zarr-based `.swim` file with the standard SWIM-RS schema.
Everything the model needs lives in one portable artifact.

Key decisions in the Tongue Ensemble build:

- **Excluded FID 1416.** A degenerate polygon (~0.76 x 0.38 m triangle) that
  produces NaN in all remote sensing extractions. Removed at build time with
  `--exclude-fids 1416` to keep the container clean.

- **NDVI fusion.** Landsat and Sentinel NDVI are fused into a single
  `derived/merged_ndvi/` time series per mask via the container's compute step.

- **Dynamics computation.** Per-field per-year irrigation flags and crop
  coefficients are derived from the ingested IrrMapper data.

### Config highlights (`tongue_ensemble.toml`)

```toml
etf_target_model = "ensemble"
etf_ensemble_members = ["ssebop", "sims", "geesebal", "eemetric", "ptjpl", "disalexi"]
mask_mode = "irrigation"
start_date = "2010-01-01"
end_date = "2025-12-31"
realizations = 20
workers = 40
```

The target model is the OpenET ensemble mean. The six individual members are
stored in the container for potential per-model analysis or alternative
calibration targets.

## 4. Health Check

**Tool:** `container.report()` (swimrs API)

After the build, an automatic health check validates the container. This runs
42 checks across properties, time series coverage, and field-level policy
rules. The Tongue Ensemble container passes with one warning:

- **WARN:** SIMS ETf missing 1/1999 fields (FID 694, non-crop — expected)

The health report is rendered under `pestrun/health/<timestamp>/` as
`health.json`, `health.html`, and `health.png`. Post-build and hindcast /
forecast container builds use the same artifact names under a sidecar
`<container>.reports/health/<timestamp>/` tree. When the container is writable,
the latest saved path is also stored in the container's
`last_health_check.report_dir` attrs so that downstream tools can verify which
report was used.

## 5. Calibration

**Tool:** `swim_mtdnrc.calibration.batch_calibrate`

### Setup

1,999 fields partitioned into 48 batches of ~40-50 fields. Each batch runs
PEST++ IES with 20 realizations and 3 optimization iterations (noptmax=3),
using 40 parallel workers.

The calibration target is the ensemble ETf: the model simulates daily ETf for
each field, and PEST++ adjusts parameters to minimize the misfit between
simulated and observed ETf on Landsat overpass dates.

### Pipeline

The `calibrate-all` action runs a pipelined workflow:

1. **Preflight gate** — verifies container health check passed
2. **Build** — assembles PEST++ control files and templates for the batch
3. **Pre-build overlap** — while PEST++ runs batch N, batch N+1 pre-builds
4. **Run** — PEST++ IES with 40 workers (~10 min per batch)
5. **Ingest** — calibrated parameters written back into the container
6. **Cleanup** — batch working directory removed (at most 2 on disk)

The pipeline is crash-safe: a JSON batch log tracks status, and the process
resumes from the last incomplete batch.

### Parameters

Eight parameters calibrated per field:

| Parameter   | Range       | What It Controls |
|-------------|-------------|------------------|
| `aw`        | 100-400 mm  | Available water capacity |
| `mad`       | 0.10-0.90   | Management allowed depletion fraction |
| `ndvi_0`    | 0.10-0.80   | NDVI sigmoid midpoint |
| `ndvi_k`    | 3-20        | NDVI sigmoid steepness |
| `ks_damp`   | 0.01-1.0    | Soil evaporation stress damping |
| `kr_damp`   | 0.01-1.0    | Transpiration stress damping |
| `swe_alpha` | -0.5-1.0    | Snowmelt temperature sensitivity |
| `swe_beta`  | 0.5-2.5     | Snowmelt radiation sensitivity |

### Results

All 1,999 fields calibrated across 48 batches.

| Parameter   | Mean    | Cross-field Std | Min     | Max     |
|-------------|---------|-----------------|---------|---------|
| `aw`        | 272 mm  | 77 mm           | 104 mm  | 400 mm  |
| `mad`       | 0.257   | 0.151           | 0.100   | 0.631   |
| `ndvi_0`    | 0.536   | 0.295           | 0.100   | 0.800   |
| `ndvi_k`    | 8.93    | 3.27            | 3.00    | 20.0    |
| `ks_damp`   | 0.512   | 0.280           | 0.010   | 1.000   |
| `kr_damp`   | 0.344   | 0.256           | 0.013   | 1.000   |
| `swe_alpha` | 0.344   | 0.087           | 0.106   | 0.681   |
| `swe_beta`  | 1.560   | 0.168           | 1.044   | 2.190   |

Notable patterns:
- **aw** hits the upper bound (400 mm) for 184 fields — likely deep alluvial soils
- **ndvi_0** hits the upper bound (0.8) for 873 fields — fields with consistently
  high NDVI where the sigmoid midpoint is unconstrained
- **swe_beta** has high ensemble uncertainty for 917 fields — snow radiation
  sensitivity is poorly constrained in a semi-arid basin where snow is marginal
- **mad** clusters near the lower bound (0.1) for 271 fields — minimal depletion
  tolerance, consistent with well-watered irrigated fields

## 6. Calibration Report

**Tool:** `container.calibration_report()` (swimrs API)

Parallel to the health check, the calibration report provides:

- **Console summary** — parameter statistics table with QC flag counts
- **calibration.html** — full HTML report with color-coded flag columns
- **calibration.png** — 8-panel histogram of parameter distributions
- **calibration.json** — machine-readable per-field flags and stats
- **`report.to_dataframe()`** — DataFrame export for analysis

QC flags identify fields at parameter bounds (potentially under-constrained)
and fields with high ensemble uncertainty (>25% of the parameter range).

```python
from swimrs.container import open_container

c = open_container("tongue_ensemble.swim")
report = c.calibration_report(output_dir=".")
print(report.summary())
df = report.to_dataframe()
```

## 7. Container as Deliverable

The calibrated container is the handoff artifact. It holds all inputs,
calibrated parameters, uncertainty, and metadata in one portable file.
Collaborators can:

```python
from swimrs.container import open_container

c = open_container("tongue_ensemble.swim")

# Health check
health = c.report(config={"mask_mode": "irrigation", "etf_target_model": "ensemble"})
print(health.summary())

# Calibration report
cal = c.calibration_report(output_dir="reports/")

# Extract parameters as DataFrame
df = cal.to_dataframe()

# Run the forward model
from swimrs.process.input import build_swim_input
swim_input = build_swim_input(c, "swim_input.h5", etf_model="ensemble")
```

No loose CSVs, no directory of intermediate files. The container and the
swim-rs API are the interface.

## 8. Known Issues and Decisions

### UID coercion bug (fixed)
Early container builds silently dropped all properties because the ingestor
compared string container UIDs against integer CSV FIDs. Fixed by coercing
DataFrame indices to string after `set_index()`. The health check system was
built specifically to catch this class of silent failure.

### IRR_MAX_YEAR bug (fixed)
ETf extractions for 2024-2025 used the 2023 IrrMapper classification instead
of the correct year. Fixed in `sid_etf.py`, `tongue_extract_ssebop.py`,
`sid_ndvi.py`, and `openet_etf.py`. Only affects 2024-2025 data; 2016-2023
was unaffected.

### Phi history not captured
The first successful calibration run did not record phi (objective function)
history because `PestResults` looked for `.rec` files in the wrong directory.
Fixed for future runs (`PestResults` now accepts a `master_dir` argument).
The calibrated parameters are valid; only the convergence diagnostics are
missing.

### SIMS coverage
SIMS only produces ETf for CDL crop-type fields. Non-crop fields (including
FID 694) have no SIMS data. The health check policy downgrades this to a
warning rather than a failure, since the ensemble mean and other 5 models
provide coverage.

## 9. Next Steps

### Projection containers
Collaborators will run the calibrated model into the future using LOCA-VIC
projected meteorology and median NDVI. The plan is a projection container
factory that clones the calibrated container, swaps in future met, and tiles
a per-DOY NDVI climatology. That future design work is not documented in the
public docs set yet.

### Parameter bounds
Several parameters show clustering at bounds (aw, ndvi_0, mad). A future
calibration pass with adjusted bounds or reparameterization may improve
constraint. The calibration report flags these fields for review.

### Statewide scaling
The same pipeline scales to all 51,404 SID fields across Montana. The
extraction tools (`sid_etf.py`, `sid_ndvi.py`) already operate at the county
level. Container build and calibration would run per-county, with results
merged into a statewide dataset.
