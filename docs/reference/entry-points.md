# Entry Points

## Purpose

This page maps the stable user-facing commands in the repo to the workflow they
start.

## Script Wrappers

| Command | Module target | Notes |
|---------|---------------|-------|
| `scripts/run_assemble_sid.py` | `swim_mtdnrc.calibration.assemble_sid.main` | Tongue SID remap and merge |
| `scripts/run_calibration.py` | `swim_mtdnrc.calibration.batch_calibrate.main` | batch calibration control surface |
| `scripts/run_clustering.py` | `swim_mtdnrc.clustering.clustering.main` | NDVI clustering |
| `scripts/run_crosswalk.py` | `swim_mtdnrc.calibration.crosswalk.main` | Tongue-SID crosswalk |
| `scripts/run_merge.py` | `swim_mtdnrc.clustering.merge_extracts.main` | Tongue and annex merge |
| `scripts/run_merge_legacy.py` | `swim_mtdnrc.calibration.merge_legacy.main` | legacy Tongue merge utilities |
| `scripts/run_openet_etf.py` | custom wrapper around `swim_mtdnrc.extraction.openet_etf.run` | state-scale OpenET extraction |
| `scripts/run_regression.py` | `swim_mtdnrc.analysis.regression.main` | feature table and regression outputs |
| `scripts/run_streamflow.py` | `swim_mtdnrc.analysis.streamflow.main` | USGS Tongue streamflow download |
| `scripts/run_build_library.py` | `swim_mtdnrc.clustering.crop_library.main` | build crop-type NDVI curve library |
| `scripts/run_scenario.py` | custom wrapper | build scenario container and diagnostics |

## Direct Module Entry Points

These are important because not every stable workflow has a wrapper script.

| Command | Role |
|---------|------|
| `uv run python -m swim_mtdnrc.calibration.build_container --help` | build or update Tongue container |
| `uv run python -m swim_mtdnrc.calibration.prep_inputs --help` | local prep utilities |
| `uv run python -m swim_mtdnrc.clustering.crop_curves --help` | crop curves and phenology |
| `uv run python -m swim_mtdnrc.clustering.cdl_crosstab --help` | cluster x CDL analysis |
| `uv run python -m swim_mtdnrc.clustering.crop_library --help` | crop-type NDVI curve library |
| `uv run python -m swim_mtdnrc.extraction.sid_etf --help` | SID ETf extraction |
| `uv run python -m swim_mtdnrc.extraction.sid_ndvi --help` | SID NDVI extraction |
| `uv run python -m swim_mtdnrc.extraction.sid_irr --help` | SID irrigation extraction |
| `uv run python -m swim_mtdnrc.extraction.sid_diagnostics --help` | SID QC and diagnostics |
| `uv run python -m swim_mtdnrc.extraction.tongue_extract_cdl --help` | Tongue CDL extraction |
| `uv run python -m swim_mtdnrc.extraction.tongue_extract_ndvi --help` | Tongue NDVI extraction |
| `uv run python -m swim_mtdnrc.extraction.tongue_extract_snodas --help` | Tongue SNODAS extraction |
| `uv run python -m swim_mtdnrc.extraction.tongue_extract_ssebop --help` | Tongue SSEBop extraction |

## Operational Notes

Use this rough classification when deciding what is safe to run:

| Type | Typical commands |
|------|------------------|
| local and analytical | clustering, crop curves, crosstab, streamflow, regression |
| local but artifact-mutating | crosswalk, legacy merge, prep inputs, container build |
| Earth Engine or remote export | SID ETf, SID NDVI, SID irrigation, Tongue remote-sensing extraction |
| calibration or inversion | `run_calibration.py` and batch calibration module actions |

Earth Engine and calibration workflows should be treated as operational runs,
not exploratory shell commands.
