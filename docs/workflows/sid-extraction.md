# SID Extraction

## Purpose

These workflows produce remote-sensing inputs keyed to SID or other state-scale
field identifiers. They are upstream of Tongue assembly and are also useful for
broader DNRC extraction work.

## When To Use This Workflow

Use the SID and state-scale extraction tools when you need:

- county-partitioned ETf or NDVI exports keyed to SID fields
- irrigation classification extracts for SID fields
- OpenET ETf exports for non-Montana field sets using the same workflow shape

## Primary Entry Points

| Entry point | Scope |
|-------------|-------|
| `uv run python /home/dgketchum/code/swim-mtdnrc/scripts/run_openet_etf.py ...` | state-scale OpenET ETf extraction for non-Tongue field sets |
| `uv run python -m swim_mtdnrc.extraction.sid_etf ...` | SID ETf extraction |
| `uv run python -m swim_mtdnrc.extraction.sid_ndvi ...` | SID NDVI extraction |
| `uv run python -m swim_mtdnrc.extraction.sid_irr ...` | SID irrigation extraction |
| `uv run python -m swim_mtdnrc.extraction.sid_diagnostics ...` | SID QC and diagnostics |

## Inputs and Dependencies

This workflow depends on:

- Earth Engine authorization
- source field boundaries
- irrigation mask logic from IrrMapper
- bucket or local export destinations

Typical external dependencies come from `swim-rs` EE helpers plus project
shapefiles and asset paths.

## What The Code Does

### ETf

`sid_etf.py` and `openet_etf.py` export ETf for one or more OpenET models by:

1. loading field boundaries
2. assigning county grouping
3. applying irrigation or inverse-irrigation masking
4. exporting annual CSVs by model and mask

### NDVI

`sid_ndvi.py` exports Landsat NDVI using the same mask semantics and county
partitioning pattern.

### Irrigation

`sid_irr.py` exports yearly irrigation-classification context used downstream in
container build and dynamics calculations.

### Diagnostics

`sid_diagnostics.py` is the QC entry point for checking observation frequency,
mask behavior, and missingness for extracted data.

## Outputs

Typical output organization is by:

- county or county-chunk
- variable
- mask
- year

This is an extraction workflow, so its primary outputs are intermediate files
used by later assembly steps rather than collaborator-facing final deliverables.

## Relationship To Tongue

The Tongue workflow uses SID-derived extracts where:

- SID polygons overlap Tongue fields
- the Tongue-SID crosswalk can map SID field IDs back to Tongue integer FIDs

Those SID extracts are later remapped and merged into Tongue-oriented output
trees by the Tongue assembly workflow.

## Caveats

- These workflows are Earth Engine jobs, not lightweight local transforms.
- County and chunk naming matter because downstream assembly assumes the export
  naming pattern is stable.
- Irrigation mask year handling is a real correctness concern and should be
  documented with the run, especially for recent years.
