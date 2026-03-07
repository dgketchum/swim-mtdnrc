# Batch Calibration Workflow

PEST++ IES ensemble calibration of 8 hydrologic parameters per field,
batched by GFID into groups of ~50 fields each (~48 batches total for
~2000 Tongue River Basin fields).

## Data Flow

```
shapefile ──> batch_manifest.csv ──> build (spinup + PEST setup)
                                         │
                                         v
                                    PEST++ IES run (40 workers)
                                         │
                                         v
                                    .par.csv ──> ingest ──> container
                                                               │
                                                          cleanup batch dir
```

## Quick Start

Run everything with a single command:

```bash
python -m swim_mtdnrc.calibration.batch_calibrate \
    --action calibrate-all \
    --workers 40 --noptmax 3 --reals 20
```

This pipelines: while batch N runs PEST++, batch N+1 pre-builds in the
background. At most 2 batch directories (~1.6 GB) on disk at any time.

## What Each Stage Does

- **Build**: Opens the container read-only, runs spinup to initialize soil
  moisture state, generates PEST++ control files (.pst), observation files,
  parameter templates, and a localization matrix.
- **Run**: Launches PEST++ IES with an ensemble of parameter realizations.
  Workers execute the forward model in parallel.
- **Ingest**: Reads the final .par.csv, computes the median across
  realizations, and writes calibrated parameters into the container's
  `calibration/` group.
- **Cleanup**: Removes the batch directory after successful ingest.

## Parameters Calibrated

| Parameter | Description | Typical Range |
|-----------|-------------|---------------|
| aw | Available water capacity | 0.05 - 0.30 |
| rew | Readily evaporable water | 2 - 12 |
| tew | Total evaporable water | 10 - 50 |
| ndvi_alpha | NDVI-Kcb slope | 0.8 - 1.5 |
| ndvi_beta | NDVI-Kcb intercept | -0.2 - 0.2 |
| melt_factor | Snowmelt rate | 0.5 - 5.0 |
| swe_alpha | SWE correction | 0.5 - 2.0 |
| swb_scale | Soil water balance scale | 0.5 - 2.0 |

## Monitoring

Check progress during or after a run:

```bash
# Container status (fields calibrated, phi reduction per batch)
python -m swim_mtdnrc.calibration.batch_calibrate --action status

# Detailed per-batch status with errors
cat /nas/swim/examples/tongue/pestrun/batch_log.json | python -m json.tool

# Follow the nohup log
tail -f calibrate_all.log
```

## Resume After Failure

If the process crashes or is killed, restart with `--resume`:

```bash
python -m swim_mtdnrc.calibration.batch_calibrate \
    --action calibrate-all --resume
```

Already-ingested batches are skipped. Partially-built batch directories on
disk are detected and reused. Failed batches (NaN spinup, PEST++ crash) are
logged to `batch_log.json` with full tracebacks and skipped.

## Cleanup

Failed batch directories are preserved for debugging. To remove them:

```bash
python -m swim_mtdnrc.calibration.batch_calibrate --action cleanup-failed
```

This removes directories for `run_failed` and `ingest_failed` batches and
marks them as `cleaned` in `batch_log.json`.

## Key Files

| File | Purpose |
|------|---------|
| `batch_manifest.csv` | Batch assignments (batch_id, FID). Single source of truth. |
| `batch_log.json` | Per-batch status, errors, dropped FIDs, timestamps. |
| Container `calibration/` | Final calibrated parameters, phi summaries. |

The manifest is written once on the first run. Changing `--batch-size` or
editing the shapefile afterward has no effect — delete the manifest to
re-partition.

## Tuning

| Flag | Default | Notes |
|------|---------|-------|
| `--noptmax` | 3 | PEST++ optimization iterations |
| `--reals` | 20 | Ensemble realizations |
| `--workers` | 40 | Parallel PEST++ workers |
| `--batch-size` | 50 | Fields per batch (only affects initial partitioning) |
