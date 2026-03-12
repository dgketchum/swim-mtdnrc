# Known Issues

## Purpose

This page centralizes workflow caveats that collaborators should know before
re-running or interpreting project outputs.

## Active Issues

### Degenerate Tongue field: `FID 1416`

- `FID 1416` is a degenerate polygon and does not behave like a normal field.
- It can produce missing or unusable extraction values.
- It should be excluded from calibration deliverables and treated carefully in
  any reproduction workflow.

### SIMS coverage gap for one non-crop field

- SIMS ETf coverage is expected to be missing for at least one non-crop field
  in the Tongue workflow.
- This is a data-source behavior issue, not necessarily a container defect.
- Health reporting should reflect this as expected behavior rather than a
  surprising calibration failure.

### Path drift across Tongue roots

- The repo still references more than one Tongue root:
  - `/nas/swim/examples/tongue/`
  - `/nas/swim/examples/tongue_annex/`
  - `/nas/swim/examples/tongue_new/`
- Re-runs should document which tree was treated as the authoritative output
  root for that run.

### Calibration provenance depends on side artifacts

- `batch_manifest.csv`, `batch_log.json`, and health report artifacts are part
  of calibration provenance.
- Do not treat them as disposable if the run may need to be resumed or audited.

### This repo is not the full hindcast/reporting surface

- `swim-mtdnrc` handles project assembly, calibration orchestration, and
  analysis.
- Hindcast execution and some reporting flows still depend directly on
  `swim-rs` tools and APIs.

## Documentation Guidance

Whenever a run is shared externally, document at least:

- the container path
- the output root used for the run
- whether the crosswalk was rebuilt or reused
- whether health gate failures were overridden
- where the latest health and calibration reports were written
