# Repo Organization

## Purpose

`swim-mtdnrc` is a project-workflow repository layered on top of `swim-rs`.
The code here is real package code, but the repo should be understood primarily
through its workflows and outputs rather than as a general-purpose framework.

## Top-Level Layout

| Path | Role |
|------|------|
| `src/swim_mtdnrc/` | Project-specific Python package code |
| `scripts/` | Thin CLI wrappers around the package `main()` entry points |
| `docs/` | Collaborator-facing project documentation |
| `tests/` | Targeted tests for project logic |
| `notes/` | Internal notes and plans; not part of the public docs surface |
| `README.md` | Front door for repo users |

## Code Layout

### `src/swim_mtdnrc/extraction`

Earth Engine and extraction workflows for:

- SID ETf, NDVI, and irrigation data
- Tongue-specific NDVI, SNODAS, and CDL extraction
- generalized OpenET ETf extraction for non-Montana field sets

### `src/swim_mtdnrc/calibration`

Operational prep and calibration workflows for Tongue:

- Tongue-SID crosswalk generation
- SID remap and merge
- legacy extract merge logic
- input conversion and cleanup
- container build orchestration
- batch calibration and restart-state persistence

### `src/swim_mtdnrc/clustering`

Analytical workflows for:

- NDVI merge
- growing-season profile extraction
- k-means clustering
- crop curves and phenology summaries
- CDL cross-tabulation

### `src/swim_mtdnrc/analysis`

Supporting analysis workflows for:

- streamflow download
- meteorology and streamflow feature-table assembly
- cluster and continuous regression analyses

## Why `src/` Stays

The `src/` layout is still the right structure for this repo because:

- workflow modules are imported by multiple scripts
- the repo has tests that target package behavior
- the code is more stable than an ad hoc scripts-only layout

The docs should solve the comprehension problem by explaining what the package
does, not by hiding the package structure.

## How This Relates to `swim-rs`

| Concern | `swim-rs` | `swim-mtdnrc` |
|---------|-----------|---------------|
| Core model and container | Yes | No |
| Core calibration engine | Yes | Wraps and orchestrates |
| Project-specific data assembly | Limited | Yes |
| Tongue operational workflows | No | Yes |
| DNRC-specific analyses | No | Yes |

Use `swim-rs` docs for framework behavior. Use these docs for the project layer.
