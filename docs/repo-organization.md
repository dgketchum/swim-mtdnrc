# Repo Organization

## Top-Level Layout

| Path | Role |
|------|------|
| `src/swim_mtdnrc/` | Project-specific Python package code |
| `scripts/` | Thin CLI wrappers around the package `main()` entry points |
| `docs/` | Collaborator-facing project documentation |
| `tests/` | Targeted tests for project logic |
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

## How This Relates to `swim-rs`

| Concern | `swim-rs` | `swim-mtdnrc` |
|---------|-----------|---------------|
| Core model and container | Yes | No |
| Core calibration engine | Yes | Wraps and orchestrates |
| Project-specific data assembly | Limited | Yes |
| Tongue operational workflows | No | Yes |
| DNRC-specific analyses | No | Yes |

Use `swim-rs` docs for framework behavior. Use these docs for the project layer.
