# Tongue River Basin: Clustering, Calibration & Regression Plan

## Overview

2,084 irrigated fields across three legacy directories (tongue, tongue_annex, tongue_new).

### Deliverables
1. Time series clustering of historical Landsat NDVI to identify crop growth patterns
2. Representative "business as usual" crop curves per cluster
3. Correlation of crop curve characteristics with meteorology and Tongue River streamflows

## Implementation Order

| Step | Module | Prereqs | EE? |
|------|--------|---------|-----|
| 0 | Repo setup | None | No |
| 1A | `clustering/merge_extracts.py` | Repo | No |
| 1C | `analysis/streamflow.py` | Repo + dataretrieval | No |
| 4A | `clustering/clustering.py` | 1A | No |
| 4B | `clustering/crop_curves.py` | 4A | No |
| 1B | `extraction/tongue_extract_ssebop.py` | Repo, user approval | Yes |
| 2A-2C | Container build | 1A, 1B | No |
| 3 | PEST calibration | 2C | No |
| 5A | `analysis/regression.py` | 4A, 1C | No |

**Critical path for clustering deliverable: 0 -> 1A -> 4A -> 4B -> 5A** (no EE needed).
