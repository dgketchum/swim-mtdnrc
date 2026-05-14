# SID Extraction

These tools handle the county-scale extraction jobs for swim inputs:
ETf, NDVI, IrrMapper irrigation, and related QC, plus the generalized OpenET ETf
extractor for non-Tongue field sets (Wyoming Tongue).
The main entry points are `run_openet_etf.py` and the direct module commands
for `sid_etf`, `sid_ndvi`, `sid_irr`, and `sid_diagnostics`. Their outputs are
extraction files organized by county, mask, model, and year. These are operational
export workflows, so the important things to
document for any run are the field set, export naming, mask behavior, and where
the resulting files were staged for downstream assembly. As of 2026-03-12 they are
in the `gs://wudr/sid` bucket, mirrored locally to `/nas/swim/sid/bucket/`.

---

## Prepped Layout

`sid_prepped.py` restructures the bucket mirror into a collaborator-ready tree
at `/nas/swim/sid/prepped/`. Each county gets its own subdirectory:

```
/nas/swim/sid/prepped/
  {county}/                    e.g. 003, 073, 081
    gis/
      sid_{county}.shp         SID fields for this county (fiona-written)
      sid_{county}.{shx,dbf,prj,cpg}
    properties/
      irr_sid_{county}.csv     IrrMapper irrigation fractions
    ndvi/
      irr/   ndvi_irr_{year}.csv
      inv_irr/
    etf/
      {model}/
        irr/   {model}_etf_irr_{year}.csv
        inv_irr/
    eta/
      irr/   ensemble_eta_irr_{year}.csv   (columns: ensemble_eta_YYYYMM01)
      inv_irr/
```

Sub-batches are merged transparently: `073a/ + 073b/ → 073/`,
`081a/b/c/d/ → 081/`.

ETa column renaming: raw `YYYY_MM` columns become `ensemble_eta_YYYYMM01`
so the 8-digit suffix is parseable by `swimrs` ingest.

### Running sid_prepped.py

All counties, all variables:

```bash
python -m swim_mtdnrc.extraction.sid_prepped
```

Subset run for testing:

```bash
python -m swim_mtdnrc.extraction.sid_prepped --counties 003,073,081 --overwrite
```

Key options:

| Flag | Default | Description |
|------|---------|-------------|
| `--bucket-root` | `/nas/swim/sid/bucket` | Local bucket mirror |
| `--prepped-root` | `/nas/swim/sid/prepped` | Output tree |
| `--counties` | all | Comma-separated county numbers |
| `--variables` | all | `ndvi,etf,eta,gis,properties` |
| `--models` | all | ETf models to include |
| `--masks` | `irr,inv_irr` | Mask types |
| `--overwrite` | false | Overwrite existing files |

---

## Building a County Container

After assembling the prepped tree, a collaborator can build a swim-rs container
for any county with:

```python
from swimrs.container import create_container

county = "003"
prepped = f"/nas/swim/sid/prepped/{county}"

c = create_container(
    f"sid_{county}.swim",
    f"{prepped}/gis/sid_{county}.shp",
    "FID",
    "1991-01-01",
    "2023-12-31",
)

c.ingest.ndvi(f"{prepped}/ndvi/irr", mask="irr")
c.ingest.etf(f"{prepped}/etf/ensemble/irr", model="ensemble", mask="irr")
c.ingest.eta(f"{prepped}/eta/irr", mask="irr")
c.ingest.properties(irr_csv=f"{prepped}/properties/irr_sid_{county}.csv")
```

GridMET met data is not included in the prepped tree — collaborators download
it separately using swim-rs tooling.
