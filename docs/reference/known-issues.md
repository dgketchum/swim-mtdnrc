# Known Issues

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
