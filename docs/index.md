{% include-markdown "../README.md" start="<!--docs-index-start-->" end="<!--docs-index-end-->" %}

## Recommended Reading Order

1. Read the repo overview in the README above.
2. Read [Repo Organization](repo-organization.md) for code and docs layout.
3. Read [Data Assets](data-assets.md) to understand the `/nas/` artifact tree.
4. Follow the workflow docs that match your task:
   - [SID Extraction](workflows/sid-extraction.md)
   - [Tongue Data Assembly](workflows/tongue-data-assembly.md)
   - [Tongue Calibration](workflows/tongue-calibration.md)
   - [Tongue Clustering and Analysis](workflows/tongue-clustering-analysis.md)

## Audience

These docs assume the reader already understands SWIM-RS concepts and wants to
understand what this repository adds:

- project-specific orchestration
- project data lineage
- operational artifact layout
- Tongue deliverable structure
- workflow entry points and caveats
