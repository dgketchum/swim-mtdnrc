#!/usr/bin/env python
"""Run legacy NDVI merge for Tongue River Basin.

Usage:
    python scripts/run_merge_legacy.py
    python scripts/run_merge_legacy.py --dry-run
"""

from swim_mtdnrc.calibration.merge_legacy import main

if __name__ == "__main__":
    main()
