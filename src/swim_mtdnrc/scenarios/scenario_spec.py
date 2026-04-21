"""Scenario specification: parse and validate crop-type substitution scenarios.

A scenario defines which fields should have their NDVI replaced with a
crop-library curve.  Scenarios can be specified in TOML or CSV format.

TOML example::

    [scenario]
    name = "corn_expansion"
    description = "Convert 50 alfalfa fields to corn"
    crop_library = "/path/to/tongue_crop_library.json"
    source_container = "/path/to/tongue_hindcast.swim"

    [[scenario.substitutions]]
    fids = [101, 102, 103]
    crop = "corn"

    [[scenario.substitutions]]
    fids = [200, 201]
    crop = "small_grains"

CSV example (two columns, ``FID`` and ``crop``)::

    FID,crop
    101,corn
    102,corn
    200,small_grains
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


@dataclass
class CropSubstitution:
    """A single field's crop substitution."""

    fid: str
    target_crop: str


@dataclass
class ScenarioSpec:
    """Complete scenario specification."""

    name: str
    description: str
    crop_library_path: str
    source_container: str
    substitutions: list[CropSubstitution] = field(default_factory=list)

    @classmethod
    def from_toml(cls, path: str) -> ScenarioSpec:
        """Parse a scenario from a TOML file."""
        with open(path, "rb") as f:
            data = tomllib.load(f)

        scenario = data["scenario"]

        subs = []
        for group in scenario.get("substitutions", []):
            crop = group["crop"]
            for fid in group["fids"]:
                subs.append(CropSubstitution(fid=str(fid), target_crop=crop))

        return cls(
            name=scenario["name"],
            description=scenario.get("description", ""),
            crop_library_path=scenario["crop_library"],
            source_container=scenario["source_container"],
            substitutions=subs,
        )

    @classmethod
    def from_csv(
        cls,
        csv_path: str,
        library_path: str,
        source_container: str,
        name: str,
        description: str = "",
    ) -> ScenarioSpec:
        """Parse a scenario from a CSV file (columns: FID, crop)."""
        subs = []
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                subs.append(
                    CropSubstitution(
                        fid=str(row["FID"]),
                        target_crop=row["crop"],
                    )
                )

        return cls(
            name=name,
            description=description,
            crop_library_path=library_path,
            source_container=source_container,
            substitutions=subs,
        )

    def validate(self, library: dict, container_fids: list[str]) -> list[str]:
        """Validate the scenario against a loaded library and container fields.

        Parameters
        ----------
        library : dict
            Loaded crop library (from ``load_crop_library``).
        container_fids : list[str]
            Field UIDs present in the source container.

        Returns
        -------
        list of str
            Validation warnings and errors.  Entries prefixed with ``ERROR:``
            indicate fatal issues; ``WARN:`` entries are advisory.
        """
        messages = []
        fid_set = set(container_fids)

        seen_fids = set()
        for sub in self.substitutions:
            if sub.fid in seen_fids:
                messages.append(f"WARN: duplicate FID {sub.fid}")
            seen_fids.add(sub.fid)

            if sub.fid not in fid_set:
                messages.append(f"ERROR: FID {sub.fid} not found in container")

            if sub.target_crop not in library:
                messages.append(
                    f"ERROR: crop '{sub.target_crop}' not in library "
                    f"(available: {', '.join(sorted(library))})"
                )

        if not self.substitutions:
            messages.append("WARN: no substitutions defined")

        return messages
