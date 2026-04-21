"""Tests for scenario specification and container construction."""

import json
import os

import fiona
from fiona.crs import CRS
import numpy as np
import pandas as pd
import pytest

from swimrs.container import SwimContainer

from swim_mtdnrc.scenarios.scenario_spec import CropSubstitution, ScenarioSpec


# ---------------------------------------------------------------------------
# ScenarioSpec tests
# ---------------------------------------------------------------------------


class TestScenarioSpecToml:
    def test_parse_toml(self, tmp_path):
        toml_content = """\
[scenario]
name = "test_scenario"
description = "Unit test scenario"
crop_library = "/fake/library.json"
source_container = "/fake/source.swim"

[[scenario.substitutions]]
fids = [1, 2, 3]
crop = "corn"

[[scenario.substitutions]]
fids = [10]
crop = "alfalfa"
"""
        toml_path = str(tmp_path / "test.toml")
        with open(toml_path, "w") as f:
            f.write(toml_content)

        spec = ScenarioSpec.from_toml(toml_path)

        assert spec.name == "test_scenario"
        assert spec.description == "Unit test scenario"
        assert len(spec.substitutions) == 4
        assert spec.substitutions[0].fid == "1"
        assert spec.substitutions[0].target_crop == "corn"
        assert spec.substitutions[3].fid == "10"
        assert spec.substitutions[3].target_crop == "alfalfa"


class TestScenarioSpecCsv:
    def test_parse_csv(self, tmp_path):
        csv_path = str(tmp_path / "subs.csv")
        pd.DataFrame(
            {"FID": [100, 200, 300], "crop": ["corn", "corn", "alfalfa"]}
        ).to_csv(csv_path, index=False)

        spec = ScenarioSpec.from_csv(
            csv_path=csv_path,
            library_path="/fake/lib.json",
            source_container="/fake/src.swim",
            name="csv_test",
        )

        assert spec.name == "csv_test"
        assert len(spec.substitutions) == 3
        assert spec.substitutions[0].fid == "100"
        assert spec.substitutions[2].target_crop == "alfalfa"


class TestScenarioSpecValidation:
    def setup_method(self):
        self.library = {
            "corn": {"curve_366": np.zeros(366)},
            "alfalfa": {"curve_366": np.zeros(366)},
        }
        self.container_fids = ["1", "2", "3", "4", "5"]

    def test_valid_scenario(self):
        spec = ScenarioSpec(
            name="valid",
            description="",
            crop_library_path="",
            source_container="",
            substitutions=[
                CropSubstitution(fid="1", target_crop="corn"),
                CropSubstitution(fid="2", target_crop="alfalfa"),
            ],
        )
        msgs = spec.validate(self.library, self.container_fids)
        assert not any(m.startswith("ERROR:") for m in msgs)

    def test_unknown_fid(self):
        spec = ScenarioSpec(
            name="bad_fid",
            description="",
            crop_library_path="",
            source_container="",
            substitutions=[
                CropSubstitution(fid="999", target_crop="corn"),
            ],
        )
        msgs = spec.validate(self.library, self.container_fids)
        errors = [m for m in msgs if m.startswith("ERROR:")]
        assert len(errors) == 1
        assert "999" in errors[0]

    def test_unknown_crop(self):
        spec = ScenarioSpec(
            name="bad_crop",
            description="",
            crop_library_path="",
            source_container="",
            substitutions=[
                CropSubstitution(fid="1", target_crop="quinoa"),
            ],
        )
        msgs = spec.validate(self.library, self.container_fids)
        errors = [m for m in msgs if m.startswith("ERROR:")]
        assert len(errors) == 1
        assert "quinoa" in errors[0]

    def test_duplicate_fid_warning(self):
        spec = ScenarioSpec(
            name="dup",
            description="",
            crop_library_path="",
            source_container="",
            substitutions=[
                CropSubstitution(fid="1", target_crop="corn"),
                CropSubstitution(fid="1", target_crop="alfalfa"),
            ],
        )
        msgs = spec.validate(self.library, self.container_fids)
        warns = [m for m in msgs if m.startswith("WARN:")]
        assert any("duplicate" in w.lower() for w in warns)

    def test_empty_substitutions_warning(self):
        spec = ScenarioSpec(
            name="empty",
            description="",
            crop_library_path="",
            source_container="",
            substitutions=[],
        )
        msgs = spec.validate(self.library, self.container_fids)
        warns = [m for m in msgs if m.startswith("WARN:")]
        assert any("no substitutions" in w.lower() for w in warns)


# ---------------------------------------------------------------------------
# Integration test: create_scenario_container
# ---------------------------------------------------------------------------

# The fixture shapefile has 1 field (site_id column).  We need a shapefile
# with multiple fields to test selective NDVI overwrite.  Build one
# synthetically so the test doesn't depend on Tongue data.


def _build_source_container(tmp_path):
    """Build a minimal source container with 3 fields and irr/inv_irr NDVI."""
    os.makedirs(tmp_path, exist_ok=True)

    # Create a 3-field shapefile using fiona directly
    shp_path = str(tmp_path / "fields.shp")
    schema = {"geometry": "Point", "properties": {"FID": "str"}}
    with fiona.open(
        shp_path, "w", driver="ESRI Shapefile", schema=schema, crs=CRS.from_epsg(4326)
    ) as dst:
        for fid, x in zip(["10", "20", "30"], [1.0, 2.0, 3.0]):
            dst.write(
                {
                    "geometry": {"type": "Point", "coordinates": (x, 45.0)},
                    "properties": {"FID": fid},
                }
            )

    container_path = str(tmp_path / "source.swim")
    container = SwimContainer.create(
        container_path,
        fields_shapefile=shp_path,
        uid_column="FID",
        start_date="2020-01-01",
        end_date="2020-12-31",
        project_name="test_source",
    )

    n_days = container.n_days
    n_fields = container.n_fields
    assert n_fields == 3

    # Properties (required for copy_static_groups UID check to pass)
    awc = container._create_property_array("properties/soils/awc")
    awc[:] = np.full(n_fields, 150.0, dtype=np.float32)

    ksat = container._create_property_array("properties/soils/ksat")
    ksat[:] = np.full(n_fields, 10.0, dtype=np.float32)

    modis = container._create_property_array(
        "properties/land_cover/modis_lc", dtype="int16", fill_value=-1
    )
    modis[:] = np.full(n_fields, 12, dtype=np.int16)

    # Irrigation properties (needed for compute_irr_data)
    irr = container._create_property_array("properties/irrigation/irr")
    irr[:] = np.array([0.8, 0.9, 0.1], dtype=np.float32)

    # Per-year irrigation fractions (irr_yearly) — needed by
    # compute_irr_data(use_mask=True) to classify irrigated years
    import json
    from zarr.core.dtype import VariableLengthUTF8

    irr_yearly_grp = container._root.require_group("properties/irrigation")
    irr_yearly_arr = irr_yearly_grp.create_array(
        "irr_yearly",
        shape=(n_fields,),
        dtype=VariableLengthUTF8(),
    )
    # Fields 10 and 20 are irrigated (f_irr > 0.1), field 30 is not.
    # Format: {year_str: f_irr_float} — flat year→float mapping.
    irr_yearly_arr[0] = json.dumps({"2020": 0.8})
    irr_yearly_arr[1] = json.dumps({"2020": 0.9})
    irr_yearly_arr[2] = json.dumps({"2020": 0.05})

    # Meteorology (needed if we want to run the model later)
    for var, val in {
        "meteorology/gridmet/prcp": 1.0,
        "meteorology/gridmet/tmin": 5.0,
        "meteorology/gridmet/tmax": 20.0,
        "meteorology/gridmet/srad": 18.0,
        "meteorology/gridmet/eto": 4.0,
    }.items():
        arr = container._create_timeseries_array(var)
        arr[:] = np.full((n_days, n_fields), val, dtype=np.float32)

    # Merged NDVI — baseline 0.3 for all fields, both masks
    for mask in ("irr", "inv_irr"):
        arr = container._create_timeseries_array(f"derived/merged_ndvi/{mask}")
        arr[:] = np.full((n_days, n_fields), 0.3, dtype=np.float32)

    # Add a fake simulation run + default_restart_run_id to verify
    # that scenario stripping works
    sim_group = container._root.create_group("simulation/runs/fake_init")
    sim_group.attrs["run_id"] = "fake_init"
    sim_group.attrs["status"] = "completed"
    container._root.attrs["default_restart_run_id"] = "fake_init"

    container.save()
    container.close()
    return container_path


def _build_test_library(tmp_path):
    """Build a minimal crop library with a seasonal corn curve.

    The curve has a realistic shape (dormant winter → greenup → peak → senescence)
    so the NDVI slope detector can identify irrigation windows.  Peak ~0.75
    around DOY 200, dormant ~0.15 in winter.
    """
    doys = np.arange(1, 367)
    # Gaussian bell: peak 0.75 at DOY 200, sigma 40 days, floor 0.15
    curve = 0.15 + 0.60 * np.exp(-0.5 * ((doys - 200) / 40) ** 2)

    library = {
        "corn": {
            "curve_366": curve.tolist(),
            "p25_366": (curve - 0.05).tolist(),
            "p75_366": (curve + 0.05).tolist(),
            "n_profiles": 100,
            "cdl_codes": [1],
            "phenology": {
                "greenup_doy": 120,
                "peak_ndvi": 0.75,
                "peak_doy": 200,
                "senescence_doy": 270,
                "season_length": 150,
            },
        }
    }
    lib_path = str(tmp_path / "library.json")
    with open(lib_path, "w") as f:
        json.dump(library, f)
    return lib_path


class TestCreateScenarioContainer:
    """Integration tests for create_scenario_container."""

    def test_substitution_overwrites_only_targeted_field(self, tmp_path):
        """Verify that only the substituted field's NDVI changes."""
        from swim_mtdnrc.scenarios.scenario_container import (
            create_scenario_container,
        )

        source_path = _build_source_container(tmp_path / "source")
        lib_path = _build_test_library(tmp_path)
        output_path = str(tmp_path / "scenario.swim")

        spec = ScenarioSpec(
            name="test",
            description="test scenario",
            crop_library_path=lib_path,
            source_container=source_path,
            substitutions=[
                CropSubstitution(fid="20", target_crop="corn"),
            ],
        )

        create_scenario_container(spec, output_path)

        # Open and verify
        result = SwimContainer.open(output_path, mode="r")
        try:
            irr = result._root["derived/merged_ndvi/irr"][:]
            inv_irr = result._root["derived/merged_ndvi/inv_irr"][:]

            # Field 10 (idx 0) should be unchanged (baseline 0.3)
            np.testing.assert_allclose(irr[:, 0], 0.3, atol=0.01)
            # Field 30 (idx 2) should be unchanged
            np.testing.assert_allclose(irr[:, 2], 0.3, atol=0.01)

            # Field 20 (idx 1) should be overwritten with the seasonal
            # corn curve — NOT the constant 0.3 baseline.  The peak
            # (around DOY 200) should be ~0.75, not 0.3.
            assert np.max(irr[:, 1]) > 0.6
            assert not np.allclose(irr[:, 1], 0.3, atol=0.01)

            # Same for inv_irr
            assert np.max(inv_irr[:, 1]) > 0.6
            np.testing.assert_allclose(inv_irr[:, 0], 0.3, atol=0.01)
        finally:
            result.close()

    def test_no_stale_simulation_runs(self, tmp_path):
        """Scenario container must not contain source simulation runs or
        a default_restart_run_id."""
        from swim_mtdnrc.scenarios.scenario_container import (
            create_scenario_container,
        )

        source_path = _build_source_container(tmp_path / "source")
        lib_path = _build_test_library(tmp_path)
        output_path = str(tmp_path / "scenario.swim")

        spec = ScenarioSpec(
            name="test",
            description="",
            crop_library_path=lib_path,
            source_container=source_path,
            substitutions=[
                CropSubstitution(fid="10", target_crop="corn"),
            ],
        )

        create_scenario_container(spec, output_path)

        result = SwimContainer.open(output_path, mode="r")
        try:
            # No simulation runs should exist
            assert "simulation/runs/fake_init" not in result._root
            # No default restart should be set
            assert "default_restart_run_id" not in result._root.attrs
        finally:
            result.close()

    def test_irr_data_recomputed_from_modified_ndvi(self, tmp_path):
        """Verify that irr_data reflects the substituted NDVI curve.

        The substituted field (FID 20) has a seasonal corn curve with real
        slopes, so the NDVI slope detector should find irrigation windows.
        The baseline field (FID 10) has constant NDVI=0.3 (zero slope), so
        the detector should find no windows from slope analysis.  Both are
        irrigated per irr_yearly, so any difference in irr_doys is caused
        by the NDVI overwrite.
        """
        from swim_mtdnrc.scenarios.scenario_container import (
            create_scenario_container,
        )

        source_path = _build_source_container(tmp_path / "source")
        lib_path = _build_test_library(tmp_path)
        output_path = str(tmp_path / "scenario.swim")

        spec = ScenarioSpec(
            name="test",
            description="",
            crop_library_path=lib_path,
            source_container=source_path,
            substitutions=[
                CropSubstitution(fid="20", target_crop="corn"),
            ],
        )

        create_scenario_container(spec, output_path)

        result = SwimContainer.open(output_path, mode="r")
        try:
            assert "derived/dynamics/irr_data" in result._root
            irr_data = result._root["derived/dynamics/irr_data"][:]
            assert irr_data.shape == (3,)

            field_10 = json.loads(irr_data[0])  # baseline, constant NDVI=0.3
            field_20 = json.loads(irr_data[1])  # substituted, seasonal corn

            # Both irrigated per irr_yearly
            assert field_10["2020"]["irrigated"] == 1
            assert field_20["2020"]["irrigated"] == 1

            # The seasonal corn curve (field 20) has NDVI slopes that the
            # detector can use, so its irr_doys should differ from the
            # constant-NDVI baseline (field 10).  This proves irr_data was
            # recomputed from the overwritten NDVI, not just copied or
            # derived solely from irr_yearly.
            doys_10 = field_10["2020"]["irr_doys"]
            doys_20 = field_20["2020"]["irr_doys"]
            assert doys_10 != doys_20, (
                "irr_doys should differ between constant-NDVI baseline "
                "and seasonal corn curve — irr_data may not reflect "
                "the overwritten NDVI"
            )
        finally:
            result.close()

    def test_validation_rejects_bad_fid(self, tmp_path):
        """Scenario with a FID not in the container should raise."""
        from swim_mtdnrc.scenarios.scenario_container import (
            create_scenario_container,
        )

        source_path = _build_source_container(tmp_path / "source")
        lib_path = _build_test_library(tmp_path)
        output_path = str(tmp_path / "scenario.swim")

        spec = ScenarioSpec(
            name="bad",
            description="",
            crop_library_path=lib_path,
            source_container=source_path,
            substitutions=[
                CropSubstitution(fid="999", target_crop="corn"),
            ],
        )

        with pytest.raises(ValueError, match="validation failed"):
            create_scenario_container(spec, output_path)
