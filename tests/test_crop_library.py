"""Tests for crop_library module."""

import os

import numpy as np

from swim_mtdnrc.clustering.crop_library import (
    FULL_YEAR_DAYS,
    GROWING_SEASON_DAYS,
    DOY_START,
    DOY_END,
    _extend_to_full_year,
    _filter_profiles_by_cdl,
    build_crop_library,
    load_crop_library,
)


class TestExtendToFullYear:
    def test_output_shape(self):
        profile = np.linspace(0.2, 0.7, GROWING_SEASON_DAYS)
        result = _extend_to_full_year(profile, smooth_window=0)
        assert result.shape == (FULL_YEAR_DAYS,)

    def test_growing_season_preserved_no_smooth(self):
        profile = np.linspace(0.2, 0.7, GROWING_SEASON_DAYS)
        result = _extend_to_full_year(profile, smooth_window=0)
        np.testing.assert_array_equal(result[DOY_START - 1 : DOY_END], profile)

    def test_winter_padded_no_smooth(self):
        profile = np.linspace(0.2, 0.7, GROWING_SEASON_DAYS)
        result = _extend_to_full_year(profile, smooth_window=0)
        # Jan-Mar should equal Apr 1 value
        assert np.all(result[: DOY_START - 1] == profile[0])
        # Nov-Dec should equal Oct 31 value
        assert np.all(result[DOY_END:] == profile[-1])

    def test_smoothing_modifies_seams(self):
        profile = np.ones(GROWING_SEASON_DAYS) * 0.6
        profile[0] = 0.2  # create a jump at spring seam
        no_smooth = _extend_to_full_year(profile, smooth_window=0)
        smoothed = _extend_to_full_year(profile, smooth_window=7)
        # The seam region should differ when smoothing is applied
        seam_idx = DOY_START - 1
        assert no_smooth[seam_idx] != smoothed[seam_idx]

    def test_no_nan_in_output(self):
        profile = np.random.uniform(0.1, 0.8, GROWING_SEASON_DAYS)
        result = _extend_to_full_year(profile)
        assert not np.any(np.isnan(result))

    def test_constant_profile_unchanged(self):
        """A flat profile should produce a flat full year (no seam artifacts)."""
        profile = np.ones(GROWING_SEASON_DAYS) * 0.4
        result = _extend_to_full_year(profile, smooth_window=7)
        np.testing.assert_allclose(result, 0.4, atol=0.01)


class TestFilterProfilesByCdl:
    def setup_method(self):
        import pandas as pd

        self.profiles = np.random.rand(6, GROWING_SEASON_DAYS)
        self.labels = [
            "100_2010",
            "100_2011",
            "200_2010",
            "200_2011",
            "300_2010",
            "300_2011",
        ]
        self.cdl_long = pd.DataFrame(
            {
                "fid_year": [
                    "100_2010",
                    "100_2011",
                    "200_2010",
                    "200_2011",
                    "300_2010",
                    "300_2011",
                ],
                "FID": [100, 100, 200, 200, 300, 300],
                "year": [2010, 2011, 2010, 2011, 2010, 2011],
                "crop_code": [36, 36, 1, 1, 171, 171],
            }
        )

    def test_single_code_filter(self):
        result = _filter_profiles_by_cdl(
            self.profiles, self.labels, self.cdl_long, [36]
        )
        assert result.shape == (2, GROWING_SEASON_DAYS)
        np.testing.assert_array_equal(result, self.profiles[:2])

    def test_multi_code_filter(self):
        result = _filter_profiles_by_cdl(
            self.profiles, self.labels, self.cdl_long, [1, 171]
        )
        assert result.shape == (4, GROWING_SEASON_DAYS)

    def test_no_match_returns_empty(self):
        result = _filter_profiles_by_cdl(
            self.profiles, self.labels, self.cdl_long, [999]
        )
        assert result.shape == (0, GROWING_SEASON_DAYS)

    def test_labels_not_in_cdl_excluded(self):
        extra_profiles = np.random.rand(8, GROWING_SEASON_DAYS)
        extra_labels = self.labels + ["400_2010", "400_2011"]
        result = _filter_profiles_by_cdl(
            extra_profiles, extra_labels, self.cdl_long, [36]
        )
        assert result.shape == (2, GROWING_SEASON_DAYS)


class TestBuildAndLoadLibrary:
    def test_library_round_trip(self, tmp_path):
        """Build a library from synthetic data and reload it."""
        # Create synthetic NDVI CSVs
        import pandas as pd

        ndvi_dir = str(tmp_path / "ndvi")
        os.makedirs(ndvi_dir)

        np.random.seed(42)
        fids = list(range(1, 51))

        for year in [2010, 2011, 2012]:
            # Generate 10 scene dates in growing season
            dates = pd.date_range(f"{year}-04-15", f"{year}-09-30", periods=10)
            cols = {"FID": fids}
            for d in dates:
                scene_id = f"LE07_035028_{d.strftime('%Y%m%d')}"
                cols[scene_id] = np.random.uniform(0.15, 0.75, len(fids))
            df = pd.DataFrame(cols)
            df.to_csv(os.path.join(ndvi_dir, f"ndvi_{year}_irr.csv"), index=False)

        # Create synthetic CDL CSV
        cdl_path = str(tmp_path / "cdl.csv")
        cdl_data = {"FID": fids}
        for year in [2010, 2011, 2012]:
            # First 20 fields = alfalfa (36), next 15 = corn (1), rest = grass (171)
            codes = [36] * 20 + [1] * 15 + [171] * 15
            cdl_data[f"crop_{year}"] = codes
        pd.DataFrame(cdl_data).to_csv(cdl_path, index=False)

        # Build library
        output_path = str(tmp_path / "library.json")
        library = build_crop_library(
            ndvi_dir=ndvi_dir,
            cdl_csv=cdl_path,
            output_path=output_path,
            min_profiles=10,
            min_scenes=3,
        )

        assert "alfalfa" in library
        assert "corn" in library
        assert "grass_pasture" in library
        assert library["alfalfa"]["n_profiles"] >= 10
        assert len(library["alfalfa"]["curve_366"]) == FULL_YEAR_DAYS

        # Reload and verify
        loaded = load_crop_library(output_path)
        assert set(loaded.keys()) == set(library.keys())
        for crop in loaded:
            assert loaded[crop]["curve_366"].shape == (FULL_YEAR_DAYS,)
            np.testing.assert_allclose(
                loaded[crop]["curve_366"],
                np.array(library[crop]["curve_366"]),
            )

    def test_min_profiles_excludes_rare_crops(self, tmp_path):
        """Crops with too few profiles should be excluded."""
        import pandas as pd

        ndvi_dir = str(tmp_path / "ndvi")
        os.makedirs(ndvi_dir)

        fids = list(range(1, 11))
        for year in [2010]:
            dates = pd.date_range(f"{year}-05-01", f"{year}-09-15", periods=8)
            cols = {"FID": fids}
            for d in dates:
                scene_id = f"LE07_035028_{d.strftime('%Y%m%d')}"
                cols[scene_id] = np.random.uniform(0.2, 0.7, len(fids))
            pd.DataFrame(cols).to_csv(
                os.path.join(ndvi_dir, f"ndvi_{year}_irr.csv"), index=False
            )

        cdl_path = str(tmp_path / "cdl.csv")
        # 8 alfalfa, 2 corn — corn won't meet threshold of 5
        pd.DataFrame({"FID": fids, "crop_2010": [36] * 8 + [1] * 2}).to_csv(
            cdl_path, index=False
        )

        library = build_crop_library(
            ndvi_dir=ndvi_dir,
            cdl_csv=cdl_path,
            output_path=None,
            min_profiles=5,
        )

        assert "alfalfa" in library
        assert "corn" not in library
