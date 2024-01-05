import os

import pytest

from lsdb_macauff.config import AllSkyParams, CatalogAllSkyParams, load_config_from_file


@pytest.fixture
def test_data_dir():
    return os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def all_sky_params(test_data_dir):
    return load_config_from_file(AllSkyParams, os.path.join(test_data_dir, "all_sky_params.yml"))


@pytest.fixture
def gaia_all_sky_params(test_data_dir):
    return load_config_from_file(CatalogAllSkyParams, os.path.join(test_data_dir, "gaia_all_sky_params.yml"))


@pytest.fixture
def wise_all_sky_params(test_data_dir):
    return load_config_from_file(CatalogAllSkyParams, os.path.join(test_data_dir, "wise_all_sky_params.yml"))


@pytest.fixture
def catalog_a_csv(test_data_dir):
    return os.path.join(test_data_dir, "catalog_a.csv")


@pytest.fixture
def catalog_b_csv(test_data_dir):
    return os.path.join(test_data_dir, "catalog_b.csv")


@pytest.fixture
def counterpart_csv(test_data_dir):
    return os.path.join(test_data_dir, "counters.csv")
