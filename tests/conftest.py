import os

import pytest
from dask.distributed import Client

from lsdb_macauff.config import AllSkyParams, CatalogAllSkyParams, load_config_from_file

# pylint: disable=missing-function-docstring, redefined-outer-name


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


@pytest.fixture
def catwise_dir(test_data_dir):
    return os.path.join(test_data_dir, "catwise_hats_data")


@pytest.fixture
def gaia_dir(test_data_dir):
    return os.path.join(test_data_dir, "gaia_hats_data")


@pytest.fixture(scope="session", name="dask_client")
def dask_client():
    """Create a single client for use by all unit test cases."""
    client = Client()
    yield client
    client.close()


def pytest_collection_modifyitems(items):
    """Modify dask unit tests to
        - ignore event loop deprecation warnings
        - have a longer timeout default timeout (5 seconds instead of 1 second)
        - require use of the `dask_client` fixture, even if it's not requsted

    Individual tests that will be particularly long-running can still override
    the default timeout, by using an annotation like:

        @pytest.mark.dask(timeout=10)
        def test_long_running():
            ...
    """
    first_dask = True
    for item in items:
        timeout = None
        for mark in item.iter_markers(name="dask"):
            timeout = 5
            if "timeout" in mark.kwargs:
                timeout = int(mark.kwargs.get("timeout"))
        if timeout:
            if first_dask:
                ## The first test requires more time to set up the dask/ray client
                timeout += 10
                first_dask = False
            item.add_marker(pytest.mark.timeout(timeout))
            item.add_marker(pytest.mark.usefixtures("dask_client"))
            item.add_marker(pytest.mark.filterwarnings("ignore::DeprecationWarning"))


@pytest.fixture
def import_metadata_dir(test_data_dir):
    return os.path.join(test_data_dir, "import_pipeline", "metadata")


@pytest.fixture
def import_metadata_yaml(test_data_dir):
    return os.path.join(test_data_dir, "import_pipeline", "catalog_a_b_matches.yaml")


@pytest.fixture
def import_catalog_a(test_data_dir):
    return os.path.join(test_data_dir, "import_pipeline", "catalog_a")


@pytest.fixture
def import_catalog_b(test_data_dir):
    return os.path.join(test_data_dir, "import_pipeline", "catalog_b")


@pytest.fixture
def import_match_csv(test_data_dir):
    return os.path.join(test_data_dir, "import_pipeline", "catalog_a_b_matches.csv")
