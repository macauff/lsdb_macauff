import os

import lsdb
import pandas as pd
import pytest
from dask.distributed import Client, LocalCluster

# pylint: disable=missing-function-docstring, redefined-outer-name


@pytest.fixture
def test_data_dir():
    return os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def gaia_params_path(test_data_dir):
    return os.path.join(test_data_dir, "gaia_params.yaml")


@pytest.fixture
def wise_params_path(test_data_dir):
    return os.path.join(test_data_dir, "wise_params.yaml")


@pytest.fixture
def gaia_wise_joint_params_path(test_data_dir):
    return os.path.join(test_data_dir, "gaia_wise_joint_params.yaml")


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
def catwise_collection_dir(test_data_dir):
    return os.path.join(test_data_dir, "catwise_hats_data")


@pytest.fixture
def catwise_dir(catwise_collection_dir):
    return os.path.join(catwise_collection_dir, "catwise_cone")


@pytest.fixture
def gaia_collection_dir(test_data_dir):
    return os.path.join(test_data_dir, "gaia_hats_data")


@pytest.fixture
def gaia_dir(gaia_collection_dir):
    return os.path.join(gaia_collection_dir, "gaia_cone")


@pytest.fixture
def catwise_cat(catwise_collection_dir):
    return lsdb.open_catalog(catwise_collection_dir)


@pytest.fixture
def gaia_cat(gaia_collection_dir):
    return lsdb.open_catalog(gaia_collection_dir)


@pytest.fixture
def expected_gaia_wise_xmatch_df(test_data_dir):
    return pd.read_csv(os.path.join(test_data_dir, "gaiadr3_catwise_matches_0.csv"), dtype_backend="pyarrow")


@pytest.fixture(scope="session", name="dask_client")
def dask_client():
    """Create a single client for use by all unit test cases."""
    cluster = LocalCluster(n_workers=1, threads_per_worker=1, dashboard_address=":0")
    client = Client(cluster)
    yield client
    client.close()
    cluster.close()


def pytest_collection_modifyitems(items):
    """Modify dask unit tests to
    - ignore event loop deprecation warnings
    - require use of the `dask_client` fixture, even if it's not requsted
    """
    for item in items:
        is_dask = False
        if item.iter_markers(name="dask"):
            is_dask = True
        if is_dask:
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
