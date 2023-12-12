from lsdb_macauff.config import AllSkyParams, CatalogAllSkyParams


def test_load_config_files(all_sky_params, gaia_all_sky_params, wise_all_sky_params):
    """Verify that we can load the Macauff configuration files"""
    assert isinstance(all_sky_params, AllSkyParams)
    assert isinstance(gaia_all_sky_params, CatalogAllSkyParams)
    assert isinstance(wise_all_sky_params, CatalogAllSkyParams)
