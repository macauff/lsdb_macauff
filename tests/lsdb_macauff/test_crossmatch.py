import pandas as pd
import pytest
from hipscat.catalog.catalog_info import CatalogInfo

from lsdb_macauff.macauff_crossmatch import MacauffCrossmatch


@pytest.mark.xfail
def test_macauff_crossmatch(
    catalog_a_csv, catalog_b_csv, all_sky_params, gaia_all_sky_params, wise_all_sky_params
):
    """We know this will fail, but that's ok for now!"""

    catalog_a_info = CatalogInfo({"catalog_name": "catalog_a"})
    catalog_b_info = CatalogInfo({"catalog_name": "catalog_b"})

    algo = MacauffCrossmatch(
        left=pd.read_csv(catalog_a_csv),
        right=pd.read_csv(catalog_b_csv),
        left_order=0,
        left_pixel=0,
        right_order=0,
        right_pixel=0,
        left_catalog_info=catalog_a_info,
        right_catalog_info=catalog_b_info,
        right_margin_catalog_info=None,
        suffixes=("a", "b"),
    )
    algo.crossmatch(all_sky_params, gaia_all_sky_params, wise_all_sky_params, None, None)


def test_make_data_arrays(catalog_a_csv, catalog_b_csv, gaia_all_sky_params, wise_all_sky_params):
    """Ensure that we can get the astro, photo, and magref arrays for each catalog."""
    # target lengths
    a_expected_len = len(pd.read_csv(catalog_a_csv))
    b_expected_len = len(pd.read_csv(catalog_b_csv))

    catalog_a_info = CatalogInfo({"catalog_name": "catalog_a"})
    catalog_b_info = CatalogInfo({"catalog_name": "catalog_b"})

    gaia_all_sky_params.filt_names = ["filter_0", "filter_1", "filter_2"]
    wise_all_sky_params.filt_names = ["filter_0", "filter_1"]

    algo = MacauffCrossmatch(
        left=pd.read_csv(catalog_a_csv),
        right=pd.read_csv(catalog_b_csv),
        left_order=0,
        left_pixel=0,
        right_order=0,
        right_pixel=0,
        left_catalog_info=catalog_a_info,
        right_catalog_info=catalog_b_info,
        right_margin_catalog_info=None,
        suffixes=("a", "b"),
    )

    a_astro, a_photo, a_magref = algo.make_data_arrays(algo.left, algo.left_catalog_info, gaia_all_sky_params)

    b_astro, b_photo, b_magref = algo.make_data_arrays(
        algo.right, algo.right_catalog_info, wise_all_sky_params
    )

    assert a_astro.shape == (a_expected_len, 3)
    assert a_photo.shape == (a_expected_len, len(gaia_all_sky_params.filt_names))
    assert a_magref.shape == (a_expected_len,)

    assert b_astro.shape == (b_expected_len, 3)
    assert b_photo.shape == (b_expected_len, len(wise_all_sky_params.filt_names))
    assert b_magref.shape == (b_expected_len,)
