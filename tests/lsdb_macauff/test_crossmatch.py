import pandas as pd
import pytest
from hipscat.catalog import Catalog
from hipscat.catalog.catalog_info import CatalogInfo
from hipscat.pixel_math import HealpixPixel

from lsdb_macauff.macauff_crossmatch import MacauffCrossmatch


@pytest.mark.xfail
def test_macauff_crossmatch(
    catalog_a_csv, catalog_b_csv, all_sky_params, gaia_all_sky_params, wise_all_sky_params
):
    """We know this will fail, but that's ok for now!"""

    catalog_a = Catalog(CatalogInfo({"catalog_name": "catalog_a"}), [HealpixPixel(0, 0)])
    catalog_b = Catalog(CatalogInfo({"catalog_name": "catalog_b"}), [HealpixPixel(0, 0)])

    algo = MacauffCrossmatch(
        left=pd.read_csv(catalog_a_csv),
        right=pd.read_csv(catalog_b_csv),
        left_order=0,
        left_pixel=0,
        right_order=0,
        right_pixel=0,
        left_metadata=catalog_a,
        right_metadata=catalog_b,
        suffixes=("a", "b"),
    )
    algo.crossmatch(all_sky_params, gaia_all_sky_params, wise_all_sky_params, None, None)

def test_make_data_arrays(
        catalog_a_csv, catalog_b_csv, all_sky_params, gaia_all_sky_params, wise_all_sky_params
):
    """Ensure that we can get the astro, photo, and magref arrays for each catalog."""
    # target lengths
    a_expected_len = len(pd.read_csv(catalog_a_csv))
    b_expected_len = len(pd.read_csv(catalog_b_csv))

    catalog_a = Catalog(CatalogInfo({"catalog_name": "catalog_a"}), [HealpixPixel(0, 0)])
    catalog_b = Catalog(CatalogInfo({"catalog_name": "catalog_b"}), [HealpixPixel(0, 0)])

    gaia_all_sky_params.filt_names = ["filter_0", "filter_1", "filter_2"]
    wise_all_sky_params.filt_names = ["filter_0", "filter_1"]

    algo = MacauffCrossmatch(
        left=pd.read_csv(catalog_a_csv),
        right=pd.read_csv(catalog_b_csv),
        left_order=0,
        left_pixel=0,
        right_order=0,
        right_pixel=0,
        left_metadata=catalog_a,
        right_metadata=catalog_b,
        suffixes=("a", "b"),
    )

    a_astro, a_photo, a_magref = algo.make_data_arrays(
        algo.left, algo.left_metadata, gaia_all_sky_params
    )

    b_astro, b_photo, b_magref = algo.make_data_arrays(
        algo.right, algo.right_metadata, wise_all_sky_params
    )

    assert a_astro.shape == (a_expected_len, 3)
    assert a_photo.shape == (a_expected_len, len(gaia_all_sky_params.filt_names))
    assert a_magref.shape == (a_expected_len,)

    assert b_astro.shape == (b_expected_len, 3)
    assert b_photo.shape == (b_expected_len, len(wise_all_sky_params.filt_names))
    assert b_magref.shape == (b_expected_len,)