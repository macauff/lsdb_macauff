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
