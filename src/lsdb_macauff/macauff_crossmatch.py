import pandas as pd
from lsdb.core.crossmatch.abstract_crossmatch_algorithm import AbstractCrossmatchAlgorithm
from macauff.macauff import Macauff

from lsdb_macauff.all_macauff_attrs import AllMacauffAttrs
from lsdb_macauff.config.pixel_params import PixelParams


class MacauffCrossmatch(AbstractCrossmatchAlgorithm):
    """Class that runs the Macauff crossmatch"""

    # pylint: disable=arguments-differ
    def crossmatch(
        self,
        joint_all_sky_params,
        left_all_sky_params,
        right_all_sky_params,
        left_tri_map_histogram,
        right_tri_map_histogram,
    ) -> pd.DataFrame:
        # Calculate macauff pixel params using self.left_order,
        # self.left_pixel, self.right_order, self.right_pixel
        macauff_pixel_params = None
        left_pixel_params = PixelParams(self.left_order, self.left_pixel, left_tri_map_histogram)
        right_pixel_params = PixelParams(self.right_order, self.right_pixel, right_tri_map_histogram)
        all_macauff_attrs = AllMacauffAttrs(
            joint_all_sky_params,
            macauff_pixel_params,
            left_all_sky_params,
            right_all_sky_params,
            left_pixel_params,
            right_pixel_params,
        )
        # Apply astrometric corrections?
        macauff = Macauff(all_macauff_attrs)
        macauff()
        return self.make_joint_dataframe(all_macauff_attrs)

    def make_joint_dataframe(self, macauff_attrs: AllMacauffAttrs) -> pd.DataFrame:
        """Creates the resulting crossmatch pandas Dataframe"""
        raise NotImplementedError()
