import pandas as pd
from lsdb.core.crossmatch.abstract_crossmatch_algorithm import \
    AbstractCrossmatchAlgorithm
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
        left_pixel_params = PixelParams(
            self.left_order, self.left_pixel, left_tri_map_histogram
        )
        right_pixel_params = PixelParams(
            self.right_order, self.right_pixel, right_tri_map_histogram
        )
        all_macauff_attrs = AllMacauffAttrs(
            joint_all_sky_params,
            macauff_pixel_params,
            left_all_sky_params,
            right_all_sky_params,
            left_pixel_params,
            right_pixel_params,
        )
        # Apply astrometric corrections?

        # get the dataframes
        a_astro, a_photo, a_magref = self.make_data_arrays(
            self.left, self.left_metadata, left_all_sky_params
        )

        all_macauff_attrs.a_astro = a_astro
        all_macauff_attrs.a_photo = a_photo
        all_macauff_attrs.a_magref = a_magref

        b_astro, b_photo, b_magref = self.make_data_arrays(
            self.right, self.right_metadata, right_all_sky_params
        )

        all_macauff_attrs.b_astro = b_astro
        all_macauff_attrs.b_photo = b_photo
        all_macauff_attrs.b_magref = b_magref

        macauff = Macauff(all_macauff_attrs)
        macauff()
        return self.make_joint_dataframe(all_macauff_attrs)

    def make_data_arrays(self, data, metadata, params):
        """Creates the astro, photo, and magref arrays for a given catalog's dataset."""
        ra_column = metadata.ra_column
        dec_column = metadata.dec_column
        uncertainty_column = params.uncertainty_column_name
        astro = data[[ra_column, dec_column, uncertainty_column]].to_numpy()

        photo = data[params.filt_names].to_numpy()

        magref = data[params.magref_column_name].to_numpy()

        return astro, photo, magref

    def make_joint_dataframe(self, macauff_attrs: AllMacauffAttrs) -> pd.DataFrame:
        """Creates the resulting crossmatch pandas Dataframe"""
        raise NotImplementedError()
