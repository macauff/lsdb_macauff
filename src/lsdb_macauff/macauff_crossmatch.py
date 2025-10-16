import pandas as pd
from lsdb.core.crossmatch.abstract_crossmatch_algorithm import AbstractCrossmatchAlgorithm

from lsdb_macauff.all_macauff_attrs import AllMacauffAttrs
from lsdb_macauff.config.pixel_params import PixelParams

from lsdb.catalog import Catalog
from lsdb_macauff.macauff_matching import MacauffMatching
import numpy as np

from hats.pixel_math.healpix_pixel import HealpixPixel

# from macauff.macauff import Macauff


class MacauffCrossmatch(AbstractCrossmatchAlgorithm):
    """Class that runs the Macauff crossmatch"""

    extra_columns = pd.DataFrame({
            'p': pd.Series(dtype=np.float64),
            'eta': pd.Series(dtype=np.float64),
            'xi': pd.Series(dtype=np.float64),
            'a_avg_cont': pd.Series(dtype=np.float64),
            'b_avg_cont': pd.Series(dtype=np.float64),
            'seps': pd.Series(dtype=np.float64),
        })

    @classmethod
    def validate(
            cls,
            left: Catalog,
            right: Catalog,
            macauff: MacauffMatching,
    ):
        super().validate(left, right)

    def perform_crossmatch(
            self,
            macauff: MacauffMatching,
    ) -> tuple[np.ndarray, np.ndarray, pd.DataFrame]:
        """Perform a cross-match between the data from two HEALPix pixels

        Finds the n closest neighbors in the right catalog for each point in the left catalog that
        are within a threshold distance by using a K-D Tree.

        Args:
            n_neighbors (int): The number of neighbors to find within each point.
            radius_arcsec (float): The threshold distance in arcseconds beyond which neighbors are not added

        Returns:
            Indices of the matching rows from the left and right tables found from cross-matching, and a
            datafame with the "_dist_arcsec" column with the great circle separation between the points.
        """
        return macauff(self.left, self.right, HealpixPixel(self.left_order, self.left_pixel), HealpixPixel(self.right_order, self.right_pixel), HealpixPixel(self.left_order, self.left_pixel) if self.left_order > self.right_order else HealpixPixel(self.right_order, self.right_pixel))
