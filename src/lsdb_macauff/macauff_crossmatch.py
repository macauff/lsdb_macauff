import numpy as np
import pandas as pd
import pyarrow as pa
from hats.pixel_math.healpix_pixel import HealpixPixel
from lsdb.catalog import Catalog
from lsdb.core.crossmatch.abstract_crossmatch_algorithm import AbstractCrossmatchAlgorithm

from lsdb_macauff.macauff_setup import MacauffSetup


class MacauffCrossmatch(AbstractCrossmatchAlgorithm):
    """Class that runs the Macauff crossmatch"""

    extra_columns = pd.DataFrame(
        {
            "p": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
            "eta": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
            "xi": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
            "a_avg_cont": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
            "b_avg_cont": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
            "a_cont_f1": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
            "a_cont_f10": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
            "b_cont_f1": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
            "b_cont_f10": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
            "sep": pd.Series(dtype=pd.ArrowDtype(pa.float64())),
        }
    )

    @classmethod
    def validate(
        cls,
        left: Catalog,
        right: Catalog,
        macauff_setup: MacauffSetup,
    ):
        super().validate(left, right)

    def perform_crossmatch(
        self,
        macauff_setup: MacauffSetup,
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
        l_inds, r_inds, extra_cols = macauff_setup(
            self.left,
            self.right,
            HealpixPixel(self.left_order, self.left_pixel)
            if self.left_order > self.right_order
            else HealpixPixel(self.right_order, self.right_pixel),
        )
        extra_cols = extra_cols.convert_dtypes(dtype_backend="pyarrow", convert_integer=False)
        return l_inds, r_inds, extra_cols
