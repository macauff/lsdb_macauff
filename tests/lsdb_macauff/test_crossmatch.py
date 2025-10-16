import lsdb

from lsdb_macauff.macauff_crossmatch import MacauffCrossmatch
from lsdb_macauff.macauff_matching import MacauffMatching


def test_macauff_xmatch_class():
    left_cat = lsdb.read_hats("../../lsdb_data/gaia_cone")
    right_cat = lsdb.read_hats("../../lsdb_data/catwise_cone")
    left_pix = left_cat.get_healpix_pixels()[0]
    right_pix = right_cat.get_healpix_pixels()[0]
    left_partition = left_cat.get_partition(left_pix.order, left_pix.pixel).compute()
    right_partition = right_cat.get_partition(right_pix.order, right_pix.pixel).compute()
    algo = MacauffMatching("../../test_data/gaia_wise_joint_params.yaml", "../../test_data/gaia_params.yaml", "../../test_data/wise_params.yaml")
    left_indices, right_indices, extras = algo(left_partition, right_partition, left_pix, right_pix, left_pix if left_pix.order > right_pix.order else right_pix)
    print(left_indices)
    print(extras)


def test_macauff_xmatch():
    left_cat = lsdb.read_hats("../../lsdb_data/gaia_cone")
    right_cat = lsdb.read_hats("../../lsdb_data/catwise_cone")
    algo = MacauffMatching("../../test_data/gaia_wise_joint_params.yaml", "../../test_data/gaia_params.yaml", "../../test_data/wise_params.yaml")
    xmatch = left_cat.crossmatch(right_cat, algorithm=MacauffCrossmatch, macauff=algo, suffixes=("_gaia", "_wise"))
    print(xmatch)
    print(xmatch.compute())
