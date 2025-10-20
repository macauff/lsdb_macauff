import numpy.testing as npt

from lsdb_macauff.macauff_crossmatch import MacauffCrossmatch
from lsdb_macauff.macauff_setup import MacauffSetup
from macauff import CrossMatch


def test_macauff_setup_loading(gaia_params_path, wise_params_path, gaia_wise_joint_params_path):
    lsdb_macauff_setup = MacauffSetup(gaia_wise_joint_params_path, gaia_params_path, wise_params_path)
    macauff_crossmatch = CrossMatch(gaia_wise_joint_params_path, gaia_params_path, wise_params_path, use_mpi=False)
    npt.assert_equal(lsdb_macauff_setup.crossmatch_params_dict, macauff_crossmatch.crossmatch_params_dict)
    npt.assert_equal(lsdb_macauff_setup.cat_a_params_dict, macauff_crossmatch.cat_a_params_dict)
    npt.assert_equal(lsdb_macauff_setup.cat_b_params_dict, macauff_crossmatch.cat_b_params_dict)


def test_macauff_xmatch(gaia_cat, catwise_cat, gaia_params_path, wise_params_path, gaia_wise_joint_params_path):
    macauff_setup = MacauffSetup(gaia_wise_joint_params_path, gaia_params_path, wise_params_path)
    xmatch = gaia_cat.crossmatch(catwise_cat, algorithm=MacauffCrossmatch, macauff=macauff_setup, suffixes=("_gaia", "_wise"))
    print(xmatch)
    print(xmatch.compute())
