import numpy.testing as npt
import pandas as pd
from macauff import CrossMatch

from lsdb_macauff.macauff_crossmatch import MacauffCrossmatch
from lsdb_macauff.macauff_setup import MacauffSetup


def test_macauff_setup_loading(gaia_params_path, wise_params_path, gaia_wise_joint_params_path):
    lsdb_macauff_setup = MacauffSetup(gaia_wise_joint_params_path, gaia_params_path, wise_params_path)
    macauff_crossmatch = CrossMatch(
        gaia_wise_joint_params_path, gaia_params_path, wise_params_path, use_mpi=False
    )
    npt.assert_equal(lsdb_macauff_setup.crossmatch_params_dict, macauff_crossmatch.crossmatch_params_dict)
    npt.assert_equal(lsdb_macauff_setup.cat_a_params_dict, macauff_crossmatch.cat_a_params_dict)
    npt.assert_equal(lsdb_macauff_setup.cat_b_params_dict, macauff_crossmatch.cat_b_params_dict)


def test_macauff_xmatch(
    gaia_cat,
    catwise_cat,
    gaia_params_path,
    wise_params_path,
    gaia_wise_joint_params_path,
    expected_gaia_wise_xmatch_df,
):
    macauff_setup = MacauffSetup(gaia_wise_joint_params_path, gaia_params_path, wise_params_path)
    xmatch = gaia_cat.crossmatch(
        catwise_cat, algorithm=MacauffCrossmatch, macauff_setup=macauff_setup, suffixes=("_gaia", "_wise")
    )
    result = xmatch.compute()
    test_df = result.sort_values(by="source_id_gaia").reset_index(drop=True)[
        expected_gaia_wise_xmatch_df.columns
    ]
    expected_df = expected_gaia_wise_xmatch_df.sort_values(by="source_id_gaia").reset_index(drop=True)
    pd.testing.assert_frame_equal(test_df, expected_df)
