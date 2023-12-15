import numpy as np

# from macauff.counterpart_pairing import source_pairing
# from macauff.group_sources import make_island_groupings
# from macauff.perturbation_auf import make_perturb_aufs
# from macauff.photometric_likelihood import compute_photometric_likelihoods


def do_nothing1(_ignored):
    """Does nothing."""
    pass


def do_nothing2(_ignored1, _ignored2):
    """Does nothing AND returns nothing."""
    return None, None


# pylint: disable=too-few-public-methods,too-many-instance-attributes
class AllMacauffAttrs:
    """Instantiates the class with the parameters to pass
    as an argument to the Macauff crossmatch algorithm"""

    # pylint: disable=too-many-instance-attributes

    def __init__(
        self,
        macauff_all_sky_params,
        macauff_pixel_params,
        left_all_sky_params,
        right_all_sky_params,
        left_pixel_params,
        right_pixel_params,
    ):
        self.macauff_all_sky_params = macauff_all_sky_params
        self.macauff_pixel_params = macauff_pixel_params
        self.left_all_sky_params = left_all_sky_params
        self.right_all_sky_params = right_all_sky_params
        self.left_pixel_params = left_pixel_params
        self.right_pixel_params = right_pixel_params

        self.a_filt_names = left_all_sky_params.filt_names
        self.b_filt_names = right_all_sky_params.filt_names

        self.include_perturb_auf = macauff_all_sky_params.include_perturb_auf

        ####### ============================
        ## Figure this out from left/right pixels
        self.cross_match_extent = [0, 0.25, 50, 50.3]
        self.cf_region_points = np.array(
            [
                [131, -1],
                [132, -1],
                [133, -1],
                [134, -1],
                [131, 0],
                [132, 0],
                [133, 0],
                [134, 0],
                [131, 1],
                [132, 1],
                [133, 1],
                [134, 1],
            ]
        )

        ####### ============================
        ## Just initialize. Not sure what to do with them.
        self.r = np.linspace(
            0, macauff_all_sky_params.pos_corr_dist, macauff_all_sky_params.real_hankel_points
        )
        self.dr = np.diff(self.r)
        self.rho = np.linspace(
            0, macauff_all_sky_params.four_max_rho, macauff_all_sky_params.four_hankel_points
        )
        self.drho = np.diff(self.rho)
        # Only need to calculate these the first time we need them, so buffer for now.
        self.j0s = None
        self.j1s = None

        self.perturb_auf_func = do_nothing2
        self.group_func = do_nothing1
        self.phot_like_func = do_nothing1
        self.count_pair_func = do_nothing1

        # self.perturb_auf_func = make_perturb_aufs
        # self.group_func = make_island_groupings
        # self.phot_like_func = compute_photometric_likelihoods
        # self.count_pair_func = source_pairing

        ####### ============================
        ## We might be able to remove these parameters, someday?
        self.rank = 0
        self.chunk_id = 1
        self.a_cat_folder_path = ""
        self.a_auf_folder_path = ""
        self.a_auf_region_points = ""
        self.b_cat_folder_path = ""
        self.b_auf_folder_path = ""
        self.a_auf_region_paths = ""
        self.joint_folder_path = ""
