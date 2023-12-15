from macauff.counterpart_pairing import source_pairing
from macauff.group_sources import make_island_groupings
from macauff.perturbation_auf import make_perturb_aufs
from macauff.photometric_likelihood import compute_photometric_likelihoods


# pylint: disable=too-few-public-methods
class AllMacauffAttrs:
    """Instantiates the class with the parameters to pass
    as an argument to the Macauff crossmatch algorithm"""

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

        self.perturb_auf_func = make_perturb_aufs
        self.group_func = make_island_groupings
        self.phot_like_func = compute_photometric_likelihoods
        self.count_pair_func = source_pairing
