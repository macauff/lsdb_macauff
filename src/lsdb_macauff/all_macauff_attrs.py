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
