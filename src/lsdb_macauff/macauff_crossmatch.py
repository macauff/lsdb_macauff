import healpy as hp
import numpy as np
import pandas as pd
from hipscat.pixel_math.hipscat_id import compute_hipscat_id
from lsdb.core.crossmatch.abstract_crossmatch_algorithm import AbstractCrossmatchAlgorithm
from macauff import AstrometricCorrections
from macauff.fit_astrometry import SNRMagnitudeRelationship
from macauff.macauff import Macauff
from macauff.misc_functions_fortran import misc_functions_fortran as mff

from lsdb_macauff.all_macauff_attrs import AllMacauffAttrs
from lsdb_macauff.config import CatalogAllSkyParams
from lsdb_macauff.config.pixel_params import PixelParams


# pylint: disable=arguments-differ,unused-variable,attribute-defined-outside-init
class MacauffCrossmatch(AbstractCrossmatchAlgorithm):
    """Class that runs the Macauff crossmatch"""

    def crossmatch(
        self,
        joint_all_sky_params,
        left_all_sky_params,
        right_all_sky_params,
        left_tri_map_histogram,
        right_tri_map_histogram,
    ) -> pd.DataFrame:
        # Calculate macauff pixel params
        macauff_pixel_params = None
        left_pixel_params = PixelParams(self.left_order, self.left_pixel, left_tri_map_histogram)
        right_pixel_params = PixelParams(self.right_order, self.right_pixel, right_tri_map_histogram)
        self.all_macauff_attrs = AllMacauffAttrs(
            joint_all_sky_params,
            macauff_pixel_params,
            left_all_sky_params,
            right_all_sky_params,
            left_pixel_params,
            right_pixel_params,
        )
        self._preprocess_catalogs()
        macauff = Macauff(self.all_macauff_attrs)
        macauff()
        return self._make_joint_dataframe()

    def make_data_arrays(self, data, metadata, params):
        """Creates the astro, photo, and magref arrays for a given catalog's dataset."""
        ra_column = metadata.catalog_info.ra_column
        dec_column = metadata.catalog_info.dec_column
        uncertainty_column = params.uncertainty_column_name
        astro = data[[ra_column, dec_column, uncertainty_column]].to_numpy()
        photo = data[params.filt_names].to_numpy()
        magref = data[params.magref_column_name].to_numpy()
        return astro, photo, magref

    def _preprocess_catalogs(self):
        """Applies preprocessing operations to the left and right catalogs"""
        a_astro, a_photo, a_magref = self.make_data_arrays(
            self.left, self.left_metadata, self.all_macauff_attrs.left_all_sky_params
        )
        self._process_astrometry(
            self.left, a_astro, self.all_macauff_attrs.left_all_sky_params, self.left_order, self.left_pixel
        )
        b_astro, b_photo, b_magref = self.make_data_arrays(
            self.right, self.right_metadata, self.all_macauff_attrs.right_all_sky_params
        )
        self._process_astrometry(
            self.right,
            b_astro,
            self.all_macauff_attrs.right_all_sky_params,
            self.right_order,
            self.right_pixel,
        )

    def _process_astrometry(
        self,
        dataframe: pd.DataFrame,
        astro: np.ndarray,
        catalog_sky_params: CatalogAllSkyParams,
        order: int,
        pixel: int,
    ):
        # Some assumptions
        ax_dimension = 2
        npy_or_csv = "csv"  # There should be another option "read from memory"
        coord_or_chunk = "chunk"

        # Calculate the HEALPix pixel centroid
        lon, lat = hp.pix2ang(2**order, pixel, lonlat=True, nest=True)
        ax1_mids, ax2_mids = np.array([lon]), np.array([lat])
        hipscat_id = compute_hipscat_id([lon], [lat])[0]

        if catalog_sky_params.correct_astrometry:
            new_sigs, abc_array = self._apply_corrections(
                hipscat_id,
                catalog_sky_params,
                astro,
                ax1_mids,
                ax2_mids,
                ax_dimension,
                npy_or_csv,
                coord_or_chunk,
            )
            dataframe[catalog_sky_params.uncertainty_column_name] = new_sigs
        else:
            smr = SNRMagnitudeRelationship(
                "",  # catalog_sky_params.correct_astro_save_folder
                ax1_mids,
                ax2_mids,
                ax_dimension,
                npy_or_csv,
                coord_or_chunk,
                catalog_sky_params.pos_and_err_indices,
                catalog_sky_params.mag_indices,
                catalog_sky_params.mag_unc_indices,
                catalog_sky_params.filt_names,
                catalog_sky_params.auf_region_frame,
                chunks=[hipscat_id],  # prefix based on the HEALPix pixel
            )
            abc_array = smr(
                "",  # self.a_csv_cat_file_string
                make_plots=True,
                overwrite_all_sightlines=True,
            )
        catalog_sky_params.snr_mag_params = abc_array

    def _apply_corrections(
        self,
        hipscat_id: int,
        catalog_sky_params: CatalogAllSkyParams,
        astro: np.ndarray,
        ax1_mids: np.ndarray,
        ax2_mids: np.ndarray,
        ax_dimension: int,
        npy_or_csv: str,
        coord_or_chunk: str,
    ):
        acbi = catalog_sky_params.best_mag_index
        ac = AstrometricCorrections(
            catalog_sky_params.psf_fwhms[acbi],
            self.all_macauff_attrs.macauff_all_sky_params.num_trials,
            catalog_sky_params.nn_radius,
            catalog_sky_params.dens_dist,
            "",  # catalog_sky_params.correct_astro_save_folder
            "",  # catalog_sky_params.auf_folder_path
            "{}/{}/trilegal_auf_simulation",  # correct_astro_tri_name
            catalog_sky_params.tri_maglim_faint,
            catalog_sky_params.tri_filt_num,
            catalog_sky_params.tri_num_faint,
            catalog_sky_params.tri_set_name,
            catalog_sky_params.tri_filt_names[acbi],
            catalog_sky_params.gal_wavs[acbi],
            catalog_sky_params.gal_aboffsets[acbi],
            catalog_sky_params.gal_filternames[acbi],
            catalog_sky_params.gal_al_avs[acbi],
            self.all_macauff_attrs.macauff_all_sky_params.d_mag,
            self.all_macauff_attrs.macauff_all_sky_params.dd_params,
            self.all_macauff_attrs.macauff_all_sky_params.l_cut,
            ax1_mids,
            ax2_mids,
            ax_dimension,
            np.array(catalog_sky_params.correct_mag_array),
            np.array(catalog_sky_params.correct_mag_slice),
            np.array(catalog_sky_params.correct_sig_slice),
            self.all_macauff_attrs.macauff_all_sky_params.n_pool,
            npy_or_csv,
            coord_or_chunk,
            catalog_sky_params.pos_and_err_indices,
            catalog_sky_params.mag_indices,
            catalog_sky_params.mag_unc_indices,
            catalog_sky_params.filt_names,
            catalog_sky_params.best_mag_index,
            catalog_sky_params.auf_region_frame,
            use_photometric_uncertainties=catalog_sky_params.use_photometric_uncertainties,
            pregenerate_cutouts=True,
            chunks=[hipscat_id],  # prefix based on the HEALPix pixel
            n_r=self.all_macauff_attrs.macauff_all_sky_params.real_hankel_points,
            n_rho=self.all_macauff_attrs.macauff_all_sky_params.four_hankel_points,
            max_rho=self.all_macauff_attrs.macauff_all_sky_params.four_max_rho,
            return_nm=True,
        )
        m_sigs, n_sigs, abc_array = ac(
            a_cat="",  # TODO: Update
            b_cat="",  # TODO: Update
            tri_download=catalog_sky_params.download_tri,
            make_plots=True,
            overwrite_all_sightlines=True,
        )
        # astro is np.array([ra,dec,unc])
        sig_mn_inds = mff.find_nearest_point(astro[0], astro[1], ax1_mids, ax2_mids)
        new_sigs = np.sqrt((m_sigs[sig_mn_inds] * astro[2]) ** 2 + n_sigs[sig_mn_inds] ** 2)
        return new_sigs, abc_array

    def _make_joint_dataframe(self) -> pd.DataFrame:
        """Creates the resulting crossmatch pandas Dataframe"""
        raise NotImplementedError()
