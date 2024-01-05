import healpy as hp
import numpy as np
import pandas as pd
from lsdb.core.crossmatch.abstract_crossmatch_algorithm import AbstractCrossmatchAlgorithm
from macauff import AstrometricCorrections
from macauff.fit_astrometry import SNRMagnitudeRelationship

from lsdb_macauff.all_macauff_attrs import AllMacauffAttrs
from lsdb_macauff.config import CatalogAllSkyParams
from lsdb_macauff.config.pixel_params import PixelParams

# from macauff.macauff import Macauff


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
        left_pixel_params = PixelParams(self.left_order, self.left_pixel, left_tri_map_histogram)
        right_pixel_params = PixelParams(self.right_order, self.right_pixel, right_tri_map_histogram)
        all_macauff_attrs = AllMacauffAttrs(
            joint_all_sky_params,
            macauff_pixel_params,
            left_all_sky_params,
            right_all_sky_params,
            left_pixel_params,
            right_pixel_params,
        )

        self._correct_astrometry()

        # get the dataframes
        a_astro, a_photo, a_magref = self.make_data_arrays(self.left, self.left_metadata, left_all_sky_params)

        all_macauff_attrs.a_astro = a_astro
        all_macauff_attrs.a_photo = a_photo
        all_macauff_attrs.a_magref = a_magref

        b_astro, b_photo, b_magref = self.make_data_arrays(
            self.right, self.right_metadata, right_all_sky_params
        )

        all_macauff_attrs.b_astro = b_astro
        all_macauff_attrs.b_photo = b_photo
        all_macauff_attrs.b_magref = b_magref

        self.all_macauff_attrs = all_macauff_attrs

        # macauff = Macauff(self.all_macauff_attrs)
        # macauff()
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

    def _get_astrometry_parameters(self):
        dataframes = [self.left, self.right]
        catalogs_params = [
            self.all_macauff_attrs.left_all_sky_params,
            self.all_macauff_attrs.right_all_sky_params,
        ]
        order = [self.left_order, self.right_order]
        pixel = [self.left_pixel, self.right_pixel]
        return list(zip(dataframes, catalogs_params, order, pixel))

    def _correct_astrometry(self):
        # Get the parameters to compute astrometric corrections
        astrometry_params = self._get_astrometry_parameters()

        for params in astrometry_params:
            dataframe, catalog_sky_params, order, pixel = params

            # Check if these assumptions are correct
            lon, lat = hp.pix2ang(2**order, pixel, lonlat=True, nest=True)
            ax1_mids, ax2_mids = np.array([lon]), np.array([lat])
            ax_dimension = 2
            npy_or_csv = "csv"
            coord_or_chunk = "chunk"

            if catalog_sky_params.correct_astrometry:
                m_sigs, n_sigs, smr = self._calculate_corrections(
                    catalog_sky_params,
                    ax1_mids,
                    ax2_mids,
                    ax_dimension,
                    npy_or_csv,
                    coord_or_chunk,
                )
                # Retrieve the function to apply the corrections
                new_uncertainties = self._apply_corrections(m_sigs, n_sigs)
                dataframe.assign(uncertainty=new_uncertainties)
            else:
                smr, _, _ = SNRMagnitudeRelationship(
                    catalog_sky_params.correct_astro_save_folder,  # This one does not exist
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
                    chunks=[self.chunk_id],  # Check if this id is related to the trilegal download
                )
            catalog_sky_params.snr_mag_params = smr

    def _calculate_corrections(
        self,
        catalog_sky_params: CatalogAllSkyParams,
        ax1_mids: np.ndarray,
        ax2_mids: np.ndarray,
        ax_dimension: int,
        npy_or_csv: str,
        coord_or_chunk: str,
    ):
        acbi = catalog_sky_params.best_mag_index
        m_sigs, n_sigs, abc_array, _, _ = AstrometricCorrections(
            catalog_sky_params.psf_fwhms[acbi],
            self.all_macauff_attrs.macauff_all_sky_params.num_trials,
            catalog_sky_params.nn_radius,
            catalog_sky_params.dens_dist,
            catalog_sky_params.correct_astro_save_folder,  # This one does not exist
            catalog_sky_params.auf_folder_path,  # Used to be in catalog, but was removed
            "{}/{}/trilegal_auf_simulation",  # (correct_astro_tri_name): confirm
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
            chunks=[self.chunk_id],  # Check if this id is related to the trilegal download
            n_r=self.all_macauff_attrs.macauff_all_sky_params.real_hankel_points,
            n_rho=self.all_macauff_attrs.macauff_all_sky_params.four_hankel_points,
            max_rho=self.all_macauff_attrs.macauff_all_sky_params.four_max_rho,
            return_nm=True,
        )
        return m_sigs, n_sigs, abc_array

    def _apply_corrections(self):
        return

    def _make_joint_dataframe(self) -> pd.DataFrame:
        """Creates the resulting crossmatch pandas Dataframe"""
        raise NotImplementedError()
