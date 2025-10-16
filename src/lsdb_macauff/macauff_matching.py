import pandas as pd
from macauff import CrossMatch
from cdshealpix.nested import healpix_to_skycoord
import numpy as np

CHUNK_ID = "0"

class MacauffMatching(CrossMatch):
    """Class that runs the Macauff matching"""

    def __init__(self, crossmatch_params_file_path, cat_a_params_file_path, cat_b_params_file_path):
        super().__init__(crossmatch_params_file_path, cat_a_params_file_path, cat_b_params_file_path,
                         resume_file_path=None, use_mpi=False, walltime=None)

    def _make_chunk_queue(self, completed_chunks):
        return []

    def read_metadata(self):
        joint_config, cat_a_config, cat_b_config = super().read_metadata()
        if joint_config["include_perturb_auf"]:
            raise NotImplementedError("Perturbations are not implemented in this version.")
        if cat_a_config["correct_astrometry"]:
            raise NotImplementedError("Astrometric corrections are not implemented in this version.")
        if cat_b_config["correct_astrometry"]:
            raise NotImplementedError("Astrometric corrections are not implemented in this version.")
        return joint_config, cat_a_config, cat_b_config

    def __call__(self, left_partition, right_partition, left_pix, right_pix, aligned_pix):
        self.left_partition = left_partition
        self.right_partition = right_partition
        self.set_chunk_from_healpix(aligned_pix)
        self._load_metadata_config(self.chunk_id)
        self._process_chunk()
        left_indices = self.ac
        right_indices = self.bc
        extra_columns = pd.DataFrame({
            'p': self.p,
            'eta': self.eta,
            'xi': self.xi,
            'a_avg_cont': self.a_avg_cont,
            'b_avg_cont': self.b_avg_cont,
            'seps': self.seps
        })
        return left_indices, right_indices, extra_columns

    def _initialise_chunk(self):
        self.a_astro = self.left_partition.iloc[:, self.a_pos_and_err_indices].to_numpy(dtype=np.float64)
        self.a_photo = self.left_partition.iloc[:, self.a_mag_indices].to_numpy(dtype=np.float64)
        self.a_magref = self.left_partition.iloc[:, self.a_best_mag_index_col].to_numpy(dtype=np.int64)
        self.a_in_overlaps = self.left_partition.iloc[:, self.a_chunk_overlap_col].to_numpy().astype(bool)
        self.b_astro = self.right_partition.iloc[:, self.b_pos_and_err_indices].to_numpy(dtype=np.float64)
        self.b_photo = self.right_partition.iloc[:, self.b_mag_indices].to_numpy(dtype=np.float64)
        self.b_magref = self.right_partition.iloc[:, self.b_best_mag_index_col].to_numpy(dtype=np.int64)
        self.b_in_overlaps = self.right_partition.iloc[:, self.b_chunk_overlap_col].to_numpy().astype(bool)

        self.make_shared_data()

    def set_chunk_from_healpix(self, aligned_pix):
        """Set the chunk parameters based on the healpix pixels."""
        self.chunk_id = CHUNK_ID
        healpix_center = healpix_to_skycoord(aligned_pix.pixel, aligned_pix.order)[0]
        self.cat_a_params_dict["chunk_id_list"] = np.array([self.chunk_id])
        self.cat_a_params_dict["auf_region_points_per_chunk"] = np.array([[healpix_center.ra.deg, healpix_center.ra.deg, 1., healpix_center.dec.deg, healpix_center.dec.deg, 1.]])
        self.cat_b_params_dict["chunk_id_list"] = np.array([self.chunk_id])
        self.cat_b_params_dict["auf_region_points_per_chunk"] = np.array([[healpix_center.ra.deg, healpix_center.ra.deg, 1., healpix_center.dec.deg, healpix_center.dec.deg, 1.]])
        self.crossmatch_params_dict["chunk_id_list"] = np.array([self.chunk_id])
        self.crossmatch_params_dict["cf_region_points_per_chunk"] = np.array([[healpix_center.ra.deg, healpix_center.ra.deg, 1., healpix_center.dec.deg, healpix_center.dec.deg, 1.]])

    def _load_metadata_config(self, chunk_id):
        '''
        Generate per-chunk class variables from the three stored parameter
        metadata files.

        Parameters
        ----------
        chunk_id : string
            Identifier for extraction of single element of metadata parameters
            that vary on a per-chunk basis, rather than being fixed for the
            entire catalogue/cross-match run, across all regions.
        '''
        for key, item in self.crossmatch_params_dict.items():
            if "_per_chunk" in key:
                # If the key contains the (end-)string per_chunk then this
                # is a list of parameters, one per chunk. In this case, pick
                # from the correct element based on chunk_id_list from the
                # joint-catalogue config file.
                ind = np.where(chunk_id == np.array(self.crossmatch_params_dict['chunk_id_list']))[0][0]
                _item = np.array(item[ind]) if item[ind] is list else item[ind]
                setattr(self, key.replace("_per_chunk", ""), _item)
            elif isinstance(item, str) and r"_{}" in item:
                # If input variable contains _{} in a string, then we expect
                # and assume that it should be modulated with the chunk ID.
                setattr(self, key, item.format(chunk_id))
            else:
                # Otherwise we just add the item unchanged.
                _item = np.array(item) if item is list else item
                setattr(self, key, _item)

        for cat_prefix, cat_dict in zip(['a_', 'b_'], [self.cat_a_params_dict, self.cat_b_params_dict]):
            for key, item in cat_dict.items():
                if "_per_chunk" in key:
                    ind = np.where(chunk_id == np.array(cat_dict['chunk_id_list']))[0][0]
                    _item = np.array(item[ind]) if item[ind] is list else item[ind]
                    setattr(self, f'{cat_prefix}{key.replace("_per_chunk", "")}', _item)
                elif isinstance(item, str) and r"_{}" in item:
                    setattr(self, f'{cat_prefix}{key}', item.format(chunk_id))
                else:
                    _item = np.array(item) if item is list else item
                    setattr(self, f'{cat_prefix}{key}', _item)

        for config, catname in zip([self.cat_a_params_dict, self.cat_b_params_dict], ['a_', 'b_']):
            ind = np.where(chunk_id == np.array(config['chunk_id_list']))[0][0]
            self._make_regions_points([f'{catname}auf_region_type', config['auf_region_type']],
                                      [f'{catname}auf_region_points',
                                       config['auf_region_points_per_chunk'][ind]],
                                      config['chunk_id_list'][ind])

        ind = np.where(chunk_id == np.array(self.crossmatch_params_dict['chunk_id_list']))[0][0]
        self._make_regions_points(['cf_region_type', self.crossmatch_params_dict['cf_region_type']],
                                  ['cf_region_points',
                                   self.crossmatch_params_dict['cf_region_points_per_chunk'][ind]],
                                  self.crossmatch_params_dict['chunk_id_list'][ind])

        for config, flag in zip([self.cat_a_params_dict, self.cat_b_params_dict], ['a_', 'b_']):
            # Only need dd_params or l_cut if we're using run_psf_auf or
            # correct_astrometry is True.
            if (self.crossmatch_params_dict['include_perturb_auf'] and
                    config['run_psf_auf']) or config['correct_astrometry']:
                for check_flag, f in zip(['dd_params_path', 'l_cut_path'], ['dd_params', 'l_cut']):
                    setattr(self, f'{flag}{f}', np.load(f'{config[check_flag]}/{f}.npy'))

        for config, flag in zip([self.cat_a_params_dict, self.cat_b_params_dict], ['a_', 'b_']):
            if self.crossmatch_params_dict['include_perturb_auf'] or config['correct_astrometry']:
                for name in ['dens_hist_tri', 'tri_model_mags', 'tri_model_mag_mids',
                             'tri_model_mags_interval', 'tri_dens_uncert', 'tri_n_bright_sources_star']:
                    # If location variable was "None" in the first place we set
                    # {name}_list in config to a list of Nones and it got updated
                    # above already.
                    if config[f'{name}_location'] != "None":
                        setattr(self, f'{flag}{name}_list', np.load(config[f'{name}_location']))
        for config, flag in zip([self.cat_a_params_dict, self.cat_b_params_dict], ['a_', 'b_']):
            if config['correct_astrometry']:
                if not config['use_photometric_uncertainties']:
                    # The reshape puts the first three elements in a[0], and hence
                    # those are this_cat_inds, with a[1] ref_cat_inds.
                    setattr(self, f'{flag}pos_and_err_indices',
                            np.array(config['pos_and_err_indices']).reshape(2, 3))
                else:
                    # If use_photometric_uncertainties then we need to make a
                    # more generic two-list nested list. This is every index
                    # except the last three in the first list, the final three
                    # indices in a second nested list.
                    setattr(self, f'{flag}pos_and_err_indices',
                            [config['pos_and_err_indices'][:-3], config['pos_and_err_indices'][-3:]])
            else:
                # Otherwise we only need three elements, so we just store them
                # in a (3,) shape array.
                setattr(self, f'{flag}pos_and_err_indices', config['pos_and_err_indices'])

    def _postprocess_chunk(self):
        '''
        Runs the post-processing stage, resolving duplicate cross-matches and
        optionally creating output .csv files for use elsewhere.

        Duplicates are determined by match pairs (or singular non-matches) that
        are entirely outside of the "core" for a given chunk. This core/halo
        divide is defined by the ``in_chunk_overlap`` array; if only a singular
        chunk is being matched (i.e., there is no compartmentalisation of a
        larger region), then ``in_chunk_overlap`` should all be set to ``False``.
        '''
        if self.include_phot_like and self.with_and_without_photometry:
            # loop_array_extensions = ['', '_without_photometry']
            loop_array_extensions = ['']
        else:
            loop_array_extensions = ['']

        for file_extension in loop_array_extensions:
            self.ac = getattr(self, f'ac{file_extension}')
            self.bc = getattr(self, f'bc{file_extension}')
            self.p = getattr(self, f'pc{file_extension}')
            self.eta = getattr(self, f'eta{file_extension}')
            self.xi = getattr(self, f'xi{file_extension}')
            self.a_avg_cont = getattr(self, f'acontamflux{file_extension}')
            self.b_avg_cont = getattr(self, f'bcontamflux{file_extension}')
            self.acontprob = getattr(self, f'pacontam{file_extension}')
            self.bcontprob = getattr(self, f'pbcontam{file_extension}')
            self.seps = getattr(self, f'crptseps{file_extension}')
