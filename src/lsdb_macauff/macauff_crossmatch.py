import numpy as np
import pandas as pd
import pyarrow as pa
from cdshealpix.nested import healpix_to_skycoord
from hats.pixel_math.healpix_pixel import HealpixPixel
from lsdb.core.crossmatch.abstract_crossmatch_algorithm import AbstractCrossmatchAlgorithm
from lsdb.core.crossmatch.crossmatch_args import CrossmatchArgs
from macauff import CrossMatch

# pylint: disable=too-many-instance-attributes, no-member, attribute-defined-outside-init
class MacauffCrossmatch(AbstractCrossmatchAlgorithm, CrossMatch):
    """Class that runs the Macauff crossmatch"""

    CHUNK_ID = "0"

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

    def __init__(self, crossmatch_params_file_path, cat_a_params_file_path, cat_b_params_file_path):
        super().__init__(
            crossmatch_params_file_path,
            cat_a_params_file_path,
            cat_b_params_file_path,
            resume_file_path=None,
            use_mpi=False,
            walltime=None,
        )
        self.validate_params()

    def validate_params(self):
        """Validate that the parameters provided are compatible with this implementation."""
        if self.crossmatch_params_dict["include_perturb_auf"]:
            raise NotImplementedError("Perturbations are not implemented in this version.")
        if self.cat_a_params_dict["correct_astrometry"]:
            raise NotImplementedError("Astrometric corrections are not implemented in this version.")
        if self.cat_b_params_dict["correct_astrometry"]:
            raise NotImplementedError("Astrometric corrections are not implemented in this version.")

    def perform_crossmatch(
        self, crossmatch_args: CrossmatchArgs
    ) -> tuple[np.ndarray, np.ndarray, pd.DataFrame]:
        """Perform a cross-match between the data from two HEALPix pixels

        Finds the n closest neighbors in the right catalog for each point in the left catalog that
        are within a threshold distance by using a K-D Tree.

        Args:
            crossmatch_args (CrossmatchArgs): The partitions and respective pixel information.

        Returns:
            Indices of the matching rows from the left and right tables found from cross-matching, and a
            datafame with the "_dist_arcsec" column with the great circle separation between the points.
        """
        l_inds, r_inds, extra_cols = self._find_crossmatch_indices(crossmatch_args)
        extra_cols = extra_cols.convert_dtypes(dtype_backend="pyarrow", convert_integer=False)
        return l_inds, r_inds, extra_cols

    def _find_crossmatch_indices(self, crossmatch_args: CrossmatchArgs):
        left_partition = crossmatch_args.left_df
        left_order = crossmatch_args.left_order
        left_pixel = crossmatch_args.left_pixel

        right_partition = crossmatch_args.right_df
        right_order = crossmatch_args.right_order
        right_pixel = crossmatch_args.right_pixel

        aligned_pix = (
            HealpixPixel(left_order, left_pixel)
            if left_order > right_order
            else HealpixPixel(right_order, right_pixel)
        )

        if left_partition is None or right_partition is None or aligned_pix is None:
            raise ValueError("left_partition, right_partition, and aligned_pix must be provided.")

        self.left_partition = left_partition
        self.right_partition = right_partition
        self.set_chunk_from_healpix(aligned_pix)
        self._load_metadata_config(self.chunk_id)
        self._process_chunk()
        left_indices = self.ac
        right_indices = self.bc
        extra_columns = pd.DataFrame(
            {
                "p": self.p,
                "eta": self.eta,
                "xi": self.xi,
                "a_avg_cont": self.a_avg_cont,
                "b_avg_cont": self.b_avg_cont,
                "a_cont_f1": self.acontprob[0],
                "a_cont_f10": self.acontprob[1],
                "b_cont_f1": self.bcontprob[0],
                "b_cont_f10": self.bcontprob[1],
                "sep": self.seps,
            }
        )
        return left_indices, right_indices, extra_columns

    def _make_chunk_queue(self, completed_chunks):
        return []

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
        self.chunk_id = MacauffCrossmatch.CHUNK_ID
        healpix_center = healpix_to_skycoord(aligned_pix.pixel, aligned_pix.order)[0]
        self.cat_a_params_dict["chunk_id_list"] = np.array([self.chunk_id])
        self.cat_a_params_dict["auf_region_points_per_chunk"] = np.array(
            [
                [
                    healpix_center.ra.deg,
                    healpix_center.ra.deg,
                    1.0,
                    healpix_center.dec.deg,
                    healpix_center.dec.deg,
                    1.0,
                ]
            ]
        )
        self.cat_b_params_dict["chunk_id_list"] = np.array([self.chunk_id])
        self.cat_b_params_dict["auf_region_points_per_chunk"] = np.array(
            [
                [
                    healpix_center.ra.deg,
                    healpix_center.ra.deg,
                    1.0,
                    healpix_center.dec.deg,
                    healpix_center.dec.deg,
                    1.0,
                ]
            ]
        )
        self.crossmatch_params_dict["chunk_id_list"] = np.array([self.chunk_id])
        self.crossmatch_params_dict["cf_region_points_per_chunk"] = np.array(
            [
                [
                    healpix_center.ra.deg,
                    healpix_center.ra.deg,
                    1.0,
                    healpix_center.dec.deg,
                    healpix_center.dec.deg,
                    1.0,
                ]
            ]
        )

    def _load_metadata_config(self, chunk_id):
        self._load_metadata_config_params(chunk_id)

    def _postprocess_chunk(self):
        """
        Runs the post-processing stage, resolving duplicate cross-matches and
        optionally creating output .csv files for use elsewhere.

        Duplicates are determined by match pairs (or singular non-matches) that
        are entirely outside of the "core" for a given chunk. This core/halo
        divide is defined by the ``in_chunk_overlap`` array; if only a singular
        chunk is being matched (i.e., there is no compartmentalisation of a
        larger region), then ``in_chunk_overlap`` should all be set to ``False``.
        """
        if self.include_phot_like and self.with_and_without_photometry:
            # loop_array_extensions = ['', '_without_photometry']
            loop_array_extensions = [""]
        else:
            loop_array_extensions = [""]

        for file_extension in loop_array_extensions:
            self.ac = getattr(self, f"ac{file_extension}")
            self.bc = getattr(self, f"bc{file_extension}")
            self.p = getattr(self, f"pc{file_extension}")
            self.eta = getattr(self, f"eta{file_extension}")
            self.xi = getattr(self, f"xi{file_extension}")
            self.a_avg_cont = getattr(self, f"acontamflux{file_extension}")
            self.b_avg_cont = getattr(self, f"bcontamflux{file_extension}")
            self.acontprob = getattr(self, f"pacontam{file_extension}")
            self.bcontprob = getattr(self, f"pbcontam{file_extension}")
            self.seps = getattr(self, f"crptseps{file_extension}")
