from __future__ import annotations

from dataclasses import dataclass
from typing import List


# pylint: disable=too-many-instance-attributes
@dataclass
class CatalogAllSkyParams:
    """Catalog-dependent sky parameters"""

    cat_name: str
    filt_names: List[str]
    uncertainty_column_name: str
    magref_column_name: str
    auf_region_type: str
    auf_region_frame: str
    correct_astrometry: bool
    compute_snr_mag_relation: bool
    best_mag_index: int | None = None
    correct_mag_array: List[float] | None = None
    correct_mag_slice: List[float] | None = None
    correct_sig_slice: List[float] | None = None
    pos_and_err_indices: List[int] | None = None
    mag_indices: List[int] | None = None
    mag_unc_indices: List[int] | None = None
    use_photometric_uncertainties: bool | None = None
    nn_radius: float | None = None
    fit_gal_flag: bool | None = None
    gal_wavs: List[float] | None = None
    gal_zmax: List[float] | None = None
    gal_nzs: List[int] | None = None
    gal_aboffsets: List[float] | None = None
    gal_filternames: List[str] | None = None
    run_fw_auf: bool | None = None
    run_psf_auf: bool | None = None
    dd_params_path: str | None = None
    l_cut_path: str | None = None
    psf_fwhms: List[float] | None = None
    snr_mag_params_path: str | None = None
    download_tri: bool | None = None
    tri_set_name: str | None = None
    tri_filt_names: List[str] | None = None
    tri_filt_num: int | None = None
    tri_maglim_faint: float | None = None
    tri_num_faint: int | None = None
    gal_al_avs: List[float] | None = None
    dens_dist: float | None = None
