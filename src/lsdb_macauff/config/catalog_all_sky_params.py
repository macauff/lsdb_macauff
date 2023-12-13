from __future__ import annotations

from dataclasses import dataclass
from typing import List


# pylint: disable=too-many-instance-attributes
@dataclass
class CatalogAllSkyParams:
    """Catalog-dependent sky parameters"""

    cat_name: str
    filt_names: List[str]
    auf_region_type: str
    auf_region_frame: str
    correct_astrometry: bool
    compute_snr_mag_relation: bool
    fit_gal_flag: bool | None = None
    gal_wavs: List[float] | None = None
    gal_zmax: List[float] | None = None
    gal_nzs: int | None = None
    gal_aboffsets: float | None = None
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
