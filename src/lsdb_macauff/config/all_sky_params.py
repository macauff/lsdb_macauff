from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass
class AllSkyParams:
    """Joint sky parameters"""

    include_perturb_auf: bool
    include_phot_like: bool
    use_phot_priors: bool
    pos_corr_dist: float
    cf_region_type: str
    cf_region_frame: str
    real_hankel_points: int
    four_hankel_points: int
    four_max_rho: int
    int_fracs: List[float]
    n_pool: int
    num_trials: int | None = None
    d_mag: float | None = None
