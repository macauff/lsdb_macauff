from dataclasses import dataclass
from typing import Dict, List


@dataclass
class PixelParams:
    """Macauff parameters for a certain HEALPix pixel"""

    order: int
    pixel: int
    tri_map_histogram: Dict[int, List[int]]
