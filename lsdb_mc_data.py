"""Script to generate test data for lsdb_macauff.

This is largely copy-pasted from macauff/src/macauff/utils.py

Modifications are made to::

- construct a single dataframe using the randomly generated data
- construct string identifiers for each object in each catalog
- write out to one CSV per catalog, and one for counterpart IDs.
"""

import os

import numpy as np
import pandas as pd
from macauff.utils import generate_random_catalogs


def generate_random_data():
    output_dir = "tests/data"
    num_a_source, num_b_source, num_common = 50, 100, 40
    extent = [0, 0.25, 50, 50.3]
    num_filters_a, num_filters_b = 3, 2
    a_uncert, b_uncert = 0.1, 0.3
    (
        a_astro,
        b_astro,
        a_photo,
        b_photo,
        amagref,
        bmagref,
        a_pair_indices,
        b_pair_indices,
    ) = generate_random_catalogs(
        num_a_source,
        num_b_source,
        num_common,
        extent,
        num_filters_a,
        num_filters_b,
        a_uncert,
        b_uncert,
        seed=5732,
    )

    os.makedirs(output_dir, exist_ok=True)
    ## Make a pretty CSV of catalog A
    cat_a_ids = [f"cat_a_3{index :03d}" for index in np.arange(num_a_source)]

    cat_a_data = {
        "survey_id": cat_a_ids,
        "ra": a_astro[:, 0],
        "dec": a_astro[:, 1],
        "astro_unc": a_astro[:, 2],
    }
    for index in range(num_filters_a):
        cat_a_data[f"filter_{index}"] = a_photo[:, index]
    cat_a_data["magref"] = amagref
    catalog_a_frame = pd.DataFrame(cat_a_data)

    catalog_a_frame.to_csv(os.path.join(output_dir, "catalog_a.csv"), index=False)

    ## Make a pretty CSV of catalog B
    cat_b_ids = [f"cat_b_8{index :03d}" for index in np.arange(num_b_source)]

    cat_b_data = {
        "survey_id": cat_b_ids,
        "ra": b_astro[:, 0],
        "dec": b_astro[:, 1],
        "astro_unc": b_astro[:, 2],
    }
    for index in range(num_filters_b):
        cat_b_data[f"filter_{index}"] = b_photo[:, index]
    cat_b_data["magref"] = bmagref
    catalog_b_frame = pd.DataFrame(cat_b_data)

    catalog_b_frame.to_csv(os.path.join(output_dir, "catalog_b.csv"), index=False)

    ## Make a pretty CSV of counterparts
    counters = {
        "cat_a": [f"cat_a_3{index :03d}" for index in a_pair_indices],
        "cat_b": [f"cat_b_8{index :03d}" for index in b_pair_indices],
    }
    pd.DataFrame(counters).to_csv(os.path.join(output_dir, "counters.csv"), index=False)


if __name__ == "__main__":
    generate_random_data()
