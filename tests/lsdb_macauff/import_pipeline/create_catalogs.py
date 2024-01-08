"""Script to convert generated test catalogs into hipscatted catalogs.

See also lsdb_mc_data.py, for generating some data using macauff utils."""

import hipscat_import.pipeline as runner
from hipscat_import.catalog.arguments import ImportArguments


def create_catalogs():
    args = ImportArguments(
        output_artifact_name="catalog_a",
        input_file_list=["/home/delucchi/git/lsdb_macauff/tests/data/catalog_a.csv"],
        input_format="csv",
        ra_column="ra",
        dec_column="dec",
        sort_columns="survey_id",
        constant_healpix_order=4,
        output_path="/home/delucchi/git/lsdb_macauff/tests/data/import_pipeline",
        dask_n_workers=1,
        overwrite=True,
    )
    runner.pipeline(args)

    args = ImportArguments(
        output_artifact_name="catalog_b",
        input_file_list=["/home/delucchi/git/lsdb_macauff/tests/data/catalog_b.csv"],
        input_format="csv",
        ra_column="ra",
        dec_column="dec",
        sort_columns="survey_id",
        constant_healpix_order=4,
        output_path="/home/delucchi/git/lsdb_macauff/tests/data/import_pipeline",
        dask_n_workers=1,
        overwrite=True,
    )
    runner.pipeline(args)


if __name__ == "__main__":
    create_catalogs()
