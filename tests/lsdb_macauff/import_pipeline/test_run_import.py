import os

import pytest
from hipscat.catalog.association_catalog.association_catalog import AssociationCatalog
from hipscat.io import file_io
from hipscat_import.catalog.file_readers import CsvReader

import lsdb_macauff.import_pipeline.run_import as runner
from lsdb_macauff.import_pipeline.arguments import MacauffArguments
from lsdb_macauff.import_pipeline.convert_metadata import from_yaml

# pylint: disable=too-many-instance-attributes
# pylint: disable=duplicate-code


@pytest.mark.dask
def test_bad_args(dask_client):
    """Runner should fail with empty or mis-typed arguments"""
    with pytest.raises(TypeError, match="MacauffArguments"):
        runner.run(None, dask_client)

    args = {"output_artifact_name": "bad_arg_type"}
    with pytest.raises(TypeError, match="MacauffArguments"):
        runner.run(args, dask_client)


@pytest.mark.dask
def test_object_to_object(
    import_catalog_a,
    import_catalog_b,
    tmp_path,
    import_metadata_yaml,
    import_match_csv,
    dask_client,
):
    """Test that we can create an association catalog by running the Macauff import pipeline."""

    from_yaml(import_metadata_yaml, tmp_path)
    matches_schema_file = os.path.join(tmp_path, "catalog_a_b_matches.parquet")
    single_metadata = file_io.read_parquet_metadata(matches_schema_file)
    schema = single_metadata.schema.to_arrow_schema()

    assert len(schema) == 7

    args = MacauffArguments(
        output_path=tmp_path,
        output_artifact_name="object_to_object",
        tmp_dir=tmp_path,
        left_catalog_dir=import_catalog_a,
        left_ra_column="catalog_a_ra",
        left_dec_column="catalog_a_dec",
        left_id_column="catalog_a_id",
        right_catalog_dir=import_catalog_b,
        right_ra_column="catalog_b_ra",
        right_dec_column="catalog_b_dec",
        right_id_column="catalog_b_name",
        input_file_list=[import_match_csv],
        input_format="csv",
        overwrite=True,
        file_reader=CsvReader(schema_file=matches_schema_file, header=None),
        metadata_file_path=matches_schema_file,
        progress_bar=False,
    )
    os.makedirs(os.path.join(args.tmp_path, "splitting"))

    runner.run(args, dask_client)

    ## Check that the association data can be parsed as a valid association catalog.
    catalog = AssociationCatalog.read_from_hipscat(args.catalog_path)
    assert catalog.on_disk
    assert catalog.catalog_path == args.catalog_path
    assert len(catalog.get_join_pixels()) == 1
    assert catalog.catalog_info.total_rows == 40
