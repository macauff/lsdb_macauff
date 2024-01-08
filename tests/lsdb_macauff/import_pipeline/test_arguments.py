"""Tests of macauff arguments"""


from os import path

import pytest

from lsdb_macauff.import_pipeline.arguments import MacauffArguments

# pylint: disable=duplicate-code


def test_macauff_arguments(catwise_dir, gaia_dir, test_data_dir, import_metadata_yaml, tmp_path):
    """Test that we can create a MacauffArguments instance with two valid catalogs."""
    args = MacauffArguments(
        output_path=tmp_path,
        output_artifact_name="object_to_source",
        tmp_dir=tmp_path,
        left_catalog_dir=gaia_dir,
        left_ra_column="ra",
        left_dec_column="dec",
        left_id_column="id",
        right_catalog_dir=catwise_dir,
        right_ra_column="source_ra",
        right_dec_column="source_dec",
        right_id_column="source_id",
        input_path=test_data_dir,
        input_format="csv",
        metadata_file_path=import_metadata_yaml,
    )

    assert len(args.input_paths) > 0


def test_empty_required(gaia_dir, catwise_dir, test_data_dir, import_metadata_yaml, tmp_path):
    """All non-runtime arguments are required."""

    ## List of required args:
    ##  - match expression that should be found when missing
    ##  - default value
    required_args = [
        ["output_path", tmp_path],
        ["output_artifact_name", "object_to_source"],
        ["left_catalog_dir", gaia_dir],
        ["left_ra_column", "ra"],
        ["left_dec_column", "dec"],
        ["left_id_column", "id"],
        ["right_catalog_dir", catwise_dir],
        ["right_ra_column", "source_ra"],
        ["right_dec_column", "source_dec"],
        ["right_id_column", "source_id"],
        ["input_path", test_data_dir],
        ["input_format", "csv"],
        ["metadata_file_path", import_metadata_yaml],
    ]

    ## For each required argument, check that a ValueError is raised that matches the
    ## expected name of the missing param.
    for index, args in enumerate(required_args):
        test_args = [
            list_args[1] if list_index != index else None
            for list_index, list_args in enumerate(required_args)
        ]

        print(f"testing required arg #{index}")

        with pytest.raises(ValueError, match=args[0]):
            MacauffArguments(
                output_path=test_args[0],
                output_artifact_name=test_args[1],
                tmp_dir=tmp_path,
                left_catalog_dir=test_args[2],
                left_ra_column=test_args[3],
                left_dec_column=test_args[4],
                left_id_column=test_args[5],
                right_catalog_dir=test_args[6],
                right_ra_column=test_args[7],
                right_dec_column=test_args[8],
                right_id_column=test_args[9],
                input_path=test_args[10],
                input_format=test_args[11],
                metadata_file_path=test_args[12],
                overwrite=True,
            )


def test_macauff_arguments_file_list(gaia_dir, catwise_dir, test_data_dir, import_metadata_yaml, tmp_path):
    """Test that we can create a MacauffArguments instance with two valid catalogs."""
    files = [path.join(test_data_dir, "catalog.csv")]
    args = MacauffArguments(
        output_path=tmp_path,
        output_artifact_name="object_to_source",
        tmp_dir=tmp_path,
        left_catalog_dir=gaia_dir,
        left_ra_column="ra",
        left_dec_column="dec",
        left_id_column="id",
        right_catalog_dir=catwise_dir,
        right_ra_column="source_ra",
        right_dec_column="source_dec",
        right_id_column="source_id",
        input_file_list=files,
        input_format="csv",
        metadata_file_path=import_metadata_yaml,
    )

    assert len(args.input_paths) > 0


def test_macauff_args_invalid_catalog(catwise_dir, test_data_dir, import_metadata_yaml, tmp_path):
    with pytest.raises(ValueError, match="left_catalog_dir"):
        MacauffArguments(
            output_path=tmp_path,
            output_artifact_name="object_to_source",
            tmp_dir=tmp_path,
            left_catalog_dir=test_data_dir,  # valid path, but not a catalog
            left_ra_column="ra",
            left_dec_column="dec",
            left_id_column="id",
            right_catalog_dir=catwise_dir,
            right_ra_column="source_ra",
            right_dec_column="source_dec",
            right_id_column="source_id",
            input_path=test_data_dir,
            input_format="csv",
            metadata_file_path=import_metadata_yaml,
        )


def test_macauff_args_right_invalid_catalog(catwise_dir, test_data_dir, import_metadata_yaml, tmp_path):
    with pytest.raises(ValueError, match="right_catalog_dir"):
        MacauffArguments(
            output_path=tmp_path,
            output_artifact_name="object_to_source",
            tmp_dir=tmp_path,
            left_catalog_dir=catwise_dir,
            left_ra_column="ra",
            left_dec_column="dec",
            left_id_column="id",
            right_catalog_dir=test_data_dir,  # valid directory with files, not a catalog
            right_ra_column="source_ra",
            right_dec_column="source_dec",
            right_id_column="source_id",
            input_path=test_data_dir,
            input_format="csv",
            metadata_file_path=import_metadata_yaml,
        )


def test_macauff_args_invalid_metadata_file(gaia_dir, catwise_dir, test_data_dir, tmp_path):
    with pytest.raises(ValueError, match="column metadata file must"):
        MacauffArguments(
            output_path=tmp_path,
            output_artifact_name="object_to_source",
            tmp_dir=tmp_path,
            left_catalog_dir=gaia_dir,
            left_ra_column="ra",
            left_dec_column="dec",
            left_id_column="id",
            right_catalog_dir=catwise_dir,
            right_ra_column="source_ra",
            right_dec_column="source_dec",
            right_id_column="source_id",
            input_path=test_data_dir,
            input_format="csv",
            metadata_file_path="ceci_n_est_pas_un_fichier.xml",
        )


def test_macauff_args_invalid_input_directory(gaia_dir, catwise_dir, import_metadata_yaml, tmp_path):
    with pytest.raises(FileNotFoundError, match="input_path not found"):
        MacauffArguments(
            output_path=tmp_path,
            output_artifact_name="object_to_source",
            tmp_dir=tmp_path,
            left_catalog_dir=gaia_dir,
            left_ra_column="ra",
            left_dec_column="dec",
            left_id_column="id",
            right_catalog_dir=catwise_dir,
            right_ra_column="source_ra",
            right_dec_column="source_dec",
            right_id_column="source_id",
            input_path="ceci_n_est_pas_un_directoire/",
            input_format="csv",
            metadata_file_path=import_metadata_yaml,
        )


def test_macauff_args_no_files(gaia_dir, catwise_dir, test_data_dir, import_metadata_yaml, tmp_path):
    with pytest.raises(FileNotFoundError, match="No input files found"):
        MacauffArguments(
            output_path=tmp_path,
            output_artifact_name="object_to_source",
            tmp_dir=tmp_path,
            left_catalog_dir=gaia_dir,
            left_ra_column="ra",
            left_dec_column="dec",
            left_id_column="id",
            right_catalog_dir=catwise_dir,
            right_ra_column="source_ra",
            right_dec_column="source_dec",
            right_id_column="source_id",
            input_path=test_data_dir,
            input_format="parquet",  # no files of this format will be found
            metadata_file_path=import_metadata_yaml,
        )
