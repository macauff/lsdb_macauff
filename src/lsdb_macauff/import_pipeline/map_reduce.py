import pickle

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hats.io import file_io, paths
from hats_import.catalog.map_reduce import _iterate_input_file
from hats_import.pipeline_resume_plan import get_pixel_cache_directory, print_task_failure

from lsdb_macauff.import_pipeline.resume_plan import MacauffResumePlan

# pylint: disable=too-many-arguments,too-many-locals


def split_associations(
    input_file,
    pickled_reader_file,
    splitting_key,
    highest_left_order,
    left_alignment_file,
    left_ra_column,
    left_dec_column,
    tmp_path,
):
    """Map a file of links to their healpix pixels and split into shards.


    Raises:
        ValueError: if the `ra_column` or `dec_column` cannot be found in the input file.
        FileNotFoundError: if the file does not exist, or is a directory
    """
    try:
        with open(left_alignment_file, "rb") as pickle_file:
            left_alignment = pickle.load(pickle_file)

        for chunk_number, data, mapped_left_pixels in _iterate_input_file(
            input_file, pickled_reader_file, highest_left_order, left_ra_column, left_dec_column, False
        ):
            aligned_left_pixels = left_alignment[mapped_left_pixels]
            unique_pixels, unique_inverse = np.unique(aligned_left_pixels, return_inverse=True)

            for unique_index, pixel in enumerate(unique_pixels):
                mapped_indexes = np.where(unique_inverse == unique_index)
                data_indexes = data.index[mapped_indexes[0].tolist()]

                filtered_data = data.filter(items=data_indexes, axis=0)

                pixel_dir = get_pixel_cache_directory(tmp_path, pixel)
                file_io.make_directory(pixel_dir, exist_ok=True)
                output_file = file_io.append_paths_to_pointer(
                    pixel_dir, f"shard_{splitting_key}_{chunk_number}.parquet"
                )
                if isinstance(data, pd.DataFrame):
                    filtered_data = data.iloc[unique_inverse == unique_index]
                    filtered_data = pa.Table.from_pandas(
                        filtered_data, preserve_index=False
                    ).replace_schema_metadata()
                else:
                    filtered_data = data.filter(unique_inverse == unique_index)

                pq.write_table(filtered_data, output_file.path, filesystem=output_file.fs)
                del filtered_data

        MacauffResumePlan.splitting_key_done(tmp_path=tmp_path, splitting_key=splitting_key)
    except Exception as exception:  # pylint: disable=broad-exception-caught
        print_task_failure(f"Failed SPLITTING stage with file {input_file}", exception)
        raise exception


def reduce_associations(left_pixel, tmp_path, catalog_path, reduce_key):
    """For all points determined to be in the target left_pixel, map them to the appropriate right_pixel
    and aggregate into a single parquet file."""
    try:
        inputs = get_pixel_cache_directory(tmp_path, left_pixel)

        if not file_io.directory_has_contents(inputs):
            MacauffResumePlan.reducing_key_done(
                tmp_path=tmp_path, reducing_key=f"{left_pixel.order}_{left_pixel.pixel}"
            )
            print(f"Warning: no input data for pixel {left_pixel}")
            return
        destination_dir = paths.pixel_directory(catalog_path, left_pixel.order, left_pixel.pixel)
        file_io.make_directory(destination_dir, exist_ok=True)

        destination_file = paths.pixel_catalog_file(catalog_path, left_pixel)

        merged_table = pq.read_table(inputs)
        pq.write_table(merged_table, destination_file.path, filesystem=destination_file.fs)
        MacauffResumePlan.reducing_key_done(tmp_path=tmp_path, reducing_key=reduce_key)
    except Exception as exception:  # pylint: disable=broad-exception-caught
        print_task_failure(
            f"Failed REDUCING stage for shard: {left_pixel.order} {left_pixel.pixel}",
            exception,
        )
        raise exception
