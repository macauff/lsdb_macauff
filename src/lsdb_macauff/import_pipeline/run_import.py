import os
import pickle

from hats.catalog import PartitionInfo
from hats.io import file_io, parquet_metadata, paths
from tqdm.auto import tqdm

from lsdb_macauff.import_pipeline.arguments import MacauffArguments
from lsdb_macauff.import_pipeline.map_reduce import reduce_associations, split_associations
from lsdb_macauff.import_pipeline.resume_plan import MacauffResumePlan


def run(args, client):
    """run macauff cross-match import pipeline"""
    if not args:
        raise TypeError("args is required and should be type MacauffArguments")
    if not isinstance(args, MacauffArguments):
        raise TypeError("args must be type MacauffArguments")

    resume_plan = MacauffResumePlan(args)

    if not resume_plan.is_splitting_done():
        pickled_reader_file = os.path.join(resume_plan.tmp_path, "reader.pickle")
        with open(pickled_reader_file, "wb") as pickle_file:
            pickle.dump(args.file_reader, pickle_file)
        futures = []
        for key, file_path in resume_plan.get_remaining_split_keys():
            futures.append(
                client.submit(
                    split_associations,
                    input_file=file_path,
                    pickled_reader_file=pickled_reader_file,
                    splitting_key=key,
                    highest_left_order=resume_plan.highest_left_order,
                    highest_right_order=resume_plan.highest_right_order,
                    left_alignment_file=resume_plan.left_alignment_file,
                    right_alignment_file=resume_plan.right_alignment_file,
                    left_ra_column=args.left_ra_column,
                    left_dec_column=args.left_dec_column,
                    right_ra_column=args.right_ra_column,
                    right_dec_column=args.right_dec_column,
                    tmp_path=args.tmp_path,
                )
            )
        resume_plan.wait_for_splitting(futures)

    if not resume_plan.is_reducing_done():
        futures = []
        for (
            left_pixel,
            pixel_key,
        ) in resume_plan.get_reduce_keys():
            futures.append(
                client.submit(
                    reduce_associations,
                    left_pixel=left_pixel,
                    tmp_path=args.tmp_path,
                    catalog_path=args.catalog_path,
                    reduce_key=pixel_key,
                )
            )

        resume_plan.wait_for_reducing(futures)

    # All done - write out the metadata
    with tqdm(total=4, desc="Finishing", disable=not args.progress_bar) as step_progress:
        parquet_metadata.write_parquet_metadata(args.catalog_path)
        total_rows = 0
        metadata_path = paths.get_parquet_metadata_pointer(args.catalog_path)
        for row_group in parquet_metadata.read_row_group_fragments(metadata_path):
            total_rows += row_group.num_rows
        partition_info = PartitionInfo.read_from_file(metadata_path)
        # partition_join_info.write_to_csv(catalog_path=args.catalog_path)
        step_progress.update(1)
        total_rows = int(total_rows)
        catalog_info = args.to_table_properties(total_rows)
        catalog_info.to_properties_file(args.catalog_path)
        step_progress.update(1)
        file_io.remove_directory(args.tmp_path, ignore_errors=True)
        step_progress.update(1)
