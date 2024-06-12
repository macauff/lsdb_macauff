"""Utility to hold the file-level pipeline execution plan."""

from __future__ import annotations

import pickle
from dataclasses import dataclass, field
from typing import List

import healpy as hp
import numpy as np
from hipscat.catalog import Catalog
from hipscat.io import FilePointer, file_io
from hipscat.pixel_math.healpix_pixel import HealpixPixel
from hipscat_import.pipeline_resume_plan import PipelineResumePlan
from tqdm.auto import tqdm

from lsdb_macauff.import_pipeline.arguments import MacauffArguments


@dataclass
class MacauffResumePlan(PipelineResumePlan):
    """Container class for holding the state of each file in the pipeline plan."""

    input_paths: List[FilePointer] = field(default_factory=list)
    """resolved list of all files that will be used in the importer"""
    left_pixels: List[HealpixPixel] = field(default_factory=list)
    highest_left_order: int = 0
    highest_right_order: int = 0
    left_alignment_file: str = None
    right_alignment_file: str = None

    SPLITTING_STAGE = "splitting"
    REDUCING_STAGE = "reducing"
    LEFT_ALIGNMENT_FILE = "left_alignment.pickle"
    RIGHT_ALIGNMENT_FILE = "right_alignment.pickle"

    def __init__(self, args: MacauffArguments):
        if not args.tmp_path:  # pragma: no cover (not reachable, but required for mypy)
            raise ValueError("tmp_path is required")
        super().__init__(resume=args.resume, progress_bar=args.progress_bar, tmp_path=args.tmp_path)
        self.input_paths = args.input_paths
        self.gather_plan(args)

    def gather_plan(self, args):
        """Initialize the plan."""
        ## Make sure it's safe to use existing resume state.
        super().safe_to_resume()

        with tqdm(total=4, desc="Planning", disable=not args.progress_bar) as step_progress:
            ## Validate existing resume state.
            ## - if a later stage is complete, the earlier stages should be complete too.
            splitting_done = self.is_splitting_done()
            reducing_done = self.is_reducing_done()

            if reducing_done and not splitting_done:
                raise ValueError("splitting must be complete before reducing")

            ## Validate that we're operating on the same file set as the previous instance.
            self.input_paths = self.check_original_input_paths(self.input_paths)

            left_catalog = Catalog.read_from_hipscat(args.left_catalog_dir)
            step_progress.update(1)
            right_catalog = Catalog.read_from_hipscat(args.right_catalog_dir)
            step_progress.update(1)
            self.highest_left_order = left_catalog.partition_info.get_highest_order()
            self.highest_right_order = right_catalog.partition_info.get_highest_order()

            self.left_pixels = left_catalog.partition_info.get_healpix_pixels()
            right_pixels = right_catalog.partition_info.get_healpix_pixels()

            ## Gather keys for execution.
            if not splitting_done:
                self.left_alignment_file = file_io.append_paths_to_pointer(
                    self.tmp_path, self.LEFT_ALIGNMENT_FILE
                )
                self.right_alignment_file = file_io.append_paths_to_pointer(
                    self.tmp_path, self.RIGHT_ALIGNMENT_FILE
                )
                if not file_io.does_file_or_directory_exist(
                    self.left_alignment_file
                ) or not file_io.does_file_or_directory_exist(self.right_alignment_file):
                    regenerated_left_alignment = np.full(hp.order2npix(self.highest_left_order), None)
                    for pixel in self.left_pixels:
                        explosion_factor = 4 ** (self.highest_left_order - pixel.order)
                        exploded_pixels = np.arange(
                            pixel.pixel * explosion_factor,
                            (pixel.pixel + 1) * explosion_factor,
                        )
                        regenerated_left_alignment[exploded_pixels] = pixel
                    regenerated_right_alignment = np.full(hp.order2npix(self.highest_right_order), None)
                    for pixel in right_pixels:
                        explosion_factor = 4 ** (self.highest_right_order - pixel.order)
                        exploded_pixels = np.arange(
                            pixel.pixel * explosion_factor,
                            (pixel.pixel + 1) * explosion_factor,
                        )
                        regenerated_right_alignment[exploded_pixels] = pixel

                    with open(self.left_alignment_file, "wb") as pickle_file:
                        pickle.dump(regenerated_left_alignment, pickle_file)

                    with open(self.right_alignment_file, "wb") as pickle_file:
                        pickle.dump(regenerated_right_alignment, pickle_file)

            step_progress.update(1)

            if not reducing_done:
                self.reduce_keys = self.get_reduce_keys()
            ## Go ahead and create our directories for storing resume files.
            file_io.make_directory(
                file_io.append_paths_to_pointer(self.tmp_path, self.SPLITTING_STAGE),
                exist_ok=True,
            )
            file_io.make_directory(
                file_io.append_paths_to_pointer(self.tmp_path, self.REDUCING_STAGE),
                exist_ok=True,
            )
            step_progress.update(1)

    def get_remaining_split_keys(self):
        """Gather remaining keys, dropping successful split tasks from done file names.

        Returns:
            list of splitting keys *not* found in files like /resume/path/split_key.done
        """
        split_keys = set(self.read_done_keys(self.SPLITTING_STAGE))
        return [
            (f"split_{i}", file_path)
            for i, file_path in enumerate(self.input_paths)
            if f"split_{i}" not in split_keys
        ]

    @classmethod
    def splitting_key_done(cls, tmp_path, splitting_key: str):
        """Mark a single splitting task as done

        Args:
            tmp_path (str): where to write intermediate resume files.
            splitting_key (str): unique string for each splitting task (e.g. "split_57")
        """
        cls.touch_key_done_file(tmp_path, cls.SPLITTING_STAGE, splitting_key)

    @classmethod
    def reducing_key_done(cls, tmp_path, reducing_key: str):
        """Mark a single reducing task as done

        Args:
            tmp_path (str): where to write intermediate resume files.
            reducing_key (str): unique string for each reducing task (e.g. "3_57")
        """
        cls.touch_key_done_file(tmp_path, cls.REDUCING_STAGE, reducing_key)

    def wait_for_splitting(self, futures):
        """Wait for splitting futures to complete."""
        self.wait_for_futures(futures, self.SPLITTING_STAGE)
        remaining_split_items = self.get_remaining_split_keys()
        if len(remaining_split_items) > 0:
            raise RuntimeError(f"{len(remaining_split_items)} split stages did not complete successfully.")
        self.touch_stage_done_file(self.SPLITTING_STAGE)

    def is_splitting_done(self) -> bool:
        """Are there files left to split?"""
        return self.done_file_exists(self.SPLITTING_STAGE)

    def get_reduce_keys(self):
        """Fetch a Tuple for each partition to reduce.

        Tuple contains:

        - left pixel (healpix pixel with both order and pixel)
        - reduce key (string of left order+pixel)

        """
        reduced_keys = set(self.read_done_keys(self.REDUCING_STAGE))
        reduce_items = [
            (hp_pixel, f"{hp_pixel.order}_{hp_pixel.pixel}")
            for hp_pixel in self.left_pixels
            if f"{hp_pixel.order}_{hp_pixel.pixel}" not in reduced_keys
        ]
        return reduce_items

    def is_reducing_done(self) -> bool:
        """Are there partitions left to reduce?"""
        return self.done_file_exists(self.REDUCING_STAGE)

    def wait_for_reducing(self, futures):
        """Wait for reducing futures to complete."""
        self.wait_for_futures(futures, self.REDUCING_STAGE)
        remaining_reduce_items = self.get_reduce_keys()
        if len(remaining_reduce_items) > 0:
            raise RuntimeError(f"{len(remaining_reduce_items)} reduce stages did not complete successfully.")
        self.touch_stage_done_file(self.REDUCING_STAGE)
