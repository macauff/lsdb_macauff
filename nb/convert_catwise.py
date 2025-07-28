import numpy as np

from dask.distributed import Client
from hats_import.hipscat_conversion.arguments import ConversionArguments
from hats_import.index.arguments import IndexArguments
from hats_import.pipeline import pipeline_with_client


def convert_catwise2020():
    with Client(
        n_workers=12,
        threads_per_worker=1,
        local_directory="/epyc/data3/hats/tmp/",
    ) as client:
        args = ConversionArguments(
            input_catalog_path=f"/epyc/projects3/sam_hipscat/catwise2020/catwise2020",
            output_path="/epyc/data3/hats/tmp/catwise",
            output_artifact_name="catwise2020",
            completion_email_address="scampos@andrew.cmu.edu",
        )
        pipeline_with_client(args, client)

if __name__ == "__main__":
    convert_catwise2020()
