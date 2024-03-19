import json
from typing import Any, List

import pandas as pd  # type:ignore
import pyarrow  # type:ignore
from google.cloud import storage  # type:ignore
from google.oauth2 import service_account  # type:ignore

# from google.cloud import storage  #type:ignore
# from google.oauth2 import service_account  #type:ignore
from kfp import compiler, dsl, local  # type:ignore
from kfp.dsl import Artifact, Dataset, Input, Output #type:ignore
import trace

local.init(runner=local.DockerRunner())


@dsl.component(
    base_image="custom_python:latest",
    packages_to_install=["numpy", "pyarrow", "pandas"],
)
def read_from_gcs(
    bucket_name: str, blob_name: str, output_dataset: Output[Dataset]
) -> None:
    import logging
    import os

    import pandas as pd  # type:ignore
    import pyarrow.parquet as pq  # type:ignore
    from google.cloud import storage  # type:ignore

    from google.oauth2 import service_account  # type:ignore

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
        gcs_path = f"gs://{bucket_name}/{blob_name}"
        table = pq.read_table(gcs_path)
        logger.info(f"Succesfully read {table}")
        local_path = "output_file.parquet"
        output_dataset.name = "output_file"

        df = table.to_pandas()
        new_df = df.drop(columns=["Gender"])
        new_df.to_parquet(local_path)

        client = storage.Client()
        bucket = client.bucket(bucket_name+2)
        blob = bucket.blob(f"{output_dataset.name}.parquet")
        blob.upload_from_filename(local_path)

        os.remove(local_path)
        output_dataset.path = f"gs://{bucket_name}/{output_dataset.name}.parquet"
        
        logger.info(f"Succesfully transformed to pandas df: {output_dataset.path},  ")
    except Exception as e:
        logger.error(f"Error has occured: {e}")
        logger.exception(e)


@dsl.component(
    base_image="custom_python:latest", packages_to_install=["pandas", "pyarrow"]
)
def do_df_operation(input_dataset: Input[Dataset]) -> str:
    import logging
    import os

    import pandas as pd
    import pyarrow.parquet as pq

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()

    try:
        logger.info(input_dataset.uri)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
        table = pq.read_table(input_dataset.uri)
        logger.info(f"Succesfully read {table}")
        logger.info(f"Path is equal to: {input_dataset.path}, type: {type(input_dataset.path)}")
        logger.info(f"URI is equal to: {input_dataset.uri}, type: {type(input_dataset.uri)}")
        logger.info(f"Metadata is equal to: {input_dataset.metadata}, type: {type(input_dataset.metadata)}")
        logger.info(f"Name is equal to: {input_dataset.name}, type: {type(input_dataset.name)}")
        df = table.to_pandas()
        return_val = df.iloc[0].Name
    except Exception as e:
        logger.error(f"Error {e} caught...")
    return return_val


@dsl.pipeline
def df_pass_pipeline(bucket_name: str, blob_name: str) -> None:
    task1 = read_from_gcs(bucket_name=bucket_name, blob_name=blob_name)  # type:ignore
    task2 = do_df_operation(input_dataset=task1.outputs["output_dataset"])


pipeline = df_pass_pipeline(
    bucket_name="new_bucket123123213", blob_name="example_file.parquet"
)