import json

from google.cloud import storage  # type:ignore
from google.oauth2 import service_account  # type:ignore

# from google.cloud import storage  #type:ignore
# from google.oauth2 import service_account  #type:ignore
from kfp import compiler, dsl, local  # type:ignore
from kfp.dsl import Artifact, Output  # type:ignore

local.init(runner=local.DockerRunner())


@dsl.component(
    base_image="custom_python:latest",
    packages_to_install=["numpy", "pandas", "pyarrow"],
)
def upload_to_gcs(
    bucket_name: str,
    source_file_name: str,
    destination_blob_name: str,
    out_artifact: Output[Artifact],
) -> None:
    """
    Custom Python image has 'google' package installed, but not numpy.
    Further, GCP credentials are mounted into this image.
    """
    import logging
    import os

    import pandas as pd  # type:ignore
    from google.cloud import storage  # type:ignore
    from google.oauth2 import service_account  # type:ignore

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    try:
        credentials = service_account.Credentials.from_service_account_file(
            "credentials.json"
        )
        data = {
            "Name": ["Alice", "Bob", "Charlie", "David"],
            "Age": [25, 30, 35, 40],
            "Gender": ["Female", "Male", "Male", "Male"],
        }
        df = pd.DataFrame(data)
        df.to_parquet(source_file_name)
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        logger.info(os.getcwd())
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(
            f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}"
        )
        logger.info("Success!")
        out_artifact.metadata["example_metadata"] = "example_metadata"
    except Exception as e:
        logger.error(f"Error has occured: {e}")


task = upload_to_gcs(
    bucket_name="new_bucket123123213",
    source_file_name="example_file.parquet",
    destination_blob_name="example_file.parquet",
)  # type:ignore
