import json

from google.cloud import storage  # type:ignore
from google.oauth2 import service_account  # type:ignore

# from google.cloud import storage  #type:ignore
# from google.oauth2 import service_account  #type:ignore
from kfp import compiler, dsl, local  # type:ignore
from kfp.dsl import Artifact, Output  # type:ignore
import jsonargparse

local.init(runner=local.DockerRunner())


@dsl.component(
    base_image="custom_python:latest",
    packages_to_install=["numpy", "pandas", "pyarrow"],
)
def get_gcs_object(
    bucket_name: str
) -> list:
    """
    Custom Python image has 'google' package installed, but not numpy.
    Further, GCP credentials are mounted into this image.
    """
    import logging
    from google.cloud import storage  # type:ignore
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    try:
        bucket_name = "anton-test-bucket123"
        client = storage.Client.from_service_account_json("credentials.json")
        bucket = client.bucket(bucket_name=bucket_name)
        blob_list = bucket.list_blobs()
        blob_name_list = []
        for blob in blob_list:
            blob_name_list.append(f"{blob.name}")

    except Exception as e:
        logger.error(f"Error has occured: {e}")
    return blob_name_list

@dsl.component(
    base_image="custom_python:latest",
    packages_to_install=["numpy", "pandas", "pyarrow"],
)

def print_gcs_objects(blob_list: list, bucket_name: str) -> None:
    import pandas as pd
    from google.cloud import storage
    from io import BytesIO
    client = storage.Client.from_service_account_json("credentials.json")
    bucket = client.bucket(bucket_name=bucket_name)
    for blob in blob_list: 
        blob = bucket.get_blob(blob)
        content = blob.download_as_string()
        print(content)

#@dsl.pipeline
def pipeline_orchestrator(bucket_name: str, 
                          pipeline_name: str,
                          upload_path: str
                          ) -> None:
    print(pipeline_name)
    if pipeline_name == "mega_pipeline":
        print("true")
        @dsl.pipeline
        def mega_pipeline() -> None:
            task_1 = get_gcs_object(bucket_name=bucket_name)
            print(task_1.output)
            task_2 = print_gcs_objects(blob_list=task_1.output, bucket_name=bucket_name)
        def mega_pipeline2() -> None:
            task_1 = get_gcs_object(bucket_name=bucket_name)
        pipeline = mega_pipeline()

if __name__ == "__main__":
    jsonargparse.CLI(pipeline_orchestrator, as_positional=False)
    #pipe = gcs_obj_pipeline(bucket_name="anton-test-bucket123")
