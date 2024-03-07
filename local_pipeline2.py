import json

from google.cloud import storage  # type:ignore
from google.oauth2 import service_account  # type:ignore

# from google.cloud import storage  #type:ignore
# from google.oauth2 import service_account  #type:ignore
from kfp import compiler, dsl, local  # type:ignore
from kfp.dsl import Artifact, Output  # type:ignore

local.init(runner=local.DockerRunner())


@dsl.component(base_image="custom_python:latest", packages_to_install=["numpy"])
def upload_to_gcs(
    bucket_name: str,
    source_file_name: str,
    destination_blob_name: str,
    out_artifact: Output[Artifact],
) -> str:
    import logging

    import numpy as np
    from google.cloud import storage  # type:ignore
    from google.oauth2 import service_account  # type:ignore

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    try:
        credentials = service_account.Credentials.from_service_account_file(
            "credentials.json"
        )
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        logger.info(np.abs(1))
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        print(
            f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}"
        )
        #out_artifact.metadata["test"] = "test123"
        logger.info("Success!")
    except Exception as e:
        logger.error(f"Error has occured: {e}")
    return "hello"


@dsl.component(base_image="python:3.8")
def add(a: int, b: int, out_artifact: Output[Artifact]):
    import json
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    result = json.dumps(a + b)
    logger.info(f"Hello from inside the pipeline, with message {a}, {b}")

    with open(out_artifact.path, "w") as f:
        f.write(result)

    out_artifact.metadata["operation"] = "addition"
    out_artifact.metadata["test"] = "test123"

"""
task = upload_to_gcs(
    bucket_name="new_bucket123123213",
    source_file_name="sample_file.txt",
    destination_blob_name="sample_file.txt",
)  # type:ignore
"""
@dsl.pipeline()
def gcs_pipeline() -> None:
    upload_to_gcs(bucket_name="new_bucket123123213", 
                         source_file_name="sample_file.txt",
                         destination_blob_name="sample_file.txt"
                         ) # type:ignore



pipeline_file = "test_pipeline.yaml"
compiler.Compiler().compile(
    gcs_pipeline,  # type: ignore
    pipeline_file,
)

# can read artifact contents
#with open(task.outputs["out_artifact"].path) as f:
#    contents = f.read()
