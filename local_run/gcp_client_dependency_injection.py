import json
from typing import Any, List
import logging

import pandas as pd  # type:ignore
import pyarrow  # type:ignore
from google.cloud import storage, client  # type:ignore
from google.oauth2 import service_account  # type:ignore

# from google.cloud import storage  #type:ignore
# from google.oauth2 import service_account  #type:ignore
from kfp import compiler, dsl, local  # type:ignore
from kfp.dsl import Artifact, Dataset, Input, Output, OutputPath  # type:ignore
from google.cloud.storage import Client

@dsl.component(
    base_image="python:3.11",
    packages_to_install=["numpy", "google"],
)
def component_inject_client(client: Client) -> list:
    from google.cloud.storage import Client
    bucket = client.bucket(bucket_name="anton-test-bucket123")
    blob_list = bucket.list_blobs()
    blob_name_list = []
    for blob in blob_list:
        blob_name_list.append(blob.name)
    return blob_list




@dsl.pipeline
def dep_injection_pipeline() -> None:
    client = storage.Client.from_service_account_json("credentials.json")
    output_list = component_inject_client(client=client)
    print(output_list)

pipe = dep_injection_pipeline()