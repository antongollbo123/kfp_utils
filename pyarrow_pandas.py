import logging
import os

import pandas  #type:ignore
import pandas as pd  #type:ignore
import pyarrow  #type:ignore
import pyarrow.parquet as pq  #type:ignore
from google.cloud import storage  # type:ignore
from google.oauth2 import service_account  # type:ignore


def get_df() -> pd.DataFrame:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    bucket_name = "new_bucket123123213"
    blob_name = "example_file.parquet"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"
    gcs_path = f"gs://{bucket_name}/{blob_name}"
    table = pq.read_table(gcs_path)
    logger.info(f"Succesfully read {table}")
    df = table.to_pandas()
    logger.info(f"Succesfully transformed to pandas df: {df.head}")
    return df
df = get_df()
breakpoint()
    
    