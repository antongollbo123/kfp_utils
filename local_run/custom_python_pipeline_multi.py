import json
from typing import Any, List
import logging

import pandas as pd  # type:ignore
import pyarrow  # type:ignore
from google.cloud import storage  # type:ignore
from google.oauth2 import service_account  # type:ignore

# from google.cloud import storage  #type:ignore
# from google.oauth2 import service_account  #type:ignore
from kfp import compiler, dsl, local  # type:ignore
from kfp.dsl import Artifact, Dataset, Input, Output, OutputPath  # type:ignore

local.init(runner=local.DockerRunner())


@dsl.component(
    base_image="python:3.11",
    packages_to_install=["numpy", "pyarrow", "pandas"],
)
def save_df(output_dataset: Output[Dataset]
) -> Dataset:
    
    import logging
    import os

    import pandas as pd  # type:ignore
    import pyarrow.parquet as pq  # type:ignore

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    data1 = {'Name': ['Alice', 'Bob', 'Charlie'],
         'Age': [25, 30, 35],
         'City': ['New York', 'Los Angeles', 'Chicago']}
    df1 = pd.DataFrame(data1)

    # Creating the second example DataFrame
    data2 = {'Product': ['Laptop', 'Phone', 'Tablet'],
            'Price': [1200, 800, 500],
            'Stock': [10, 20, 15]}
    df2 = pd.DataFrame(data2)
    
    df1.to_csv(output_dataset.path, header=True, index=False)
    df2.to_csv(output_dataset.path, header=True, index=False)
    
    logger.info(f"Succesfully saved df to disk at location {output_dataset.path}!")

@dsl.component(
    base_image="python:3.11",
    packages_to_install=["numpy", "pyarrow", "pandas"],
)
def read_df(input_dataset: Input[Dataset]) -> None:
    import pandas as pd
    import logging
    df1  = pd.read_csv(input_dataset.path)
    return_val = f"DF_SHAPE: {df1.shape[0]}"
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)
    logger.info(input_dataset.path)
    logger.info(input_dataset.uri)


@dsl.pipeline
def df_pass_pipeline() -> None:
    task1 = save_df()  # type:ignore
    task2 = read_df(input_dataset=task1.outputs["output_dataset"])

pipe_df = df_pass_pipeline()
