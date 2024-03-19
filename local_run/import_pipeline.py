from number_sum_pipeline import number_sum_pipeline
from kfp.dsl import pipeline # type:ignore
from kfp import dsl, local  # type:ignore
from google.cloud import bigquery 
local.init(runner=local.SubprocessRunner())


@pipeline
def double_pipeline() -> None:
    pipeline1 = number_sum_pipeline(x=1, y=2, z=3)
    pipeline2 = number_sum_pipeline(x=2, y=3, z=4).after(pipeline1)


pipe = double_pipeline()