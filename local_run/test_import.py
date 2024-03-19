from get_gcs_object_pipeline import get_gcs_object, print_gcs_objects
import kfp #type:ignore
from kfp import dsl

@dsl.pipeline
def mega_pipeline() -> None:
    task_1 = get_gcs_object(bucket_name="anton-test-bucket123")
    task_2 = print_gcs_objects(blob_list=task_1.output)

pipe = mega_pipeline()