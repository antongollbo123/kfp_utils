

import kfp  #type:ignore
from kfp import compiler, dsl, local

local.init(runner=local.DockerRunner())

@dsl.component(base_image="custom_python:latest")
def print_hello(message: str) -> None:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info(f"Hello from inside the pipeline, with message {message}")
    #return message

@dsl.pipeline
def logging_pipeline(message: str) -> None:
    """My test pipeline"""
    print_hello(message=message) # type: ignore

pipeline = logging_pipeline(message="hello from the other side")
#pipeline_file = "logging_pipeline.yaml"
#compiler.Compiler().compile(logging_pipeline,  # type: ignore
#                            pipeline_file)

#client = kfp.Client()
#client.create_run_from_pipeline_package(pipeline_file=pipeline_file,
#                                        arguments={},
#                                        experiment_name="Logging pipeline experiment",
#                                        namespace=None, )