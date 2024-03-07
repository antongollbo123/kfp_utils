from kfp import dsl, local  # type:ignore

local.init(runner=local.SubprocessRunner())


@dsl.component
def add(a: int, b: int) -> int:
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info(f"Hello from inside the pipeline, with message {a}, {b}")
    return a + b


# run a single component
task = add(a=1, b=2)  # type:ignore
assert task.output == 3


# or run it in a pipeline
@dsl.pipeline
def math_pipeline(x: int, y: int, z: int) -> int:
    t1 = add(a=x, b=y)  # type:ignore
    t2 = add(a=t1.output, b=z)  # type:ignore
    return t2.output  # type:ignore


pipeline_task = math_pipeline(x=1, y=2, z=3)
assert pipeline_task.output == 6
