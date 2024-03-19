from kfp import dsl, local  # type:ignore

local.init(runner=local.SubprocessRunner())


@dsl.component
def add(a: int, b: int) -> int:
    import logging

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.info(f"Hello from inside the pipeline, with message {a}, {b}")
    return a + b

@dsl.component
def add_2(a: int, b: int) -> list:
    return [a,b]

@dsl.component
def add_3(a: list) -> int:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    sum_ = 0
    for i in a:
        logger.info(f"Iterating through list, with element {i}")
        sum_ += i
    sum_ = int(sum_)
    return sum_

@dsl.component
def add_4(a: list) -> list:
    str_list = []
    for i in a:
        str_list.append(str(i))
    return str_list


# or run it in a pipeline
@dsl.pipeline
def number_sum_pipeline(x: int, y: int, z: int) -> list:
    t1 = add(a=x, b=y)  # type:ignore
    t2 = add(a=t1.output, b=z)  # type:ignore
    t3 = add_2(a=t2.output, b=t2.output)
    #t4 = add_3(a=t3.output)
    t4 = add_4(a=t3.output)
    return t4.output