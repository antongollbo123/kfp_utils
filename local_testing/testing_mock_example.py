from kfp.dsl import component, pipeline  # type:ignore
import typing
from kfp.dsl import Dataset, Output, Input, component, Model  # type:ignore
import pytest
import pandas as pd  # type:ignore


def make_test_artifact(artifact_type):
    class TestArtifact(artifact_type):  # type: ignore
        def _get_path(self):
            return super()._get_path() or self.uri

    return TestArtifact


@pytest.fixture(scope="session", name="input_data")
def input_dataset_artifact(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("artifact_store")
    uri = str(temp_dir / "input.csv")
    df = pd.DataFrame({"text": ["Hello world", "Goodbye world"]})
    df.to_csv(uri)
    output_dataset = make_test_artifact(Dataset)(uri=uri)
    return output_dataset


@pytest.fixture(scope="session", name="output_data")
def output_dataset_artifact(tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("artifact_store")
    uri = str(temp_dir / "output.csv")
    output_dataset = make_test_artifact(Dataset)(uri=uri)
    return output_dataset


@component(
    packages_to_install=[
        "pandas==1.3.4",
        "numpy==1.23.2",
    ],
    base_image="python:3.10.5-slim-bullseye",
)
def clean_text_data(input_art: Input[Dataset], output_art: Output[Dataset]) -> None:
    import pandas as pd
    import numpy as np

    df = pd.read_csv(input_art.path)

    df["text"] = df["text"].str.lower()
    df["text"] = df["text"].str.replace(r"[^\w\s]", "")
    df["text"] = df["text"].str.replace(r"\d+", "")
    df["text"] = df["text"].str.replace(r"\s+", " ")
    df["text"] = df["text"].str.strip()

    df.to_csv(output_art.path, index=False)


def test_clean_text_data_cleans_text_data(input_data, output_data):
    clean_text_data.python_func(
        input_art=input_data, output_art=output_data)
    df = pd.read_csv(output_data.path)
    breakpoint()
    assert df['text'].str.contains('hello world').any()
    assert df['text'].str.contains('goodbye world').any()