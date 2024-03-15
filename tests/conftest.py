from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark import SparkContext

from tests.helpers.spark_session import create_spark_session_for_tests


@pytest.fixture(scope="session", autouse=True)
def spark_session() -> SparkSession:
    return create_spark_session_for_tests()


@pytest.fixture(scope="session")
def spark_context(spark_session: SparkSession) -> SparkContext:
    return spark_session.sparkContext

def get_data_folder() -> Path:
    return Path(__file__).parent.joinpath("data").resolve()
