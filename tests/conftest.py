import pytest


@pytest.fixture(scope="session")
def spark_major_version(spark_session):
    return spark_session.version.split(".")[0]
