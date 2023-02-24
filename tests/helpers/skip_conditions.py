import pytest
from tests.conftest import create_spark_session_for_tests

# # warm up spark_session to be able to import Spark functions
spark_session = create_spark_session_for_tests()


only_spark2 = pytest.mark.skipif(
    spark_session.version.split(".")[0] != "2", reason="This test supports only Spark 2"
)

only_spark3 = pytest.mark.skipif(
    spark_session.version.split(".")[0] != "3", reason="This test supports only Spark 3"
)
