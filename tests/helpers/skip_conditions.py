import pytest
from pyspark.sql import SparkSession
import pytest_spark

# warm up spark_session to be able to import Spark functions
spark_conf = pytest_spark.config.SparkConfigBuilder.initialize()
spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()

only_spark2 = pytest.mark.skipif(
    spark_session.version.split(".")[0] != 2, reason="This test supports only Spark 2"
)

only_spark3 = pytest.mark.skipif(
    spark_session.version.split(".")[0] != 3, reason="This test supports only Spark 3"
)
