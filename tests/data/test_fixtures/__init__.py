from pyspark.sql import SparkSession
import pytest_spark

# warm up spark_session to be able to import Spark functions
spark_conf = pytest_spark.config.SparkConfigBuilder.initialize()
SparkSession.builder.config(conf=spark_conf).getOrCreate()
