from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def create_spark_session_for_tests() -> SparkSession:
    spark_builder = (
        SparkSession.builder.appName("Spooq Tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraClassPath", "../bin/custom_jars/sqlite-jdbc.jar")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraClassPath", "../bin/custom_jars/sqlite-jdbc.jar")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    )
    # https://docs.delta.io/latest/api/python/index.html#delta.pip_utils.configure_spark_with_delta_pip
    return configure_spark_with_delta_pip(spark_builder).getOrCreate()
