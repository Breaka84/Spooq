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
        .config("spark.driver.memory", "1500m")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "512m")
        # based on medium blog post at
        # https://medium.com/constructor-engineering/faster-pyspark-unit-tests-1cb7dfa6bdf6
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedJobs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        .config(
            "spark.driver.extraJavaOptions",
            " ".join(
                [
                    "-Ddelta.log.cacheSize=3",
                    "-XX:+CMSClassUnloadingEnabled",
                    "-XX:+UseCompressedOops",
                ]
            ),
        )
    )
    # https://docs.delta.io/latest/api/python/index.html#delta.pip_utils.configure_spark_with_delta_pip
    return configure_spark_with_delta_pip(spark_builder).getOrCreate()
