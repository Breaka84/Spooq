from builtins import chr
from builtins import str
from builtins import object
import pytest
from copy import deepcopy
from doubles import expect
from pyspark.sql.functions import udf as spark_udf
from pyspark.sql.functions import lit
from pyspark.sql import types as sql_types
from py4j.protocol import Py4JJavaError

from tests import DATA_FOLDER
from spooq.loader import HiveLoader


@pytest.fixture(scope="function")
def default_params():
    return {
        "db_name": "test_hive_loader",
        "table_name": "test_partitioned",
        "repartition_size": 2,
        "clear_partition": True,
        "auto_create_table": True,
        "overwrite_partition_value": True,
        "partition_definitions": [
            {"column_name": "partition_key_int", "column_type": "IntegerType", "default_value": 7}
        ],
    }


@pytest.fixture()
def full_table_name(default_params):
    return "{db}.{tbl}".format(db=default_params["db_name"], tbl=default_params["table_name"])


@pytest.fixture()
def default_loader(default_params):
    return HiveLoader(**default_params)


@spark_udf
def convert_int_to_ascii_char(input):
    return chr(input)


def construct_partition_query(partition_definitions):
    partition_queries = []
    for dct in partition_definitions:
        if issubclass(dct["column_type"], sql_types.NumericType):
            partition_queries.append("{part} = {dt}".format(part=dct["column_name"], dt=dct["default_value"]))
        else:
            partition_queries.append("{part} = '{dt}'".format(part=dct["column_name"], dt=dct["default_value"]))
    return ", ".join(partition_queries)


class TestBasicAttributes(object):
    def test_logger_should_be_accessible(self, default_loader):
        assert hasattr(default_loader, "logger")

    def test_name_is_set(self, default_loader):
        assert default_loader.name == "HiveLoader"

    def test_str_representation_is_correct(self, default_loader):
        assert str(default_loader) == "Loader Object of Class HiveLoader"


class TestSinglePartitionColumn(object):
    @pytest.fixture()
    def input_df(self, spark_session, default_params, full_table_name):

        df = spark_session.read.parquet(f"{DATA_FOLDER}/schema_v1/parquetFiles")
        df = df.withColumn("partition_key_int", df.meta.version % 10)  # 0-9
        spark_session.conf.set("hive.exec.dynamic.partition", "true")
        spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark_session.sql("DROP DATABASE IF EXISTS {db} CASCADE".format(db=default_params["db_name"]))
        spark_session.sql("CREATE DATABASE {db}".format(db=default_params["db_name"]))
        df.write.partitionBy("partition_key_int").saveAsTable(full_table_name)
        yield df
        spark_session.sql("DROP DATABASE IF EXISTS {db} CASCADE".format(db=default_params["db_name"]))

    class TestWarnings(object):
        def test_more_columns_than_expected(self, input_df, default_loader):
            df_to_load = input_df.withColumn("5th_wheel", lit(12345))
            with pytest.raises(AssertionError) as excinfo:
                default_loader.load(df_to_load)
            assert "Input columns don't match the columns of the Hive table" in str(excinfo.value)

        def test_less_columns_than_expected(self, input_df, default_loader):
            df_to_load = input_df.drop("birthday")
            with pytest.raises(AssertionError) as excinfo:
                default_loader.load(df_to_load)
            assert "Input columns don't match the columns of the Hive table" in str(excinfo.value)

        def test_different_columns_order_than_expected(self, input_df, default_loader):
            df_to_load = input_df.select(list(reversed(input_df.columns)))
            with pytest.raises(AssertionError) as excinfo:
                default_loader.load(df_to_load)
            assert "Input columns don't match the columns of the Hive table" in str(excinfo.value)

    class TestClearPartition(object):
        """Clearing the Hive Table Partition before inserting"""

        @pytest.mark.parametrize("partition", [0, 2, 3, 6, 9])
        def test_clear_partition(self, spark_session, input_df, partition, default_params, full_table_name):
            """Partition is dropped"""
            default_params["partition_definitions"][0]["default_value"] = partition
            loader = HiveLoader(**default_params)
            partition_query = construct_partition_query(loader.partition_definitions)
            inverted_partition_query = partition_query.replace("=", "!=").replace(", ", " and ")
            expected_count = input_df.where("partition_key_int != " + str(partition)).count()
            loader._clear_hive_partition()
            actual_count = spark_session.table(full_table_name).count()

            assert actual_count == expected_count

        def test_clear_partition_is_called_exactly_once(self, default_loader, input_df):
            """Clear Partition is called exactly once (Default)"""
            expect(default_loader)._clear_hive_partition.exactly(1).time
            default_loader.load(input_df)

        def test_clear_partition_is_not_called(self, default_loader, input_df):
            """Clear Partition is not called (Default Values was Overridden)"""
            default_loader.clear_partition = False
            expect(default_loader)._clear_hive_partition.exactly(0).time
            default_loader.load(input_df)

    class TestPartitionDefinitions(object):
        @pytest.mark.parametrize(
            "partition_definitions", ["Some string", 123, 75.0, b"abcd", ("Hello", "World"), {"Nice_to": "meet_you"}]
        )
        def test_input_is_not_a_list(self, partition_definitions, default_params):
            default_params["partition_definitions"] = partition_definitions
            with pytest.raises(AssertionError) as excinfo:
                HiveLoader(**default_params)
            assert "partition_definitions has to be a list containing dicts" in str(excinfo.value)

        @pytest.mark.parametrize("partition_definitions", ["Some string", 123, 75.0, b"abcd", ("Hello", "World")])
        def test_list_input_contains_non_dict_items(self, partition_definitions, default_params):
            default_params["partition_definitions"] = [partition_definitions]
            with pytest.raises(AssertionError) as excinfo:
                HiveLoader(**default_params)
            assert "Items of partition_definitions have to be dictionaries" in str(excinfo.value)

        def test_column_name_is_missing(self, default_params):
            default_params["partition_definitions"][0]["column_name"] = None
            with pytest.raises(AssertionError) as excinfo:
                HiveLoader(**default_params)
            assert "No column name set!" in str(excinfo.value)

        @pytest.mark.parametrize("data_type", [13, "no_spark_type", "arrray", "INT", ["IntegerType", "StringType"]])
        def test_column_type_not_a_valid_spark_sql_type(self, data_type, default_params):
            default_params["partition_definitions"][0]["column_type"] = data_type
            with pytest.raises(AssertionError) as excinfo:
                HiveLoader(**default_params)
            assert "Not a valid (PySpark) datatype for the partition column" in str(excinfo.value)

        @pytest.mark.parametrize("default_value", [None, "", [], {}])
        def test_default_value_is_empty(self, default_value, default_params, input_df):
            default_params["partition_definitions"][0]["default_value"] = default_value
            with pytest.raises(AssertionError) as excinfo:
                loader = HiveLoader(**default_params)
                loader.load(input_df)
            assert "No default partition value set for partition column" in str(excinfo.value)

        def test_default_value_is_missing(self, default_params, input_df):
            default_params["partition_definitions"][0].pop("default_value")
            with pytest.raises(AssertionError) as excinfo:
                loader = HiveLoader(**default_params)
                loader.load(input_df)
            assert "No default partition value set for partition column" in str(excinfo.value)

    @pytest.mark.parametrize("partition", [0, 2, 3, 6, 9])
    class TestLoadPartition(object):
        def test_add_new_static_partition(self, input_df, default_params, partition, full_table_name, spark_session):
            default_params["partition_definitions"][0]["default_value"] = partition
            loader = HiveLoader(**default_params)
            partition_query = construct_partition_query(default_params["partition_definitions"])
            inverted_partition_query = partition_query.replace("=", "!=").replace(", ", " and ")
            df_to_load = input_df.where(partition_query)

            count_pre_total = input_df.where(inverted_partition_query).count()
            count_to_load = df_to_load.count()
            count_post_total = input_df.count()
            assert (
                count_post_total == count_pre_total + count_to_load
            ), "Something went wrong in the test setup of the input dataframe (input_df)"

            spark_session.sql(
                "alter table {tbl} drop partition ({part_def})".format(tbl=full_table_name, part_def=partition_query)
            )
            assert (
                spark_session.table(full_table_name).count() == count_pre_total
            ), "test partition was not successfully dropped from output hive table"

            assert df_to_load.count() > 0, "Dataframe to load is empty!"
            loader.load(df_to_load)

            spark_session.catalog.refreshTable(full_table_name)

            assert (
                spark_session.table(full_table_name).count() == count_post_total
            ), "test partition was not successfully loaded to output hive table"

        def test_overwrite_static_partition(self, input_df, default_params, partition, full_table_name, spark_session):
            default_params["partition_definitions"][0]["default_value"] = partition
            loader = HiveLoader(**default_params)
            partition_query = construct_partition_query(default_params["partition_definitions"])
            df_to_load = input_df.where(partition_query)

            count_pre_total = spark_session.table(full_table_name).count()
            count_to_load = input_df.where(partition_query).count()
            count_post_total = input_df.count()
            assert (
                count_post_total == count_pre_total
            ), "Something went wrong in the test setup of the input DataFrame (input_df)"

            assert df_to_load.count() > 0, "DataFrame to load is empty!"
            loader.load(df_to_load)

            spark_session.catalog.refreshTable(full_table_name)

            assert (
                spark_session.table(full_table_name).count() == count_post_total
            ), "test partition was not successfully loaded to output hive table"

        def test_append_to_static_partition(self, input_df, default_params, partition, full_table_name, spark_session):
            default_params["partition_definitions"][0]["default_value"] = partition
            default_params["clear_partition"] = False
            loader = HiveLoader(**default_params)
            partition_query = construct_partition_query(default_params["partition_definitions"])
            df_to_load = input_df.where(partition_query)

            count_pre_total = spark_session.table(full_table_name).count()
            count_to_load = df_to_load.count()
            count_post_total = count_pre_total + count_to_load

            assert df_to_load.count() > 0, "DataFrame to load is empty!"
            loader.load(df_to_load)

            spark_session.catalog.refreshTable(full_table_name)

            assert (
                spark_session.table(full_table_name).count() == count_post_total
            ), "test partition was not successfully loaded to output hive table"

        def test_create_partitioned_table(self, input_df, default_params, partition, full_table_name, spark_session):
            default_params["partition_definitions"][0]["default_value"] = partition
            default_params["auto_create_table"] = True
            loader = HiveLoader(**default_params)
            spark_session.sql("drop table if exists " + full_table_name)

            spark_session.catalog.setCurrentDatabase(default_params["db_name"])
            assert default_params["table_name"] not in [
                tbl.name for tbl in spark_session.catalog.listTables()
            ], "Test setup of database is not clean. Table already exists!"

            partition_query = construct_partition_query(default_params["partition_definitions"])
            df_to_load = input_df.where(partition_query)

            count_to_load = df_to_load.count()

            assert df_to_load.count() > 0, "DataFrame to load is empty!"
            loader.load(df_to_load)

            spark_session.catalog.refreshTable(full_table_name)
            assert default_params["table_name"] in [
                tbl.name for tbl in spark_session.catalog.listTables()
            ], "Table was not created!"

            assert (
                spark_session.table(full_table_name).count() == count_to_load
            ), "test partition was not successfully loaded to automatically created output hive table"

            try:
                assert spark_session.sql("show partitions " + full_table_name).count() > 0
            except Py4JJavaError as e:
                raise AssertionError("Created table is not partitioned. " + str(e))

        def test_add_new_static_partition_with_overwritten_partition_value(
            self, input_df, default_params, partition, full_table_name, spark_session
        ):
            default_params["partition_definitions"][0]["default_value"] = partition
            default_params["clear_partition"] = False

            loader = HiveLoader(**default_params)
            partition_query = construct_partition_query(default_params["partition_definitions"])
            inverted_partition_query = partition_query.replace("=", "!=").replace(", ", " and ")
            output_table = spark_session.table(full_table_name)

            count_pre_partition = output_table.where(partition_query).count()
            count_post_partition = input_df.count()
            count_post_total = input_df.count() * 2

            assert input_df.count() > 0, "Dataframe to load is empty!"
            loader.load(input_df)

            assert (
                output_table.count() == count_post_total
            ), "test partition was not successfully loaded to output hive table"

            assert (
                output_table.where(partition_query).count() == input_df.count() + count_pre_partition
            ), "test partition was not successfully loaded to output hive table"


class TestMultiplePartitionColumn(object):
    @pytest.fixture()
    def input_df(self, spark_session, default_params, full_table_name):

        df = spark_session.read.parquet(f"{DATA_FOLDER}/schema_v1/parquetFiles")
        df = df.withColumn("partition_key_int", df.meta.version % 10)  # 0-9
        df = df.withColumn("partition_key_str", convert_int_to_ascii_char(df.partition_key_int + 100))  # d-m
        spark_session.conf.set("hive.exec.dynamic.partition", "true")
        spark_session.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        spark_session.sql("DROP DATABASE IF EXISTS {db} CASCADE".format(db=default_params["db_name"]))
        spark_session.sql("CREATE DATABASE {db}".format(db=default_params["db_name"]))
        df.write.partitionBy("partition_key_int", "partition_key_str").saveAsTable(full_table_name)
        yield df
        spark_session.sql("DROP DATABASE IF EXISTS {db} CASCADE".format(db=default_params["db_name"]))

    @pytest.fixture(scope="function")
    def default_params(self):
        return {
            "db_name": "test_hive_loader",
            "table_name": "test_partitioned",
            "repartition_size": 2,
            "clear_partition": True,
            "auto_create_table": True,
            "overwrite_partition_value": True,
            "partition_definitions": [
                {"column_name": "partition_key_int", "column_type": "IntegerType", "default_value": 7},
                {"column_name": "partition_key_str", "column_type": "StringType", "default_value": "k"},
            ],
        }

    class TestClearPartition(object):
        """Clearing the Hive Table Partition before inserting"""

        # input_df.groupBy("partition_key_int", "partition_key_str").count().orderBy("partition_key_int", "partition_key_str").show(200)

        @pytest.mark.parametrize("partition", [[0, "d"], [2, "f"], [3, "g"], [6, "j"], [9, "m"]])
        def test_clear_partition(self, spark_session, input_df, partition, default_params, full_table_name):
            """Partition is dropped"""
            (
                default_params["partition_definitions"][0]["default_value"],
                default_params["partition_definitions"][1]["default_value"],
            ) = partition
            loader = HiveLoader(**default_params)
            partition_query = construct_partition_query(loader.partition_definitions).replace(", ", " and ")
            inverted_partition_query = partition_query.replace("=", "!=")
            expected_count = input_df.where(inverted_partition_query).count()
            loader._clear_hive_partition()
            actual_count = spark_session.table(full_table_name).count()

            assert actual_count == expected_count

        def test_clear_partition_is_called_exactly_once(self, default_loader, input_df):
            """Clear Partition is called exactly once (Default)"""
            expect(default_loader)._clear_hive_partition.exactly(1).time
            default_loader.load(input_df)

        def test_clear_partition_is_not_called(self, default_loader, input_df):
            """Clear Partition is not called (Default Values was Overridden)"""
            default_loader.clear_partition = False
            expect(default_loader)._clear_hive_partition.exactly(0).time
            default_loader.load(input_df)

    class TestPartitionDefinitions(object):
        @pytest.mark.parametrize(
            "partition_definitions", ["Some string", 123, 75.0, b"abcd", ("Hello", "World"), {"Nice_to": "meet_you"}]
        )
        def test_input_is_not_a_list(self, partition_definitions, default_params):
            default_params["partition_definitions"] = partition_definitions
            with pytest.raises(AssertionError) as excinfo:
                HiveLoader(**default_params)
            assert "partition_definitions has to be a list containing dicts" in str(excinfo.value)

        @pytest.mark.parametrize("partition_definitions", ["Some string", 123, 75.0, b"abcd", ("Hello", "World")])
        def test_list_input_contains_non_dict_items(self, partition_definitions, default_params):
            default_params["partition_definitions"] = [partition_definitions]
            with pytest.raises(AssertionError) as excinfo:
                HiveLoader(**default_params)
            assert "Items of partition_definitions have to be dictionaries" in str(excinfo.value)

        def test_column_name_is_missing(self, default_params):
            (
                default_params["partition_definitions"][0]["column_name"],
                default_params["partition_definitions"][1]["column_name"],
            ) = (None, "f")
            with pytest.raises(AssertionError) as excinfo:
                HiveLoader(**default_params)
            assert "No column name set!" in str(excinfo.value)

        @pytest.mark.parametrize("data_type", [13, "no_spark_type", "arrray", "INT", ["IntegerType", "StringType"]])
        def test_column_type_not_a_valid_spark_sql_type(self, data_type, default_params):
            (
                default_params["partition_definitions"][0]["column_type"],
                default_params["partition_definitions"][0]["column_type"],
            ) = ("IntegerType", data_type)
            with pytest.raises(AssertionError) as excinfo:
                HiveLoader(**default_params)
            assert "Not a valid (PySpark) datatype for the partition column" in str(excinfo.value)

        @pytest.mark.parametrize("default_value", [None, "", [], {}])
        def test_default_value_is_empty(self, default_value, default_params, input_df):
            (
                default_params["partition_definitions"][0]["default_value"],
                default_params["partition_definitions"][0]["default_value"],
            ) = (3, default_value)
            with pytest.raises(AssertionError) as excinfo:
                loader = HiveLoader(**default_params)
                loader.load(input_df)
            assert "No default partition value set for partition column" in str(excinfo.value)

        def test_default_value_is_missing(self, default_params, input_df):
            default_params["partition_definitions"][1].pop("default_value")
            with pytest.raises(AssertionError) as excinfo:
                loader = HiveLoader(**default_params)
                loader.load(input_df)
            assert "No default partition value set for partition column" in str(excinfo.value)

    @pytest.mark.parametrize("partition", [[0, "d"], [2, "f"], [3, "g"], [6, "j"], [9, "m"]])
    class TestLoadPartition(object):
        def test_add_new_static_partition(self, input_df, default_params, partition, full_table_name, spark_session):
            (
                default_params["partition_definitions"][0]["default_value"],
                default_params["partition_definitions"][1]["default_value"],
            ) = partition
            loader = HiveLoader(**default_params)
            partition_clause = construct_partition_query(loader.partition_definitions)
            where_clause = partition_clause.replace(", ", " and ")
            where_clause_inverted = where_clause.replace("=", "!=")
            df_to_load = input_df.where(where_clause)

            count_pre_total = input_df.where(where_clause_inverted).count()
            count_to_load = df_to_load.count()
            count_post_total = input_df.count()
            assert (
                count_post_total == count_pre_total + count_to_load
            ), "Something went wrong in the test setup of the input dataframe (input_df)"

            spark_session.sql(
                "alter table {tbl} drop partition ({part_def})".format(tbl=full_table_name, part_def=partition_clause)
            )
            assert (
                spark_session.table(full_table_name).count() == count_pre_total
            ), "test partition was not successfully dropped from output hive table"

            assert df_to_load.count() > 0, "Dataframe to load is empty!"
            loader.load(df_to_load)

            spark_session.catalog.refreshTable(full_table_name)

            assert (
                spark_session.table(full_table_name).count() == count_post_total
            ), "test partition was not successfully loaded to output hive table"

        def test_overwrite_static_partition(self, input_df, default_params, partition, full_table_name, spark_session):
            (
                default_params["partition_definitions"][0]["default_value"],
                default_params["partition_definitions"][1]["default_value"],
            ) = partition
            loader = HiveLoader(**default_params)
            where_clause = construct_partition_query(loader.partition_definitions).replace(", ", " and ")
            df_to_load = input_df.where(where_clause)

            count_pre_total = spark_session.table(full_table_name).count()
            count_to_load = df_to_load.count()
            count_post_total = input_df.count()
            assert (
                count_post_total == count_pre_total
            ), "Something went wrong in the test setup of the input DataFrame (input_df)"

            assert df_to_load.count() > 0, "DataFrame to load is empty!"
            loader.load(df_to_load)

            spark_session.catalog.refreshTable(full_table_name)

            assert (
                spark_session.table(full_table_name).count() == count_post_total
            ), "test partition was not successfully loaded to output hive table"

        def test_append_to_static_partition(self, input_df, default_params, partition, full_table_name, spark_session):
            (
                default_params["partition_definitions"][0]["default_value"],
                default_params["partition_definitions"][1]["default_value"],
            ) = partition
            default_params["clear_partition"] = False
            loader = HiveLoader(**default_params)
            where_clause = construct_partition_query(loader.partition_definitions).replace(", ", " and ")
            #
            df_to_load = input_df.where(where_clause)

            count_pre_total = spark_session.table(full_table_name).count()
            count_to_load = df_to_load.count()
            count_post_total = count_pre_total + count_to_load

            assert df_to_load.count() > 0, "DataFrame to load is empty!"
            loader.load(df_to_load)

            spark_session.catalog.refreshTable(full_table_name)

            assert (
                spark_session.table(full_table_name).count() == count_post_total
            ), "test partition was not successfully loaded to output hive table"

        def test_create_partitioned_table(self, input_df, default_params, partition, full_table_name, spark_session):
            (
                default_params["partition_definitions"][0]["default_value"],
                default_params["partition_definitions"][1]["default_value"],
            ) = partition
            default_params["auto_create_table"] = True
            loader = HiveLoader(**default_params)
            spark_session.sql("drop table if exists " + full_table_name)

            spark_session.catalog.setCurrentDatabase(default_params["db_name"])
            assert default_params["table_name"] not in [
                tbl.name for tbl in spark_session.catalog.listTables()
            ], "Test setup of database is not clean. Table already exists!"

            where_clause = construct_partition_query(loader.partition_definitions).replace(", ", " and ")
            df_to_load = input_df.where(where_clause)

            count_to_load = df_to_load.count()

            assert df_to_load.count() > 0, "DataFrame to load is empty!"
            loader.load(df_to_load)

            spark_session.catalog.refreshTable(full_table_name)
            assert default_params["table_name"] in [
                tbl.name for tbl in spark_session.catalog.listTables()
            ], "Table was not created!"

            assert (
                spark_session.table(full_table_name).count() == count_to_load
            ), "test partition was not successfully loaded to automatically created output hive table"

            try:
                assert spark_session.sql("show partitions " + full_table_name).count() > 0
            except Py4JJavaError as e:
                raise AssertionError("Created table is not partitioned. " + str(e))

        def test_add_new_static_partition_with_overwritten_partition_value(
            self, input_df, default_params, partition, full_table_name, spark_session
        ):
            (
                default_params["partition_definitions"][0]["default_value"],
                default_params["partition_definitions"][1]["default_value"],
            ) = partition
            default_params["clear_partition"] = False
            loader = HiveLoader(**default_params)
            where_clause = construct_partition_query(loader.partition_definitions).replace(", ", " and ")
            output_table = spark_session.table(full_table_name)

            count_pre_partition = output_table.where(where_clause).count()
            count_post_partition = input_df.count()
            count_post_total = input_df.count() * 2

            assert input_df.count() > 0, "Dataframe to load is empty!"
            loader.load(input_df)

            assert (
                output_table.count() == count_post_total
            ), "test partition was not successfully loaded to output hive table"

            assert (
                output_table.where(where_clause).count() == input_df.count() + count_pre_partition
            ), "test partition was not successfully loaded to output hive table"
