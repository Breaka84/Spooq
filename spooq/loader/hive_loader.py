from __future__ import absolute_import
from past.builtins import basestring
from pyspark.sql import SparkSession
from pyspark.sql import types as sql_types
from pyspark.sql.functions import lit

from .loader import Loader


class HiveLoader(Loader):
    """
    Persists a PySpark DataFrame into a Hive Table.

    Examples
    --------
    >>> HiveLoader(
    >>>     db_name="users_and_friends",
    >>>     table_name="friends_partitioned",
    >>>     partition_definitions=[{
    >>>         "column_name": "dt",
    >>>         "column_type": "IntegerType",
    >>>         "default_value": 20200201}],
    >>>     clear_partition=True,
    >>>     repartition_size=10,
    >>>     overwrite_partition_value=False,
    >>>     auto_create_table=False,
    >>> ).load(input_df)

    >>> HiveLoader(
    >>>     db_name="users_and_friends",
    >>>     table_name="all_friends",
    >>>     partition_definitions=[],
    >>>     repartition_size=200,
    >>>     auto_create_table=True,
    >>> ).load(input_df)

    Parameters
    ----------
    db_name : :any:`str`
        The database name to load the data into.
    table_name : :any:`str`
        The table name to load the data into. The database name must not be included in this
        parameter as it is already defined in the `db_name` parameter.
    partition_definitions : :any:`list` of :py:class:`dict`
        (Defaults to `[{"column_name": "dt", "column_type": "IntegerType", "default_value": None}]`).

            * **column_name** (:any:`str`) - The Column's Name to partition by.
            * **column_type** (:any:`str`) - The PySpark SQL DataType for the Partition Value as
              a String. This should normally either be 'IntegerType()' or 'StringType()'
            * **default_value** (:any:`str` or :any:`int`) - If `column_name` does not contain
              a value or `overwrite_partition_value` is set, this value will be used for the
              partitioning

    clear_partition : :any:`bool`, (Defaults to True)
        This flag tells the Loader to delete the defined partitions before
        inserting the input DataFrame into the target table. Has no effect if no partitions are
        defined.
    repartition_size : :any:`int`, (Defaults to 40)
        The DataFrame will be repartitioned on Spark level before inserting into the table.
        This effects the number of output files on which the Hive table is based.
    auto_create_table : :any:`bool`, (Defaults to True)
        Whether the target table will be created if it does not yet exist.
    overwrite_partition_value : :any:`bool`, (Defaults to True)
        Defines whether the values of columns defined in `partition_definitions` should
        explicitly set by default_values.

    Raises
    ------
    :any:`exceptions.AssertionError`:
        partition_definitions has to be a list containing dicts. Expected dict content:
        'column_name', 'column_type', 'default_value' per partition_definitions item.

    :any:`exceptions.AssertionError`:
        Items of partition_definitions have to be dictionaries.

    :any:`exceptions.AssertionError`:
        No column name set!

    :any:`exceptions.AssertionError`:
        Not a valid (PySpark) datatype for the partition column {name} | {type}.

    :any:`exceptions.AssertionError`:
        `clear_partition` is only supported if `overwrite_partition_value` is also enabled.
        This would otherwise result in clearing partitions on basis of dynamically values
        (from DataFrame) instead of explicitly defining the partition(s) to clear.
    """

    def __init__(
        self,
        db_name,
        table_name,
        partition_definitions=[{"column_name": "dt", "column_type": "IntegerType", "default_value": None}],
        clear_partition=True,
        repartition_size=40,
        auto_create_table=True,
        overwrite_partition_value=True,
    ):
        super(HiveLoader, self).__init__()
        self._assert_partition_definitions_is_valid(partition_definitions)
        self.partition_definitions = partition_definitions
        self.db_name = db_name
        self.table_name = table_name
        self.full_table_name = db_name + "." + table_name
        self.repartition_size = repartition_size
        if clear_partition and not overwrite_partition_value:
            raise ValueError(
                "clear_partition is only supported if overwrite_partition_value is also enabled. ",
                "This would otherwise result in clearing partitions on basis of dynamically values",
                "(from dataframe) instead of explicitly defining the partition(s) to clear",
            )
        self.clear_partition = clear_partition
        self.overwrite_partition_value = overwrite_partition_value
        self.auto_create_table = auto_create_table
        self.spark = (
            SparkSession.Builder()
            .enableHiveSupport()
            .appName("spooq.extractor: {nm}".format(nm=self.name))
            .getOrCreate()
        )

    def _assert_partition_definitions_is_valid(self, definitions):
        assert isinstance(definitions, list), (
            "partition_definitions has to be a list containing dicts.\n",
            "Expected dict content: 'column_name', 'column_type', 'default_value' per partition_definitions item.",
        )

        for dct in definitions:
            assert isinstance(dct, dict), "Items of partition_definitions have to be dictionaries"
            assert dct["column_name"], "No column name set!"
            assert isinstance(dct["column_type"], basestring) and hasattr(
                sql_types, dct["column_type"]
            ), "Not a valid (PySpark) datatype for the partition column {name} | {type}".format(
                name=dct["column_name"], type=dct["column_type"]
            )
            dct["column_type"] = getattr(sql_types, dct["column_type"])

    def load(self, input_df):
        self.spark.conf.set("hive.exec.dynamic.partition", "true")
        self.spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        input_df = input_df.repartition(self.repartition_size)
        input_df = self._add_partition_definition_to_dataframe(input_df)

        if self._table_exists():
            output_df = self.spark.table(self.full_table_name)
            assert input_df.columns == output_df.columns, "Input columns don't match the columns of the Hive table"
            if self.clear_partition:
                self._clear_hive_partition()
            input_df.write.insertInto(self.full_table_name)
        elif self.auto_create_table:
            input_df.write.partitionBy(*[dct["column_name"] for dct in self.partition_definitions]).saveAsTable(
                self.full_table_name
            )

        else:
            raise Exception(
                "Table: {tbl} does not exist and `auto_create_table` is set to False".format(tbl=self.full_table_name)
            )

    def _add_partition_definition_to_dataframe(self, input_df):
        for partition_definition in self.partition_definitions:
            if partition_definition["column_name"] not in input_df.columns or self.overwrite_partition_value:
                assert "default_value" in list(partition_definition.keys()) and (
                    partition_definition["default_value"] or partition_definition["default_value"] == 0
                ), "No default partition value set for partition column: {name}!\n".format(
                    name=partition_definition["column_name"]
                )
                input_df = input_df.withColumn(
                    partition_definition["column_name"],
                    lit(partition_definition["default_value"]).cast(partition_definition["column_type"]()),
                )
        return input_df

    def _table_exists(self):
        table_exists = False
        self.spark.catalog.setCurrentDatabase(self.db_name)
        for tbl in self.spark.catalog.listTables():
            if self.table_name == tbl.name:
                table_exists = True
        return table_exists

    def _clear_hive_partition(self):
        def _construct_partition_query_string(partition_definitions):
            partition_queries = []
            for dct in partition_definitions:
                assert "default_value" in list(
                    dct.keys()
                ), "clear_partitions needs a default_value per partition definition!"
                if issubclass(dct["column_type"], sql_types.NumericType):
                    partition_queries.append("{part} = {dt}".format(part=dct["column_name"], dt=dct["default_value"]))
                else:
                    partition_queries.append("{part} = '{dt}'".format(part=dct["column_name"], dt=dct["default_value"]))
            return ", ".join(partition_queries)

        partition_query = _construct_partition_query_string(self.partition_definitions)
        command = """ALTER TABLE {tbl} DROP IF EXISTS PARTITION ({part_def})""".format(
            tbl=self.full_table_name, part_def=partition_query
        )

        self.logger.debug("Command used to clear Partition: {cmd}".format(cmd=command))
        self.spark.sql(command)
