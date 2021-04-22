from __future__ import absolute_import
from builtins import str
from past.builtins import basestring
import pandas as pd
from copy import copy
from pyspark.sql import SparkSession
from pyspark.sql.functions import min as sql_min
from pyspark.sql.functions import max as sql_max
from pyspark.sql.types import StructField, StructType
from pyspark.sql.types import IntegerType, StringType


from .extractor import Extractor


class JDBCExtractor(Extractor):
    def __init__(self, jdbc_options, cache=True):
        super(JDBCExtractor, self).__init__()
        self._assert_jdbc_options(jdbc_options)
        self.jdbc_options = jdbc_options
        self.cache = cache
        self.spark = (
            SparkSession.Builder()
            .enableHiveSupport()
            .appName("spooq.extractor: {nm}".format(nm=self.name))
            .getOrCreate()
        )

    def _load_from_jdbc(self, query, jdbc_options, cache=True):
        jdbc_options = copy(self.jdbc_options)
        jdbc_options["dbtable"] = "({q}) as table_statement".format(q=query)
        source_df = self.spark.read.format("jdbc").options(**jdbc_options).load()
        if cache:
            source_df.cache()

        return source_df

    def _assert_jdbc_options(self, jdbc_options):
        for key in ["url", "driver", "user", "password"]:
            assert key in jdbc_options, key + " is missing from the jdbc_options."
            assert isinstance(jdbc_options[key], basestring), key + " has to be provided as a string object."


class JDBCExtractorFullLoad(JDBCExtractor):
    """
    Connects to a JDBC Source and fetches the data defined by the provided Query.

    Examples
    --------
    >>> import spooq.extractor as E
    >>>
    >>> extractor = E.JDBCExtractorFullLoad(
    >>>     query="select id, first_name, last_name, gender, created_at test_db.from users",
    >>>     jdbc_options={
    >>>         "url": "jdbc:postgresql://localhost/test_db",
    >>>         "driver": "org.postgresql.Driver",
    >>>         "user": "read_only",
    >>>         "password": "test123",
    >>>     },
    >>> )
    >>>
    >>> extracted_df = extractor.extract()
    >>> type(extracted_df)
    pyspark.sql.dataframe.DataFrame

    Parameters
    ----------
    query : :any:`str`
        Defines the actual query sent to the JDBC Source. This has to be a valid SQL query
        with respect to the source system (e.g., T-SQL for Microsoft SQL Server).

    jdbc_options : :class:`dict`, optional
        A set of parameters to configure the connection to the source:
            * **url** (:any:`str`) - A JDBC URL of the form jdbc:subprotocol:subname.
              e.g., jdbc:postgresql://localhost:5432/dbname
            * **driver** (:any:`str`) - The class name of the JDBC driver to use to connect to this URL.
            * **user** (:any:`str`) - Username to authenticate with the source database.
            * **password** (:any:`str`) - Password to authenticate with the source database.

        See :meth:`pyspark.sql.DataFrameReader.jdbc` and
        https://spark.apache.org/docs/2.4.3/sql-data-sources-jdbc.html for more information.

    cache : :any:`bool`, defaults to :any:`True`
        Defines, weather to :meth:`~pyspark.sql.DataFrame.cache` the dataframe, after it is loaded.
        Otherwise the Extractor will reload all data from the source system eachtime an action is
        performed on the DataFrame.

    Raises
    ------
    :any:`exceptions.AssertionError`:
        All jdbc_options values need to be present as string variables.
    """

    def __init__(self, query, jdbc_options, cache=True):
        super(JDBCExtractorFullLoad, self).__init__(jdbc_options=jdbc_options, cache=cache)
        self.query = query

    def extract(self):
        """
        This is the Public API Method to be called for all classes of Extractors

        Parameters
        ----------

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            PySpark dataframe from the input JDBC connection.

        """
        return self._load_from_jdbc(self.query, jdbc_options=self.jdbc_options, cache=self.cache)


class JDBCExtractorIncremental(JDBCExtractor):
    """
    Connects to a JDBC Source and fetches the data with respect to boundaries.
    The boundaries are inferred from the partition to load and logs from previous loads
    stored in the ``spooq_values_table``.

    Examples
    --------
    >>> import spooq.extractor as E
    >>>
    >>> # Boundaries derived from previously logged extractions => ("2020-01-31 03:29:59", False)
    >>>
    >>> extractor = E.JDBCExtractorIncremental(
    >>>     partition="20200201",
    >>>     jdbc_options={
    >>>         "url": "jdbc:postgresql://localhost/test_db",
    >>>         "driver": "org.postgresql.Driver",
    >>>         "user": "read_only",
    >>>         "password": "test123",
    >>>     },
    >>>     source_table="users",
    >>>     spooq_values_table="spooq_jdbc_log_users",
    >>> )
    >>>
    >>> extractor._construct_query_for_partition(extractor.partition)
    select * from users where updated_at > "2020-01-31 03:29:59"
    >>>
    >>> extracted_df = extractor.extract()
    >>> type(extracted_df)
    pyspark.sql.dataframe.DataFrame

    Parameters
    ----------
    partition : :any:`int` or :any:`str`
        Partition to extract. Needed for logging the incremental load in
        the ``spooq_values_table``.

    jdbc_options : :class:`dict`, optional
        A set of parameters to configure the connection to the source:
            * **url** (:any:`str`) - A JDBC URL of the form jdbc:subprotocol:subname.
              e.g., jdbc:postgresql://localhost:5432/dbname
            * **driver** (:any:`str`) - The class name of the JDBC driver to use to connect to this URL.
            * **user** (:any:`str`) - Username to authenticate with the source database.
            * **password** (:any:`str`) - Password to authenticate with the source database.

        See :meth:`pyspark.sql.DataFrameReader.jdbc` and
        https://spark.apache.org/docs/2.4.3/sql-data-sources-jdbc.html for more information.

    source_table : :any:`str`
        Defines the tablename of the source to be loaded from. For example 'purchases'.
        This is necessary to build the query.

    spooq_values_table : :any:`str`
        Defines the Hive table where previous and future loads of a specific source table
        are logged. This is necessary to derive boundaries for the current partition.

    spooq_values_db : :any:`str`, optional
        Defines the Database where the ``spooq_values_table`` is stored.
        Defaults to `'spooq_values'`.

    spooq_values_partition_column : :any:`str`, optional
        The column name which is used for the boundaries.
        Defaults to `'updated_at'`.

    cache : :any:`bool`, defaults to :any:`True`
        Defines, weather to :meth:`~pyspark.sql.DataFrame.cache` the dataframe, after it is loaded. Otherwise the Extractor
        will reload all data from the source system again, if a second action upon the dataframe
        is performed.

    Raises
    ------
    :any:`exceptions.AssertionError`:
        All jdbc_options values need to be present as string variables.
    """

    def __init__(
        self,
        partition,
        jdbc_options,
        source_table,
        spooq_values_table,
        spooq_values_db="spooq_values",
        spooq_values_partition_column="updated_at",
        cache=True,
    ):
        super(JDBCExtractorIncremental, self).__init__(jdbc_options)
        self.partition = partition
        self.source_table = source_table
        self.spooq_values_table = spooq_values_table
        self.spooq_values_db = spooq_values_db
        self.spooq_values_partition_column = spooq_values_partition_column
        self.cache = cache

    def extract(self):
        query = self._construct_query_for_partition(partition=self.partition)
        loaded_df = self._load_from_jdbc(query, self.jdbc_options, cache=self.cache)
        self._update_boundaries_for_current_partition_on_table(
            loaded_df,
            self.spooq_values_db,
            self.spooq_values_table,
            self.partition,
            self.spooq_values_partition_column,
        )
        return loaded_df

    def _construct_query_for_partition(self, partition):
        """Constructs and returns a predicated Query :any:`str` depending on the `partition`

        Based on the partition and previous loading logs (`spooq_values_table`),
        boundaries will be calculated and injected in the where clause of the query.

        Parameters
        ----------
        partition : Integer or :any:`str`

        Returns
        -------
        :any:`str`
            Complete Query :any:`str` to be used for JDBC Connections

        """

        select_statement = "select *"
        where_clause = ""
        lower_bound, upper_bound = self._get_boundaries_for_import(partition)

        def _fix_boundary_value_syntax(boundary):
            """If a boundary value is not a number, it has to be quoted for correct syntax."""

            try:
                boundary = int(boundary)
            except ValueError:
                boundary = '"{bnd}"'.format(bnd=boundary)
            return boundary

        if lower_bound and upper_bound:
            where_clause = "where {chk_col} > {low_bnd} and {chk_col} <= {up_bnd}".format(
                chk_col=self.spooq_values_partition_column,
                low_bnd=_fix_boundary_value_syntax(lower_bound),
                up_bnd=_fix_boundary_value_syntax(upper_bound),
            )
        elif lower_bound:
            where_clause = "where {chk_col} > {low_bnd}".format(
                chk_col=self.spooq_values_partition_column,
                low_bnd=_fix_boundary_value_syntax(lower_bound),
            )
        elif upper_bound:
            where_clause = "where {chk_col} <= {up_bnd}".format(
                chk_col=self.spooq_values_partition_column,
                up_bnd=_fix_boundary_value_syntax(upper_bound),
            )

        query = "{select} from {tbl} {where}".format(select=select_statement, tbl=self.source_table, where=where_clause)
        return " ".join(query.split())

    def _get_boundaries_for_import(self, partition):
        """
        Returns the lower and upper boundaries to be used in the where clause of the query.
        This information is deducted from the ``partition`` parameter and previous loading logs
        (persisted in `spooq_values_table`).

        Parameters
        ----------
        partition : :py:class:`int` or :any:`str`

        Returns
        -------
        Tuple of :any:`str`
            Values of the tuple can also be `False`
        """
        pd_df = self._get_previous_boundaries_table_as_pd(self.spooq_values_db, self.spooq_values_table)
        partition = int(partition)
        table_is_empty = pd_df.empty
        partition_exists = not pd_df.loc[pd_df["dt"] == partition].empty
        succeeding_partition_exists = not pd_df.loc[pd_df["dt"] > partition].empty
        preceding_partition_exists = not pd_df.loc[pd_df["dt"] < partition].empty

        if table_is_empty:
            """First import ever, starting from zero"""
            return False, False

        elif partition_exists:
            """Partition to insert already exists (reload / backfill)"""
            return self._get_lower_and_upper_bounds_from_current_partition(pd_df, partition)

        else:
            """Partition to insert does not yet exist (new day to insert)"""
            if preceding_partition_exists and not succeeding_partition_exists:
                """No equal or newer partitions exist (newest partition will be imported). Default Case"""
                return (
                    self._get_upper_bound_from_preceding_partition(pd_df, partition),
                    False,
                )

            elif not preceding_partition_exists and succeeding_partition_exists:
                """No older or equal partitions exist"""
                return (
                    False,
                    self._get_lower_bound_from_succeeding_partition(pd_df, partition),
                )

            elif preceding_partition_exists and succeeding_partition_exists:
                """At least one older, no equal and at least one newer partitions exist"""
                return (
                    self._get_upper_bound_from_preceding_partition(pd_df, partition),
                    self._get_lower_bound_from_succeeding_partition(pd_df, partition),
                )

            else:
                raise Exception(
                    """
                    ERROR: Something weird happened...
                    There was a logical problem getting the correct boundaries
                    from the spooqvalue table!
                    Please debug me ;-)
                    """
                )

    def _get_previous_boundaries_table_as_pd(self, spooq_values_db, spooq_values_table):
        """
        Converts the previous_boundaries_table and returns a Pandas Dataframe.

        Parameters
        ----------
        spooq_values_db : :any:`str`
        spooq_values_table : :any:`str`

        Returns
        -------
        Pandas Dataframe
            Content of `spooq_values_table` from `spooq_values_db`
        """
        return self._get_previous_boundaries_table(spooq_values_db, spooq_values_table).toPandas()

    def _get_previous_boundaries_table(self, spooq_values_db, spooq_values_table):
        """
        Fetches and returns a DataFrame containing the logs of previous loading
        jobs (`spooq_values_table`) of this entity

        Parameters
        ----------
        spooq_values_db : :any:`str`
        spooq_values_table : :any:`str`

        Returns
        -------
        PySpark DataFrame
            Content of `spooq_values_table` from `spooq_values_db`

        """
        table_name = "{db}.{tbl}".format(db=spooq_values_db, tbl=spooq_values_table)
        self.logger.info("Loading spooqValues Table from {name}".format(name=table_name))
        df = self.spark.table(table_name)
        try:
            self.spooq_values_partition_column
        except AttributeError:
            self.spooq_values_partition_column = df.select("partition_column").distinct().collect()[0].partition_column
        return df

    @staticmethod
    def _get_lower_bound_from_succeeding_partition(pd_df, partition):
        succeeding_pd_df = pd_df.loc[pd_df["dt"] > partition]
        return succeeding_pd_df.sort_values("dt", ascending=1).iloc[0].first_value

    @staticmethod
    def _get_upper_bound_from_preceding_partition(pd_df, partition):
        preceding_pd_df = pd_df.loc[pd_df["dt"] < partition]
        return preceding_pd_df.sort_values("dt", ascending=0).iloc[0].last_value

    @staticmethod
    def _get_lower_and_upper_bounds_from_current_partition(pd_df, partition):
        current_pd_df = pd_df.loc[pd_df["dt"] == partition].iloc[0]
        return (current_pd_df.first_value, current_pd_df.last_value)

    def _get_lowest_boundary_from_df(self, df, spooq_values_partition_column):
        return eval(
            "df.na.drop(how='any', subset=['{chk_col}']).select(sql_min(df.{chk_col}).alias('minimum')).collect()[0].minimum".format(
                chk_col=spooq_values_partition_column
            )
        )

    def _get_highest_boundary_from_df(self, df, spooq_values_partition_column):
        return eval(
            "df.na.drop(how='any', subset=['{chk_col}']).select(sql_max(df.{chk_col}).alias('maximum')).collect()[0].maximum".format(
                chk_col=spooq_values_partition_column
            )
        )

    def _update_boundaries_for_current_partition_on_table(
        self, df, spooq_values_db, spooq_values_table, partition, spooq_values_partition_column
    ):

        lowest_boundary = self._get_lowest_boundary_from_df(df, spooq_values_partition_column)
        highest_boundary = self._get_highest_boundary_from_df(df, spooq_values_partition_column)
        self._write_boundaries_to_hive(
            lowest_boundary,
            highest_boundary,
            spooq_values_db,
            spooq_values_table,
            partition,
            spooq_values_partition_column,
        )

    def _write_boundaries_to_hive(
        self,
        lowest_boundary,
        highest_boundary,
        spooq_values_db,
        spooq_values_table,
        partition,
        spooq_values_partition_column,
    ):

        self.spark.conf.set("hive.exec.dynamic.partition", "true")
        self.spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

        schema = StructType(
            [
                StructField("partition_column", StringType(), True),
                StructField("dt", IntegerType(), False),
                StructField("first_value", StringType(), False),
                StructField("last_value", StringType(), False),
            ]
        )

        input_data = [
            [
                str(spooq_values_partition_column),
                int(partition),
                str(lowest_boundary),
                str(highest_boundary),
            ]
        ]

        df_output = self.spark.createDataFrame(input_data, schema=schema)

        df_output.repartition(1).write.mode("overwrite").insertInto(
            "{db}.{tbl}".format(db=spooq_values_db, tbl=spooq_values_table, dt=partition)
        )
