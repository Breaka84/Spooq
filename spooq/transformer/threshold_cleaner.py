from __future__ import absolute_import
import sys

if sys.version_info.major > 2:
    # This is needed for python 2 as otherwise pyspark raises an exception for following command:
    # data_type = input_df.schema[str(column_name)].dataType
    # Pyspark checks if the input is a string, which does not work
    # with the new strings from builtins
    from builtins import str
import pyspark.sql.functions as F
import pyspark.sql.types as sql_types
from pyspark.sql.column import Column

from .base_cleaner import BaseCleaner


class ThresholdCleaner(BaseCleaner):
    """
    Cleanes values based on defined boundaries and optionally logs the original values.
    The valid value ranges are provided via dictionary for each column to be cleaned. Boundaries can either be of
    static or dynamic nature that resolve to numbers, timetamps or dates.

    Examples
    --------
    >>> from pyspark.sql import functions as F
    >>> from spooq.transformer import ThresholdCleaner
    >>> transformer = ThresholdCleaner(
    >>>     thresholds={
    >>>         "created_at": {
    >>>             "min": F.date_sub(F.current_date(), 365 * 10),  # Ten years ago
    >>>             "max": F.current_date()
    >>>         },
    >>>         "size_cm": {
    >>>             "min": 70,
    >>>             "max": 250,
    >>>             "default": None
    >>>         },
    >>>     }
    >>> )

    >>> from spooq.transformer import ThresholdCleaner
    >>> from pyspark.sql import Row
    >>>
    >>> input_df = spark.createDataFrame([
    >>>     Row(id=0, integers=-5, doubles=-0.75),
    >>>     Row(id=1, integers= 5, doubles= 1.25),
    >>>     Row(id=2, integers=15, doubles= 0.67),
    >>> ])
    >>> transformer = ThresholdCleaner(
    >>>     thresholds={
    >>>         "integers": {"min":  0, "max": 10},
    >>>         "doubles":  {"min": -1, "max":  1}
    >>>     },
    >>>     column_to_log_cleansed_values="cleansed_values_threshold",
    >>>     store_as_map=True,
    >>> )
    >>> output_df = transformer.transform(input_df)
    >>> output_df.show(truncate=False)
    +---+--------+-----------+---------------------+
    |id |integers|doubles|cleansed_values_threshold|
    +---+--------+-------+-------------------------+
    |0  |null    |-0.75  |{integers -> -5}         |
    |1  |5       |null   |{doubles -> 1.25}        |
    |2  |null    |0.67   |{integers -> 15}         |
    +---+--------+-------+-------------------------+
    >>> output_df.printSchema()
     |-- id: long (nullable = true)
     |-- integers: long (nullable = true)
     |-- doubles: double (nullable = true)
     |-- cleansed_values_threshold: map (nullable = false)
     |    |-- key: string
     |    |-- value: string (valueContainsNull = true)

    Parameters
    ----------
    thresholds : :py:class:`dict`
        Dictionary containing column names and respective valid ranges

    column_to_log_cleansed_values : :any:`str`, Defaults to None
        Defines a column in which the original (uncleansed) value will be stored in case of cleansing. If no column
        name is given, nothing will be logged.

    store_as_map : :any:`bool`, Defaults to False
        Specifies if the logged cleansed values should be stored in a column as :class:`~pyspark.sql.types.MapType` with
        stringified values or as :class:`~pyspark.sql.types.StructType` with the original respective data types.

    Note
    ----
    Following cleansing rule attributes per column are supported:

        * min, mandatory: :any:`str`, :any:`int`, |SPARK_COLUMN|, |SPARK_FUNCTION|
            A number or timestamp/date which serves as the lower limit for allowed values. Values
            below this threshold will be cleansed. Supports literals, Spark functions and Columns.
        * max, mandatory: :any:`str`, :any:`int`, |SPARK_COLUMN|, |SPARK_FUNCTION|
            A number or timestamp/date which serves as the upper limit for allowed values. Values
            above this threshold will be cleansed. Supports literals, Spark functions and Columns.
        * default, defaults to None: :any:`str`, :any:`int`, |SPARK_COLUMN|, |SPARK_FUNCTION|
            If a value gets cleansed it gets replaced with the provided default value. Supports literals,
            Spark functions and Columns.

    The :py:meth:`~pyspark.sql.Column.between` method is used internally.

    Returns
    -------
    |SPARK_DATAFRAME|
        The transformed DataFrame

    Raises
    ------
    :any:`exceptions.ValueError`
        Threshold-based cleaning only supports Numeric, Date and Timestamp Types!
        Column with name: {col_name} and type of: {col_type} was provided

    Warning
    -------
    Only Numeric, TimestampType, and DateType data types are supported!
    """

    def __init__(self, thresholds={}, column_to_log_cleansed_values=None, store_as_map=False):
        super().__init__(
            cleaning_definitions=thresholds,
            column_to_log_cleansed_values=column_to_log_cleansed_values,
            store_as_map=store_as_map,
            temporary_columns_prefix="dac28b56d8055953a7038bfe3b5097e7",
        )
        self.logger.debug("Range Definitions: " + str(self.cleaning_definitions))

    def transform(self, input_df):
        self.logger.debug("input_df Schema: " + input_df._jdf.schema().treeString())
        ordered_column_names = input_df.columns

        if self.column_to_log_cleansed_values:
            column_names_to_clean = self.cleaning_definitions.keys()
            temporary_column_names = self._get_temporary_column_names(column_names_to_clean)
            input_df = self._add_temporary_columns(input_df, column_names_to_clean, temporary_column_names)

        cleansing_expressions = []
        for column_name, value_range in list(self.cleaning_definitions.items()):

            data_type = input_df.schema[str(column_name)].dataType
            substitute = value_range.get("default", None)
            if not isinstance(substitute, Column):
                substitute = F.lit(substitute)

            if not isinstance(data_type, (sql_types.NumericType, sql_types.DateType, sql_types.TimestampType)):
                raise ValueError(
                    "Threshold-based cleaning only supports Numeric, Date and Timestamp Types!\n",
                    "Column with name: {col_name} and type of: {col_type} was provided".format(
                        col_name=column_name, col_type=data_type
                    ),
                )

            self.logger.debug("Ranges for column " + column_name + ": " + str(value_range))

            cleansing_expression = (
                F.when(
                    input_df[column_name].between(value_range["min"], value_range["max"]),
                    input_df[column_name],
                )
                .otherwise(substitute)
                .try_cast(data_type)
            )
            self.logger.debug("Cleansing Expression for " + column_name + ": " + str(cleansing_expression))
            cleansing_expressions.append((column_name, cleansing_expression))

        self.logger.info("Full threshold cleansing expression:")
        self.logger.info(
            ".".join(
                [
                    f"withColumn({column_name}, {str(cleansing_expr)})"
                    for (column_name, cleansing_expr) in cleansing_expressions
                ]
            )
        )
        for (column_name, cleansing_expr) in cleansing_expressions:
            input_df = input_df.withColumn(column_name, cleansing_expr)

        if self.column_to_log_cleansed_values:
            input_df = self._log_cleansed_values(input_df, column_names_to_clean, temporary_column_names)
            ordered_column_names.append(self.column_to_log_cleansed_values)

        return input_df.select(ordered_column_names)
