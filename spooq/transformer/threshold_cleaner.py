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
    Sets outiers within a DataFrame to a default value.
    Takes a dictionary with valid value ranges for each column to be cleaned.

    Examples
    --------
    >>> from spooq.transformer import ThresholdCleaner
    >>> transformer = ThresholdCleaner(
    >>>     thresholds={
    >>>         "created_at": {
    >>>             "min": 0,
    >>>             "max": 1580737513,
    >>>             "default": pyspark.sql.functions.current_date()
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
    >>>     Row(id=0, integers=-5, some_factor=-0.75),
    >>>     Row(id=1, integers= 5, some_factor= 1.25),
    >>>     Row(id=2, integers=15, some_factor= 0.67),
    >>> ])
    >>> transformer = ThresholdCleaner(
    >>>     thresholds={
    >>>         "integers":    {"min":  0, "max": 10},
    >>>         "some_factor": {"min": -1, "max":  1}
    >>>     },
    >>>     column_to_log_cleansed_values="cleansed_values_threshold"
    >>> )
    >>> output_df = transformer.transform(input_df)
    >>> output_df.show()
    +---+--------+-----------+-------------------------+
    | id|integers|some_factor|cleansed_values_threshold|
    +---+--------+-----------+-------------------------+
    |  0|    null|      -0.75|                    [-5,]|
    |  1|       5|       null|                 [, 1.25]|
    |  2|    null|       0.67|                    [15,]|
    +---+--------+-----------+-------------------------+
    >>> output_df.printSchema()
    root
     |-- id: long (nullable = true)
     |-- integers: long (nullable = true)
     |-- some_factor: double (nullable = true)
     |-- cleansed_values_threshold: struct (nullable = false)
     |    |-- integers: long (nullable = true)
     |    |-- some_factor: double (nullable = true)


    Parameters
    ----------
    thresholds : :py:class:`dict`
        Dictionary containing column names and respective valid ranges

    column_to_log_cleansed_values : :any:`str`, Defaults to None
        Defines a column in which the original (uncleansed) value will be stored in case of cleansing. If no column
        name is given, nothing will be logged.

    Note
    ----
    Following cleansing rule attributes per column are supported:

        * min, mandatory - any
            A number or timestamp/date which serves as the lower limit for allowed values. Values
            below this threshold will be cleansed.
        * max, mandatory - any
            A number or timestamp/date which serves as the upper limit for allowed values. Values
            above this threshold will be cleansed.
        * default, defaults to None - :class:`~pyspark.sql.column.Column` or any primitive Python value
            If a value gets cleansed it gets replaced with the provided default value.

    The :meth:`pyspark.sql.functions.between` method is used internally.

    Returns
    -------
    :any:`pyspark.sql.DataFrame`
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

    def __init__(self, thresholds={}, column_to_log_cleansed_values=None):
        super().__init__(cleaning_definitions=thresholds, column_to_log_cleansed_values=column_to_log_cleansed_values)
        self.logger.debug("Range Definitions: " + str(self.cleaning_definitions))
        self.TEMPORARY_COLUMNS_PREFIX = "dac28b56d8055953a7038bfe3b5097e7"  # SHA1 hash of "ThresholdCleaner"

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
                .cast(data_type)
            )
            self.logger.debug("Cleansing Expression for " + column_name + ": " + str(cleansing_expression))
            cleansing_expressions.append((column_name, cleansing_expression))

        self.logger.info("Full treshold cleansing expression:")
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
