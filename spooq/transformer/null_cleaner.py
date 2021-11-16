import pyspark.sql.functions as F
from pyspark.sql.column import Column

from .base_cleaner import BaseCleaner


class NullCleaner(BaseCleaner):
    """
    Fills Null values of the specifield fields.
    Takes a dictionary with the fields to be cleaned and the default value to be set when the field is null.

    Examples
    --------
    >>> from pyspark.sql import functions as F
    >>> from spooq.transformer import NullCleaner
    >>> transformer = NullCleaner(
    >>>     cleaning_definitions={
    >>>         "points": {
    >>>             "default": 0
    >>>         }
    >>>     }
    >>> )

    >>> from spooq.transformer import NullCleaner
    >>> from pyspark.sql import Row
    >>>
    >>> input_df = spark.createDataFrame([
    >>>     Row(id=0, points=5),
    >>>     Row(id=1, points= None),
    >>>     Row(id=2, points=15),
    >>> ])
    >>> transformer = NullCleaner(
    >>>     cleaning_definitions={
    >>>         "points":    {"default":  0},
    >>>     },
    >>>     column_to_log_cleansed_values="cleansed_values_null",
    >>>     store_as_map=True,
    >>> )
    >>> output_df = transformer.transform(input_df)
    >>> output_df.show()
    +---+------+--------------------+
    | id|points|cleansed_values_null|
    +---+------+--------------------+
    |  0|     5|                null|
    |  1|     0|    [points -> null]|
    |  2|    15|                null|
    +---+------+--------------------+
    >>> output_df.printSchema()
     |-- id: long (nullable = true)
     |-- points: long (nullable = true)
     |-- cleansed_values_null: map (nullable = true)
     |    |-- key: string
     |    |-- value: string (valueContainsNull = true)

    Parameters
    ----------
    cleaning_definitions : :py:class:`dict`
        Dictionary containing column names and respective default values

    column_to_log_cleansed_values : :any:`str`, Defaults to None
        Defines a column in which the original (uncleansed) value will be stored in case of cleansing. If no column
        name is given, nothing will be logged.

    store_as_map : :any:`bool`, Defaults to False
        Specifies if the logged cleansed values should be stored in a column as :any:`pyspark.sql.types.MapType` or as
        :any:`pyspark.sql.types.StructType` with stringified values.

    Note
    ----
    The following cleaning_definitions attributes per column are mandatory:

        * default - :class:`~pyspark.sql.column.Column` or any primitive Python value
            If a value gets cleansed it gets replaced with the provided default value.

    Returns
    -------
    :any:`pyspark.sql.DataFrame`
        The transformed DataFrame

    Raises
    ------
    :any:`exceptions.ValueError`
        Null-based cleaning requires the field default.
        Default parameter is not specified for column with name: {column_name}
    """

    def __init__(self, cleaning_definitions=None, column_to_log_cleansed_values=None, store_as_map=False):
        cleaning_definitions = cleaning_definitions if cleaning_definitions is not None else {}
        super().__init__(
            cleaning_definitions=cleaning_definitions,
            column_to_log_cleansed_values=column_to_log_cleansed_values,
            store_as_map=store_as_map,
            temporary_columns_prefix="ead8cn9f7tf0sf1cs1ua61464zti82kj",
        )
        self.logger.debug("Cleansing Definitions: " + str(self.cleaning_definitions))

    def transform(self, input_df):
        self.logger.debug("input_df Schema: " + input_df._jdf.schema().treeString())
        ordered_column_names = input_df.columns

        if self.column_to_log_cleansed_values:
            column_names_to_clean = self.cleaning_definitions.keys()
            temporary_column_names = self._get_temporary_column_names(column_names_to_clean)
            input_df = self._add_temporary_columns(input_df, column_names_to_clean, temporary_column_names)

        cleansing_expressions = []
        for column_name, cleansing_value in list(self.cleaning_definitions.items()):

            data_type = input_df.schema[str(column_name)].dataType
            substitute = cleansing_value.get("default")

            if substitute is None:
                raise ValueError("Null-based cleaning requires the field default.", f"Default parameter is not specified for column with name: {column_name}")

            if not isinstance(substitute, Column):
                substitute = F.lit(substitute)

            self.logger.debug("Cleansing value for column " + column_name + ": " + str(cleansing_value))

            cleansing_expression = (
                F.when(
                    F.col(column_name).isNull(),
                    substitute,
                ).otherwise(F.col(column_name))
                .cast(data_type)
            )
            self.logger.debug("Cleansing Expression for " + column_name + ": " + str(cleansing_expression))
            cleansing_expressions.append((column_name, cleansing_expression))

        self.logger.info("Full null cleansing expression:")
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
