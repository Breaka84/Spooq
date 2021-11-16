import sys
import pyspark.sql.functions as F, types as T
from pyspark.sql.column import Column

from .base_cleaner import BaseCleaner


class EnumCleaner(BaseCleaner):
    """
    Cleanses a dataframe based on lists of allowed|disallowed values.

    Examples
    --------
    >>> from spooq.transformer import EnumCleaner
    >>>
    >>> transformer = EnumCleaner(
    >>>     cleaning_definitions={
    >>>         "status": {
    >>>             "elements": ["active", "inactive"],
    >>>         },
    >>>         "version": {
    >>>             "elements": ["", "None", "none", "null", "NULL"],
    >>>             "mode": "disallow",
    >>>             "default": None,
    >>>         },
    >>>     }
    >>> )

    >>> from spooq.transformer import EnumCleaner
    >>> from pyspark.sql import Row
    >>>
    >>> input_df = spark.createDataFrame([
    >>>     Row(a="stay", b="positive"),
    >>>     Row(a="stay", b="negative"),
    >>>     Row(a="stay", b="positive"),
    >>> ])
    >>> transformer = EnumCleaner(
    >>>     cleaning_definitions={
    >>>         "b": {
    >>>             "elements": ["positive"],
    >>>             "mode": "allow",
    >>>         }
    >>>     },
    >>>     column_to_log_cleansed_values="cleansed_values_enum",
    >>>     store_as_map=True,
    >>> )
    >>> output_df = transformer.transform(input_df)
    >>> output_df.show()
    +----+--------+--------------------+
    |   a|       b|cleansed_values_enum|
    +----+--------+--------------------+
    |stay|positive|                  []|
    |stay|    null|     [b -> negative]|
    |stay|positive|                  []|
    +----+--------+--------------------+
    >>> output_df.printSchema()
    root
     |-- a: string (nullable = true)
     |-- b: string (nullable = true)
     |-- cleansed_values_enum: map (nullable = false)
     |    |-- key: string
     |    |-- value: string (valueContainsNull = true)

    Parameters
    ----------
    cleaning_definitions : :py:class:`dict`
        Dictionary containing column names and respective cleansing rules

    column_to_log_cleansed_values : :any:`str`, Defaults to None
        Defines a column in which the original (uncleansed) value will be stored in case of cleansing. If no column
        name is given, nothing will be logged.

    store_as_map : :any:`bool`, Defaults to False
        Specifies if the logged cleansed values should be stored in a column as :any:`pyspark.sql.types.MapType` with
        stringified values or as :any:`pyspark.sql.types.StructType` with the original respective data types.

    Note
    ----
    Following cleansing rule attributes per column are supported:

        * elements, mandatory - :class:`list`
            A list of elements which will be used to allow or reject (based on mode) values from the input DataFrame.
        * mode, allow|disallow, defaults to 'allow' - :any:`str`
            "allow" will set all values which are NOT in the list (ignoring NULL) to the default value.
            "disallow" will set all values which ARE in the list (ignoring NULL) to the default value.
        * default, defaults to None - :class:`~pyspark.sql.column.Column` or any primitive Python value
            If a value gets cleansed it gets replaced with the provided default value.

    Returns
    -------
    :any:`pyspark.sql.DataFrame`
        The transformed DataFrame

    Raises
    ------
    :any:`exceptions.ValueError`
        Enumeration-based cleaning requires a non-empty list of elements per cleaning rule!
        Spooq did not find such a list for column: {column_name}

    :any:`exceptions.ValueError`
        Only the following modes are supported by EnumCleaner: 'allow' and 'disallow'.

    Warning
    -------
    None values are explicitly ignored as input values because `F.lit(None).isin(["elem1", "elem2"])` will neither
    return True nor False but None.
    If you want to replace Null values you should use the transformer :py:class:`spooq.transformer.NullCleaner`
    """

    def __init__(self, cleaning_definitions={}, column_to_log_cleansed_values=None, store_as_map=False):
        super().__init__(
            cleaning_definitions=cleaning_definitions,
            column_to_log_cleansed_values=column_to_log_cleansed_values,
            store_as_map=store_as_map,
            temporary_columns_prefix="9b7798529fef529c8f2586be7ca43a66",
        )
        self.logger.debug("Enumeration List: " + str(self.cleaning_definitions))

    def transform(self, input_df):
        self.logger.debug("input_df Schema: " + input_df._jdf.schema().treeString())
        if self.column_to_log_cleansed_values:
            column_names_to_clean = self.cleaning_definitions.keys()
            temporary_column_names = self._get_temporary_column_names(column_names_to_clean)
            input_df = self._add_temporary_columns(input_df, column_names_to_clean, temporary_column_names)

        for column_name, cleaning_definition in list(self.cleaning_definitions.items()):
            self.logger.debug(f"Cleaning Definition for Column {column_name}: {str(cleaning_definition)}")
            elements = cleaning_definition.get("elements", None)
            if not elements:
                raise ValueError(
                    f"Enumeration-based cleaning requires a non-empty list of elements per cleaning rule!",
                    f"\nSpooq did not find such a list for column: {column_name}",
                )
            mode = cleaning_definition.get("mode", "NOT_DEFINED")
            substitute = cleaning_definition.get("default", None)
            data_type = input_df.schema[column_name].dataType
            if not isinstance(substitute, Column):
                substitute = F.lit(substitute)

            if mode == "allow":
                input_df = input_df.withColumn(
                    column_name,
                    F.when(F.col(column_name).isNull(), F.lit(None))
                    .otherwise(F.when(F.col(column_name).isin(elements), F.col(column_name)).otherwise(substitute))
                    .cast(data_type),
                )
            elif mode == "disallow":
                input_df = input_df.withColumn(
                    column_name,
                    F.when(F.col(column_name).isNull(), F.lit(None))
                    .otherwise(F.when(F.col(column_name).isin(elements), substitute).otherwise(F.col(column_name)))
                    .cast(data_type),
                )
            elif mode == "NOT_DEFINED":
                raise RuntimeError(
                    f"Please provide a `mode` attribute! Possible values are: ['allow', 'disallow'].\n"
                    f"column_name: {column_name} and cleaning_definition: {cleaning_definition}"
                )
            else:
                raise ValueError(
                    f"Only the following modes are supported by EnumCleaner: ['allow', 'disallow'].\n"
                    f"column_name: {column_name} and cleaning_definition: {cleaning_definition}"
                )

        if self.column_to_log_cleansed_values:
            input_df = self._log_cleansed_values(input_df, column_names_to_clean, temporary_column_names)

        return input_df
