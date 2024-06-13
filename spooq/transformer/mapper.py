from functools import partial
from types import FunctionType
from typing import Union, List, Tuple, Any, Callable
import warnings

from pyspark.sql.utils import AnalysisException, ParseException
from pyspark.sql import (
    functions as F,
    types as T,
    DataFrame,
    Column,
)

from .transformer import Transformer
from .mapper_custom_data_types import _get_select_expression_for_custom_type
from .mapper_transformations import as_is

class DataTypeValidationFailed(ValueError):
    pass


class Mapper(Transformer):
    """
    Selects, transforms and casts or validates a DataFrame based on the provided mapping.

    Examples
    --------
    >>> from pyspark.sql import functions as F, types as T
    >>> from spooq.transformer import Mapper
    >>> from spooq.transformer import mapper_transformations as spq
    >>>
    >>> mapping = [
    >>>     ("id",            "data.relationships.food.data.id",  spq.to_str),
    >>>     ("version",       "data.version",                     spq.to_int),
    >>>     ("type",          "elem.attributes.type",             "string"),
    >>>     ("created_at",    "elem.attributes.created_at",       spq.to_timestamp),
    >>>     ("created_on",    "elem.attributes.created_at",       spq.to_timestamp(cast="date")),
    >>>     ("processed_at",  F.current_timestamp(),              spq.as_is,
    >>> ]
    >>> mapper = Mapper(mapping=mapping)
    >>> mapper.transform(input_df).printSchema()
    root
     |-- id: string (nullable = true)
     |-- version: integer (nullable = true)
     |-- type: string (nullable = true)
     |-- created_at: timestamp (nullable = true)
     |-- created_on: date (nullable = true)
     |-- processed_at: timestamp (nullable = false)

    Parameters
    ----------
    mapping  : :class:`list` of :any:`tuple` containing three elements, respectively.
        This is the main parameter for this transformation. It gives information
        about the column names for the output DataFrame, the column names (paths)
        from the input DataFrame, and their data types. Custom data types are also supported, which can
        clean, pivot, anonymize, ... the data itself. Please have a look at the
        :py:mod:`spooq.transformer.mapper_custom_data_types` module for more information.

    missing_column_handling : :any:`str`, Defaults to 'raise_error'
        Specifies how to proceed in case a source column does not exist in the source DataFrame:
            * raise_error (default)
                Raise an exception
            * nullify
                Create source column filled with null
            * skip
                Skip the mapping transformation

    ignore_ambiguous_columns : :any:`bool`, Defaults to False
        It can happen that the input DataFrame has ambiguous column names (like "Key" vs "key") which will
        raise an exception with Spark when reading. This flag surpresses this exception and skips those affected
        columns.

    mode : :any:`str`, Defaults to "replace"
        Defines the operation mode of the mapping transformation.
        Following modes are supported:

            * replace
                The output schema is the same as the provided mapping.
                => output schema: columns from mapping
            * append
                The columns provided in the mapping are added at the end of the input schema. If a column already
                exists in the input DataFrame, its position is kept.
                => output schema: input columns + columns from mapping
            * prepend
                The columns provided in the mapping are added at the beginning of the input schema. If a column already
                exists in the input DataFrame, its position is kept.
                => output schema: columns from mapping + input columns
            * rename_and_validate
                All built-in, custom transformations (except renaming) and casts are disabled. The Mapper only
                renames the columns and validates that the output data type is the same as the input data type. The
                transformation will fail if any spooq / custom transformations (except `as_is`) are defined!
                => output schema: columns from mapping


    Keyword Arguments
    -----------------
    ignore_missing_columns : :any:`bool`, Defaults to False
        DEPRECATED: please use missing_column_handling instead!


    Note
    ----
    Let's talk about Mappings:

    The mapping should be a list of tuples that contain all necessary information per column.

    * Column Name: :any:`str`
        Sets the name of the column in the resulting output DataFrame.
    * Source Path / Name / Column / Function: :any:`str`, :class:`~pyspark.sql.Column` or :mod:`~pyspark.sql.functions`
        Points to the name of the column in the input DataFrame. If the input
        is a flat DataFrame, it will essentially be the column name. If it is of complex
        type, it will point to the path of the actual value. For example: ``data.relationships.sample.data.id``,
        where id is the value we want. It is also possible to directly pass
        a PySpark Column which will get evaluated. This can contain arbitrary logic supported by Spark. For example:
        ``F.current_date()`` or ``F.when(F.col("size") == 180, F.lit("tall")).otherwise(F.lit("tiny"))``.
    * DataType: :any:`str`, :class:`~pyspark.sql.types.DataType` or :mod:`~spooq.transformer.mapper_transformations`
        DataTypes can be types from :py:mod:`pyspark.sql.types` (like T.StringType()),
        simple strings supported by PySpark (like "string") and custom transformations provided by spooq
        (like spq.to_timestamp). You can find more information about the transformations at
        https://spooq.rtfd.io/en/latest/transformer/mapper.html#module-spooq.transformer.mapper_transformations.

    Note
    ----
    The available input columns can vary from batch to batch if you use schema inference
    (f.e. on json data) for the extraction. Via the parameter `missing_column_handling` you can specify a strategy on
    how to handle missing columns on the input DataFrame.
    It is advised to use the 'raise_error' option as it can uncover typos and bugs.
    """

    def __init__(
        self,
        mapping: List[Tuple[Any, Any, Any]],
        ignore_ambiguous_columns: bool = False,
        missing_column_handling: str = "raise_error",
        mode: str = "replace",
        **kwargs
    ):
        super(Mapper, self).__init__()
        self.mapping = mapping
        self.missing_column_handling = missing_column_handling
        self.ignore_ambiguous_columns = ignore_ambiguous_columns
        self.mode = mode

        if self.mode not in ["replace", "append", "prepend", "rename_and_validate"]:
            raise ValueError(f"""Value: '{mode}' was used as mode for the Mapper transformer.
                Only the following values are allowed for `mode`:
                - replace: The output schema is the same as the provided mapping.
                - append: The columns provided in the mapping are added at the end of the input schema.
                - prepend: The columns provided in the mapping are added at the beginning of the input schema.
                - rename_and_validate: The Mapper only renames the columns and validates that the output data
                  type is the same as the input data type.
            """)

        if "ignore_missing_columns" in kwargs:
            message = "Parameter `ignore_missing_columns` is deprecated, use `missing_column_handling` instead!"
            if kwargs["ignore_missing_columns"]:
                message += "\n`missing_column_handling` was set to `nullify` because you defined " \
                           "`ignore_missing_columns=True`!"
                self.missing_column_handling = "nullify"
            self.logger.warning(message)
            warnings.warn(message=message, category=FutureWarning)

        if self.missing_column_handling not in ["raise_error", "skip", "nullify"]:
            raise ValueError("""Only the following values are allowed for `missing_column_handling`:
                - raise_error: raise an exception in case the source column is missing
                - skip: skip transformation in case the source is missing
                - nullify: set source column to null in case it is missing""")

    def transform(self, input_df: DataFrame) -> DataFrame:
        self.logger.info("Generating SQL Select-Expression for Mapping...")
        self.logger.debug("Input Schema/Mapping:")
        self.logger.debug("\n" + "\n".join(["\t".join(map(str, mapping_line)) for mapping_line in self.mapping]))

        input_columns = input_df.columns
        select_expressions = []
        with_column_expressions = []
        validation_errors = []

        for (name, source_column, data_transformation) in self.mapping:
            self.logger.debug(f"generating Select statement for attribute: {name}")
            self.logger.debug(
                f"generate mapping for name: {name}, "
                f"source_column: {source_column}, "
                f"data_transformation: {data_transformation}"
            )
            source_column: Column = self._get_source_spark_column(source_column, name, input_df)
            if source_column is None:
                if self.mode == "rename_and_validate":
                    validation_errors.append(DataTypeValidationFailed(f"Input column: {name} not found!"))
                continue

            source_spark_data_type: T.DataType = self._get_spark_data_type(input_df.select(source_column).dtypes[0][1])
            target_transformation: Union[T.DataType, Column] = (
                self._get_spark_data_type(data_transformation)
                or self._get_spooq_transformation(name, source_column, data_transformation)
            )

            if self.mode == "rename_and_validate":
                try:
                    self._validate_data_type(
                        name,
                        source_spark_data_type,
                        data_transformation,
                        target_transformation,
                    )
                except DataTypeValidationFailed as e:
                    self.logger.warning(e)
                    validation_errors.append(e)

            if isinstance(target_transformation, T.DataType):
                if target_transformation != source_spark_data_type:
                    select_expression = source_column.cast(target_transformation).alias(name)
                else:
                    select_expression = source_column.alias(name)
            else:
                select_expression = target_transformation

            self.logger.debug(f"Select-Expression for Attribute {name}: {select_expression}")
            if self.mode in ["append", "prepend"] and name in input_columns:
                with_column_expressions.append((name, select_expression))
            else:
                select_expressions.append(select_expression)

        if validation_errors:
            raise DataTypeValidationFailed(f"Validation has failed!\n{validation_errors}")

        self.logger.info("SQL Select-Expression for new mapping generated!")
        self.logger.debug("SQL Select-Expressions for new mapping:\n" + "\n".join(str(select_expressions).split(",")))
        self.logger.debug("SQL WithColumn-Expressions for new mapping: " + str(with_column_expressions))
        if self.mode == "prepend":
            df_to_return = input_df.select(select_expressions + ["*"])
        elif self.mode == "append":
            df_to_return = input_df.select(["*"] + select_expressions)
        else:
            df_to_return = input_df.select(select_expressions)

        if with_column_expressions:
            for name, expression in with_column_expressions:
                df_to_return = df_to_return.withColumn(name, expression)
        return df_to_return

    def _get_source_spark_column(
        self, source_column: Union[str, Column], name: str, input_df: DataFrame
    ) -> Union[Column, None]:
        """
        Returns the provided source column as a Column.
        Supports source column definition as a string or a Column (including functions).
        """
        try:
            input_df.select(source_column)
            if isinstance(source_column, str):
                source_column = F.col(source_column)

        except AnalysisException as e:
            if isinstance(source_column, str) and self.missing_column_handling == "skip":
                self.logger.warning(
                    f"Missing column ({str(source_column)}) skipped (via missing_column_handling='skip'): {e.desc}"
                )
                return None
            elif isinstance(source_column, str) and self.missing_column_handling == "nullify":
                self.logger.warning(
                    f"Missing column ({str(source_column)}) replaced with NULL (via missing_column_handling='nullify'): {e.desc}"
                )
                source_column = F.lit(None)
            elif "ambiguous" in e.desc.lower() and self.ignore_ambiguous_columns:
                self.logger.warning(
                    f'Exception ignored (via ignore_ambiguous_columns=True) for column "{source_column}": {e.desc}'
                )
                return None
            else:
                self.logger.exception(f"""
                Column: '{source_column}' cannot be resolved but is referenced in the mapping by column: '{name}'.
                You can make use of the following parameters to handle missing input columns:
                - missing_column_handling='nullify': set missing source column to null
                - missing_column_handling='skip': skip the transformation
                """)
                raise e
        return source_column

    def _get_spark_data_type(
        self, data_transformation: Union[str, Column, Callable, T.DataType]
    ) -> T.DataType:
        """
        Returns the PySpark data type as Python object (None if not found). Supports Python objects and strings.
        """
        data_type = None

        if isinstance(data_transformation, T.DataType):
            data_type = data_transformation  # Spark datatype as Python object

        elif isinstance(data_transformation, str):
            data_type_ = data_transformation.replace("()", "")
            try:
                data_type = getattr(T, data_type_)()  # Spark datatype as string
            except AttributeError:
                try:
                    data_type = T._parse_datatype_string(data_type_)  # Spark datatype as short string
                except ParseException:
                    pass

        return data_type

    def _validate_data_type(
        self,
        name: str,
        source_spark_data_type: T.DataType,
        original_data_transformation: Union[str, T.DataType, Callable],
        target_transformation: Union[T.DataType, Column],
    ):
        """
        Validates that the input and output data types are the same. NULL sources and as_is transformations are valid.
        """
        if isinstance(target_transformation, T.DataType):
            if isinstance(source_spark_data_type, T.NullType):
                self.logger.warning(
                    f"The data type of column {name} could not be validated because the source "
                    "data type is NULL!"
                )
            elif target_transformation != source_spark_data_type:
                raise DataTypeValidationFailed(
                    f"The mode is set to `rename_and_validate` but the target data_type ({target_transformation}) "
                    f"does not match the source data_type ({source_spark_data_type}) for the column: {name}!"
                )
            else:
                self.logger.debug(f"No validation errors found for column: {name}")
        else:
            if original_data_transformation in [as_is, "as_is"]:
                pass
            else:
                raise DataTypeValidationFailed(
                    f"Spooq transformations are not allowed in 'rename_and_validate' mode! Name: {name}, transformation: {original_data_transformation}"
                )

    def _get_spooq_transformation(
        self, name: str, source_column: Union[str, Column], data_transformation: Union[str, Callable]
    ) -> Column:
        """
        Applies the defined spooq transformation and returns the target column.
        """
        self.logger.debug(
            f"Get spooq transformation for column: {name} (source: {source_column}) with "
            f"following requested transformation: {data_transformation}"
        )
        if isinstance(data_transformation, FunctionType):
            # function without brackets / parameters (f.e.: `spq.as_is`)
            spooq_partial = data_transformation()
            return spooq_partial(source_column=source_column, name=name)

        elif isinstance(data_transformation, partial):
            # function with brackets / parameters (f.e.: `spq.as_is()` or `spq.as_is(cast="string")`)
            args = data_transformation.keywords
            args.setdefault("source_column", source_column)
            args.setdefault("name", name)
            return data_transformation(**args)
        else:
            # spooq_function_string (f.e.: `"as_is"`)
            return _get_select_expression_for_custom_type(source_column, name, data_transformation)
