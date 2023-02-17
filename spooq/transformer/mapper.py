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


class Mapper(Transformer):
    """
    Constructs and applies a PySpark SQL expression, based on the provided mapping.

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

    skip_missing_columns : :any:`bool`, Defaults to False
        Specifies if the mapping transformation should be skipped if a referenced input
        column is missing in the provided DataFrame. Only one of `nullify_missing_columns` and `skip_missing_columns`
        can be set to True. If none of the two is set to True then an exception will be raised in case the input
        column was not found.

    nullify_missing_columns : :any:`bool`, Defaults to False
        Specifies if the mapping transformation should use NULL if a referenced input
        column is missing in the provided DataFrame. Only one of `nullify_missing_columns` and `skip_missing_columns`
        can be set to True. If none of the two is set to True then an exception will be raised in case the input
        column was not found.

    ignore_ambiguous_columns : :any:`bool`, Defaults to False
        It can happen that the input DataFrame has ambiguous column names (like "Key" vs "key") which will
        raise an exception with Spark when reading. This flag surpresses this exception and skips those affected
        columns.

    ignore_missing_columns : :any:`bool`, Defaults to False
        DEPRECATED: please use nullify_missing_columns instead!

    mode : :any:`str`, Defaults to "replace"
        Defines weather the mapping should fully replace the schema of the input DataFrame or just add to it.
        Following modes are supported:

            * replace
                The output schema is the same as the provided mapping.
                => output schema: new columns
            * append
                The columns provided in the mapping are added at the end of the input schema. If a column already
                exists in the input DataFrame, its position is kept.
                => output schema: input columns + new columns
            * prepend
                The columns provided in the mapping are added at the beginning of the input schema. If a column already
                exists in the input DataFrame, its position is kept.
                => output schema: new columns + input columns

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
    (f.e. on json data) for the extraction. Ignoring missing columns on the input DataFrame is
    highly encouraged in this case. Although, if you have tight control over the structure
    of the extracted DataFrame, setting `ignore_missing_columns` to True is advised
    as it can uncover typos and bugs.
    """

    def __init__(
        self,
        mapping: List[Tuple[Any, Any, Any]],
        skip_missing_columns: bool = False,
        nullify_missing_columns: bool = False,
        ignore_ambiguous_columns: bool = False,
        ignore_missing_columns: bool = False,
        mode: str = "replace",
    ):
        super(Mapper, self).__init__()
        if ignore_missing_columns:
            self.logger.warn("Parameter `ignore_missing_columns` is deprecated, use `nullify_missing_columns` instead!")
            warnings.warn(
                message="Parameter `ignore_missing_columns` is deprecated, use `nullify_missing_columns` instead!",
                category=FutureWarning
            )
        self.mapping = mapping
        self.skip_missing_columns = skip_missing_columns
        self.nullify_missing_columns = nullify_missing_columns or ignore_missing_columns
        if self.nullify_missing_columns and self.skip_missing_columns:
            raise ValueError(
                "Only one of the parameters `nullify_missing_columns` (before `ignore_missing_columns`) and "
                "`skip_missing_columns` can be set to True!"
            )
        self.ignore_ambiguous_columns = ignore_ambiguous_columns
        self.mode = mode

    def transform(self, input_df: DataFrame) -> DataFrame:
        self.logger.info("Generating SQL Select-Expression for Mapping...")
        self.logger.debug("Input Schema/Mapping:")
        self.logger.debug("\n" + "\n".join(["\t".join(map(str, mapping_line)) for mapping_line in self.mapping]))

        input_columns = input_df.columns
        select_expressions = []
        with_column_expressions = []

        for (name, source_column, data_transformation) in self.mapping:
            self.logger.debug("generating Select statement for attribute: {nm}".format(nm=name))
            self.logger.debug(
                f"generate mapping for name: {name}, "
                f"source_column: {source_column}, "
                f"data_transformation: {data_transformation}"
            )
            source_column = self._get_spark_column(source_column, name, input_df)
            if source_column is None:
                continue
            select_expression = self._get_select_expression(name, source_column, data_transformation)

            self.logger.debug(f"Select-Expression for Attribute {name}: {select_expression}")
            if self.mode != "replace" and name in input_columns:
                with_column_expressions.append((name, select_expression))
            else:
                select_expressions.append(select_expression)

        self.logger.info("SQL Select-Expression for new mapping generated!")
        self.logger.debug("SQL Select-Expressions for new mapping:\n" + "\n".join(str(select_expressions).split(",")))
        self.logger.debug("SQL WithColumn-Expressions for new mapping: " + str(with_column_expressions))
        if self.mode == "prepend":
            df_to_return = input_df.select(select_expressions + ["*"])
        elif self.mode == "append":
            df_to_return = input_df.select(["*"] + select_expressions)
        elif self.mode == "replace":
            df_to_return = input_df.select(select_expressions)
        else:
            exception_message = (
                "Only 'prepend', 'append' and 'replace' are allowed for Mapper mode!"
                "Value: '{val}' was used as mode for the Mapper transformer."
            )
            self.logger.exception(exception_message)
            raise ValueError(exception_message)

        if with_column_expressions:
            for name, expression in with_column_expressions:
                df_to_return = df_to_return.withColumn(name, expression)
        return df_to_return

    def _get_spark_column(
        self, source_column: Union[str, Column], name: str, input_df: DataFrame
    ) -> Union[Column, None]:
        """
        Returns the provided source column as a Pyspark.sql.Column and marks if it is missing or not.
        Supports source column definition as a string or a Pyspark.sql.Column (including functions).
        """
        try:
            input_df.select(source_column)
            if isinstance(source_column, str):
                source_column = F.col(source_column)

        except AnalysisException as e:
            if isinstance(source_column, str) and self.skip_missing_columns:
                self.logger.warn(
                    f"Missing column ({source_column}) skipped (via skip_missing_columns=True): {e.desc}"
                )
                return None
            elif isinstance(source_column, str) and self.nullify_missing_columns:
                self.logger.warn(
                    f"Missing column ({source_column}) replaced with NULL (via nullify_missing_columns=True): {e.desc}"
                )
                source_column = F.lit(None)
            elif "ambiguous" in e.desc.lower() and self.ignore_ambiguous_columns:
                self.logger.warn(
                    f'Exception ignored (via ignore_ambiguous_columns=True) for column "{source_column}": {e.desc}'
                )
                return None
            else:
                self.logger.exception(f"""
                Column: '{source_column}' cannot be resolved but is referenced in the mapping by column: '{name}'.
                You can make use of the following parameters to handle missing input columns:
                - `nullify_missing_columns`: set missing source column to null
                - `skip_missing_columns`: skip the transformation
                """)
                raise e
        return source_column

    def _get_select_expression(
        self, name: str, source_column: Union[str, Column], data_transformation: Union[str, Column, Callable]
    ) -> Column:
        """
        Returns a valid pyspark sql select-expression with cast and alias, depending on the input parameters.
        """
        self.logger.debug(f"name: {name}, source_column: {source_column}, data_transformation: {data_transformation}")
        try:
            return self._get_select_expression_for_spark_transformation(name, source_column, data_transformation)
        except ValueError:
            return self._get_select_expression_for_spooq_transformation(name, source_column, data_transformation)

    @staticmethod
    def _get_select_expression_for_spark_transformation(
        name: str, source_column: Union[str, Column], data_transformation: Union[str, Column, Callable]
    ) -> Column:
        data_type = None
        try:
            data_type = getattr(T, str(data_transformation).replace("()", ""))()
        except AttributeError:
            pass
        try:
            data_type = T._parse_datatype_string(str(data_transformation))
        except ParseException:
            pass

        if data_type is None:
            raise ValueError(f"No suitable Spark Data Type found for {data_transformation}")

        return source_column.cast(data_type).alias(name)

    @staticmethod
    def _get_select_expression_for_spooq_transformation(
        name: str, source_column: Union[str, Column], data_transformation: Union[str, Column, Callable]
    ) -> Column:
        if isinstance(data_transformation, FunctionType):
            try:  # Function returns a partial
                spooq_partial = data_transformation()
                return spooq_partial(source_column=source_column, name=name)
            except TypeError as e:  # Function is directly callable
                if "required positional arguments" not in str(e):
                    raise e
                return data_transformation(source_column=source_column, name=name)

        elif isinstance(data_transformation, partial):
            args = data_transformation.keywords
            args.setdefault("source_column", source_column)
            args.setdefault("name", name)
            return data_transformation(**args)
        else:  # spooq_function_string
            return _get_select_expression_for_custom_type(source_column, name, data_transformation)
