from functools import partial
from types import FunctionType

from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.column import Column

from .transformer import Transformer
from .mapper_custom_data_types import _get_select_expression_for_custom_type
import spooq.transformer.mapper_transformations as spq_trans


class Mapper(Transformer):
    """
    Constructs and applies a PySpark SQL expression, based on the provided mapping.

    Examples
    --------
    >>> from pyspark.sql import functions as F
    >>> from spooq.transformer import Mapper
    >>>
    >>> mapping = [
    >>>     ("id",            "data.relationships.food.data.id",  "StringType"),
    >>>     ("version",       "data.version",                     "extended_string_to_int"),
    >>>     ("type",          "elem.attributes.type",             "StringType"),
    >>>     ("created_at",    "elem.attributes.created_at",       "str_to_timestamp"),
    >>>     ("created_on",    "elem.attributes.created_at",       "extended_string_to_date"),
    >>>     ("process_date",  F.current_timestamp(),              "DateType"),
    >>> ]
    >>> mapper = Mapper(mapping=mapping)
    >>> mapper.transform(input_df).printSchema()
    root
     |-- id: string (nullable = true)
     |-- version: integer (nullable = true)
     |-- type: string (nullable = true)
     |-- created_at: timestamp (nullable = true)
     |-- created_on: date (nullable = true)
     |-- process_date: date (nullable = false)


    Parameters
    ----------
    mapping  : :class:`list` of :any:`tuple` containing three :any:`str` or :class:`~pyspark.sql.Column` or :mod:`~pyspark.sql.functions`
        This is the main parameter for this transformation. It gives information
        about the column names for the output DataFrame, the column names (paths)
        from the input DataFrame, and their data types. Custom data types are also supported, which can
        clean, pivot, anonymize, ... the data itself. Please have a look at the
        :py:mod:`spooq.transformer.mapper_custom_data_types` module for more information.

    ignore_missing_columns : :any:`bool`, Defaults to False
        Specifies if the mapping transformation should use NULL if a referenced input
        column is missing in the provided DataFrame. If set to False, it will raise an exception.

    ignore_ambiguous_columns : :any:`bool`, Defaults to False
        It can happen that the input DataFrame has ambiguous column names (like "Key" vs "key") which will
        raise an exception with Spark when reading. This flag surpresses this exception and skips those affected
        columns.

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
    * Source Path / Name / Column / Function: :any:`str` or :class:`~pyspark.sql.Column` or :mod:`~pyspark.sql.functions`
        Points to the name of the column in the input DataFrame. If the input
        is a flat DataFrame, it will essentially be the column name. If it is of complex
        type, it will point to the path of the actual value. For example: ``data.relationships.sample.data.id``,
        where id is the value we want. It is also possible to directly pass
        a PySpark Column which will get evaluated. This can contain arbitrary logic supported by Spark. For example:
        ``F.current_date()`` or ``F.when(F.col("size") == 180, F.lit("tall")).otherwise(F.lit("tiny"))``.
    * DataType: :any:`str` or :class:`~pyspark.sql.types.DataType`
        DataTypes can be types from :py:mod:`pyspark.sql.types`, selected custom datatypes or
        injected, ad-hoc custom datatypes.
        The datatype will be interpreted as a PySpark built-in if it is a member of :py:mod:`pyspark.sql.types` module.
        If it is not an importable PySpark data type, a method to construct the statement will be
        called by the data type's name.

    Note
    ----
    The available input columns can vary from batch to batch if you use schema inference
    (f.e. on json data) for the extraction. Ignoring missing columns on the input DataFrame is
    highly encouraged in this case. Although, if you have tight control over the structure
    of the extracted DataFrame, setting `ignore_missing_columns` to True is advised
    as it can uncover typos and bugs.

    Note
    ----
    Please see :py:mod:`spooq.transformer.mapper_custom_data_types` for all available custom
    data types and how to inject your own.

    Note
    ----
    Attention: Decimal is NOT SUPPORTED by Hive! Please use Double instead!
    """

    def __init__(self, mapping, ignore_missing_columns=False, ignore_ambiguous_columns=False, mode="replace"):
        super(Mapper, self).__init__()
        self.mapping = mapping
        self.ignore_missing_columns = ignore_missing_columns
        self.ignore_ambiguous_columns = ignore_ambiguous_columns
        self.mode = mode

    def transform(self, input_df):
        self.logger.info("Generating SQL Select-Expression for Mapping...")
        self.logger.debug("Input Schema/Mapping:")
        self.logger.debug("\n" + "\n".join(["\t".join(map(str, mapping_line)) for mapping_line in self.mapping]))

        input_columns = input_df.columns
        select_expressions = []
        with_column_expressions = []

        for (name, source_column, data_transformation) in self.mapping:
            self.logger.debug("generating Select statement for attribute: {nm}".format(nm=name))
            self.logger.debug(
                f"generate mapping for name: {name}, source_column: {source_column}, data_transformation: {data_transformation}"
            )
            source_column = self._get_spark_column(source_column, name, input_df)
            if source_column is None:
                continue
            select_expression = self._get_select_expression(name, source_column, data_transformation)

            self.logger.debug(
                "Select-Expression for Attribute {nm}: {sql_expr}".format(nm=name, sql_expr=str(select_expression))
            )
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

    def _get_spark_column(self, source_column, name, input_df):
        """
        Returns the provided source column as a Pyspark.sql.Column and marks if it is missing or not.
        Supports source column definition as a string or a Pyspark.sql.Column (including functions).
        """
        try:
            input_df.select(source_column)
            if isinstance(source_column, str):
                source_column = F.col(source_column)

        except AnalysisException as e:
            if isinstance(source_column, str) and self.ignore_missing_columns:
                self.logger.warn(
                    f"Missing column ({str(source_column)}) replaced with NULL (via ignore_missing_columns=True): {e.desc}"
                )
                source_column = F.lit(None)
            elif "ambiguous" in e.desc.lower() and self.ignore_ambiguous_columns:
                self.logger.warn(
                    f'Exception ignored (via ignore_ambiguous_columns=True) for column "{str(source_column)}": {e.desc}'
                )
                return None
            else:
                self.logger.exception(
                    'Column: "{}" cannot be resolved '.format(str(source_column))
                    + 'but is referenced in the mapping by column: "{}".\n'.format(name)
                )
                raise e
        return source_column

    def _get_spark_data_transformation_type(self, data_transformation):
        """
        Returns the provided data_transformation as a Pyspark.sql.type.DataType (for spark-built-ins)
        or as a string (for custom spooq transformations) and marks if it is built-in or not.
        Supports source column definition as a string or a Pyspark.sql.Column (including functions).
        """
        self.logger.debug(f"data_transformation: {data_transformation}")

        if isinstance(data_transformation, T.DataType):
            data_transformation_type = "spark_data_transformation"

        elif isinstance(data_transformation, str):
            if hasattr(T, data_transformation.replace("()", "")):
                data_transformation_type = "spark_data_transformation_string"
            else:
                data_transformation_type = "spooq_function_string"

        elif isinstance(data_transformation, FunctionType):
            data_transformation_type = "spooq_function"

        elif isinstance(data_transformation, partial):
            data_transformation_type = "spooq_partial"

        else:
            raise ValueError(
                "data_transformation not supported! "
                f"class: {type(data_transformation)}, "
                f"name: {data_transformation}"
            )

        return data_transformation_type

    def _get_select_expression(self, name, source_column, data_transformation):
        """
        Returns a valid pyspark sql select-expression with cast and alias, depending on the input parameters.
        """
        self.logger.debug(
            f"name: {name}, source_column: {source_column}, data_transformation: {data_transformation}"
        )
        data_transformation_type = self._get_spark_data_transformation_type(data_transformation)

        if data_transformation_type.startswith("spark"):
            return self._get_select_expression_for_spark_transformation(
                name, source_column, data_transformation, data_transformation_type
            )
        elif data_transformation_type.startswith("spooq"):
            return self._get_select_expression_for_spooq_transformation(
                name, source_column, data_transformation, data_transformation_type
            )

    @staticmethod
    def _get_select_expression_for_spark_transformation(
        name, source_column, data_transformation, data_transformation_type
    ):
        if data_transformation_type == "spark_data_transformation":
            return source_column.cast(data_transformation).alias(name)
        else:  # spark_data_transformation_string
            data_transformation = data_transformation.replace("()", "")
            data_transformation = getattr(T, data_transformation)()
            return source_column.cast(data_transformation).alias(name)

    @staticmethod
    def _get_select_expression_for_spooq_transformation(
        name, source_column, data_transformation, data_transformation_type
    ):
        if data_transformation_type == "spooq_function":
            try:  # Function returns a partial
                spooq_partial = data_transformation()
                return spooq_partial(source_column=source_column, name=name)
            except TypeError as e:  # Function is directly callable
                if "required positional arguments" not in str(e):
                    raise e
                return data_transformation(source_column=source_column, name=name)

        elif data_transformation_type == "spooq_partial":
            if not data_transformation.keywords:
                return data_transformation(source_column=source_column, name=name)
            else:
                args = data_transformation.keywords
                args.setdefault("source_column", source_column)
                args.setdefault("name", name)
                return data_transformation(**args)
        else:  # spooq_function_string
            return _get_select_expression_for_custom_type(source_column, name, data_transformation)
