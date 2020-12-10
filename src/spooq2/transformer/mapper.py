from __future__ import absolute_import
from builtins import str
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.column import Column

from .transformer import Transformer
from .mapper_custom_data_types import _get_select_expression_for_custom_type


class Mapper(Transformer):
    """
    Constructs and applies a PySpark SQL expression, based on the provided mapping.

    Examples
    --------
    >>> mapping = [
    >>>     ('id',              'data.relationships.food.data.id',     'StringType'),
    >>>     ('message_id',      'data.id',                             'StringType'),
    >>>     ('type',            'data.relationships.food.data.type',   'StringType'),
    >>>     ('created_at',      'elem.attributes.created_at',          'timestamp_ms_to_s'),
    >>>     ('updated_at',      'elem.attributes.updated_at',          'timestamp_ms_to_s'),
    >>>     ('deleted_at',      'elem.attributes.deleted_at',          'timestamp_ms_to_s'),
    >>>     ('brand',           'elem.attributes.brand',               'StringType')
    >>> ]
    >>> transformer = Mapper(mapping=mapping)

    >>> mapping = [
    >>>     ('id',          'data.relationships.food.data.id',   'StringType'),
    >>>     ('updated_at',  'elem.attributes.updated_at',        'timestamp_ms_to_s'),
    >>>     ('deleted_at',  'elem.attributes.deleted_at',        'timestamp_ms_to_s'),
    >>>     ('name',        'elem.attributes.name',              'array')
    >>> ]
    >>> transformer = Mapper(mapping=mapping)

    Parameters
    ----------
    mapping  : :class:`list` of :any:`tuple` containing three :any:`str`
        This is the main parameter for this transformation. It essentially gives information
        about the column names for the output DataFrame, the column names (paths)
        from the input DataFrame, and their data types. Custom data types are also supported, which can
        clean, pivot, anonymize, ... the data itself. Please have a look at the
        :py:mod:`spooq2.transformer.mapper_custom_data_types` module for more information.

    ignore_missing_columns : :any:`bool`, Defaults to True
        Specifies if the mapping transformation should raise an exception if a referenced input
        column is missing in the provided DataFrame.

    Note
    ----
    Let's talk about Mappings:

    The mapping should be a list of tuples which are containing all information per column.

    * Column Name : :any:`str`
        Sets the name of the column in the resulting output DataFrame.
    * Source Path / Name : :any:`str`
        Points to the name of the column in the input DataFrame. If the input
        is a flat DataFrame, it will essentially be the column name. If it is of complex
        type, it will point to the path of the actual value. For example:
        ``data.relationships.sample.data.id``, where id is the value we want.
    * DataType : :any:`str`
        DataTypes can be types from :any:`pyspark.sql.types`, selected custom datatypes or
        injected, ad-hoc custom datatypes.
        The datatype will be interpreted as a PySpark built-in if it is a member of ``pyspark.sql.types``.
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
    Please see :py:mod:`spooq2.transformer.mapper_custom_data_types` for all available custom
    data types and how to inject your own.

    Note
    ----
    Attention: Decimal is NOT SUPPORTED by Hive! Please use Double instead!
    """

    def __init__(self, mapping, ignore_missing_columns=False, mode="replace"):
        super(Mapper, self).__init__()
        self.mapping = mapping
        self.ignore_missing_columns = ignore_missing_columns
        self.mode = mode

    def transform(self, input_df):
        self.logger.info("Generating SQL Select-Expression for Mapping...")
        self.logger.debug("Input Schema/Mapping: {mp}".format(mp=str(self.mapping)))

        input_columns = input_df.columns
        select_expressions = []
        with_column_expressions = []

        for (name, source_column, data_type) in self.mapping:
            self.logger.debug(
                "generating Select statement for attribute: {nm}".format(nm=name)
            )

            source_column = self._get_spark_column(source_column, name, input_df)
            data_type, data_type_is_spark_builtin = self._get_spark_data_type(data_type)
            select_expression = self._get_select_expression(name, source_column, data_type, data_type_is_spark_builtin)

            self.logger.debug("Select-Expression for Attribute {nm}: {sql_expr}"
                              .format(nm=name, sql_expr=str(select_expression)))
            if self.mode != "replace" and name in input_columns:
                with_column_expressions.append((name, select_expression))
            else:
                select_expressions.append(select_expression)

        self.logger.info("SQL Select-Expression for new mapping generated!")
        self.logger.debug("SQL Select-Expressions for new mapping: " + str(select_expressions))
        self.logger.debug("SQL WithColumn-Expressions for new mapping: " + str(with_column_expressions))
        if self.mode == "prepend":
            df_to_return = input_df.select(select_expressions + ["*"])
        elif self.mode == "append":
            df_to_return = input_df.select(["*"] + select_expressions)
        elif self.mode == "replace":
            df_to_return = input_df.select(select_expressions)
        else:
            exception_message = ("Only 'prepend', 'append' and 'replace' are allowed for Mapper mode!"
                                 "Value: '{val}' was used as mode for the Mapper transformer.")
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
                source_column = F.lit(None)
            else:
                self.logger.exception(
                    "Column: \"{}\" cannot be resolved ".format(str(source_column)) +
                    "but is referenced in the mapping by column: \"{}\".\n".format(name))
                raise e
        return source_column

    @staticmethod
    def _get_spark_data_type(data_type):
        """
        Returns the provided data_type as a Pyspark.sql.type.DataType (for spark-built-ins)
        or as a string (for custom spooq transformations) and marks if it is built-in or not.
        Supports source column definition as a string or a Pyspark.sql.Column (including functions).
        """
        if isinstance(data_type, T.DataType):
            data_type_is_spark_builtin = True
        elif isinstance(data_type, str):
            data_type = data_type.replace("()", "")
            if hasattr(T, data_type):
                data_type_is_spark_builtin = True
                data_type = getattr(T, data_type)()
            else:
                data_type_is_spark_builtin = False
        else:
            raise ValueError(
                "data_type not supported! class: \"{}\", name: \"{}\"".format(
                    type(data_type).__name__, str(data_type)))
        return data_type, data_type_is_spark_builtin

    @staticmethod
    def _get_select_expression(name, source_column, data_type,
                               data_type_is_spark_builtin):
        """
        Returns a valid pyspark sql select-expression with cast and alias, depending on the input parameters.
        """
        if data_type_is_spark_builtin:
            return source_column.cast(data_type).alias(name)
        else:  # Custom Data Type
            return _get_select_expression_for_custom_type(source_column, name, data_type)

