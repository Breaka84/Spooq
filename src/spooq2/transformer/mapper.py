from __future__ import absolute_import
from builtins import str
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit
from pyspark.sql import types as sql_types

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
    Please see :py:mod:`spooq2.transformer.mapper_custom_data_types` for all available custom
    data types and how to inject your own.

    Note
    ----
    Attention: Decimal is NOT SUPPORTED by Hive! Please use Double instead!
    """

    def __init__(self, mapping, ignore_missing_columns=True):
        super(Mapper, self).__init__()
        self.mapping = mapping
        self.ignore_missing_columns = ignore_missing_columns
        self.logger.debug("Mapping: {mp}".format(mp=str(self.mapping)))

    def transform(self, input_df):
        self.logger.info("Schema: " + str(self.mapping))

        select_expressions = []

        for (name, path, data_type) in self.mapping:
            self.logger.debug(
                "generating Select statement for attribute: {nm}".format(nm=name)
            )

            data_type = data_type.replace("()", "")
            data_type_is_spark_builtin = hasattr(sql_types, data_type)
            path_segments = path.split(".")

            source_column = input_df
            source_column_is_missing = False
            for path_segment in path_segments:
                try:
                    source_column = source_column[path_segment]
                    input_df.select(source_column)
                except AnalysisException:
                    if self.ignore_missing_columns:
                        source_column = None
                        source_column_is_missing = True
                        break
                    else:
                        raise ValueError(
                            "Column: \"{}\" is missing in the input DataFrame ".format(path) +
                            "but is referenced in the mapping by column: \"{}\"".format(name)
                        )

            del path_segment, path_segments

            if data_type_is_spark_builtin:

                if source_column_is_missing:
                    select_expression = lit(None)
                else:
                    select_expression = source_column

                select_expressions.append(
                    select_expression.cast(getattr(sql_types, data_type)()).alias(name)
                )

            else:  # Custom Data Type

                if source_column_is_missing:
                    select_expression = lit(None).alias(name)
                else:
                    select_expression = _get_select_expression_for_custom_type(
                        source_column, name, data_type
                    )

                select_expressions.append(select_expression)

        self.logger.info(
            "SQL Select-Expression for mapping: " + str(select_expressions)
        )
        spark_sql_query = input_df.select(select_expressions)

        return spark_sql_query
