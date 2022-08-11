"""This is a collection of module level methods to construct a specific
PySpark DataFrame query for custom defined data types.

These methods are not meant to be called directly but via the
the :py:class:`~spooq.transformer.mapper.Mapper` transformer.
Please see that particular class on how to apply custom data types.

For injecting your **own custom data types**, please have a visit to the
:py:meth:`add_custom_data_type` method!
"""
import sys
from functools import partial
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spooq.transformer import mapper_transformations as spq

__all__ = ["add_custom_data_type"]


def add_custom_data_type(function_name, func):
    """
    Registers a custom data type in runtime to be used with the :py:class:`~spooq.transformer.mapper.Mapper` transformer.

    Example
    -------
    >>> import spooq.transformer.mapper_custom_data_types as custom_types
    >>> import spooq.transformer as T
    >>> from pyspark.sql import Row, functions as F, types as T

    >>> def hello_world(source_column, name):
    >>>     "A UDF (User Defined Function) in Python"
    >>>     def _to_hello_world(col):
    >>>         if not col:
    >>>             return None
    >>>         else:
    >>>             return "Hello World"
    >>>
    >>>     udf_hello_world = F.udf(_to_hello_world, T.StringType())
    >>>     return udf_hello_world(source_column).alias(name)
    >>>
    >>> input_df = spark.createDataFrame(
    >>>     [Row(hello_from=u'tsivorn1@who.int'),
    >>>      Row(hello_from=u''),
    >>>      Row(hello_from=u'gisaksen4@skype.com')]
    >>> )
    >>>
    >>> custom_types.add_custom_data_type(function_name="hello_world", func=hello_world)
    >>> transformer = T.Mapper(mapping=[("hello_who", "hello_from", "hello_world")])
    >>> df = transformer.transform(input_df)
    >>> df.show()
    +-----------+
    |  hello_who|
    +-----------+
    |Hello World|
    |       null|
    |Hello World|
    +-----------+

    >>> def first_and_last_name(source_column, name):
    >>>     "A PySpark SQL expression referencing multiple columns"
    >>>     return F.concat_ws("_", source_column, F.col("attributes.last_name")).alias(name)
    >>>
    >>> custom_types.add_custom_data_type(function_name="full_name", func=first_and_last_name)
    >>>
    >>> transformer = T.Mapper(mapping=[
    >>>     ("first_name", "attributes.first_name", "StringType"),
    >>>     ("last_name",  "attributes.last_name",  "StringType"),
    >>>     ("full_name",  "attributes.first_name", "full_name"),
    >>> ])

    Parameters
    ----------
    function_name: :any:`str`
        The name of your custom data type

    func: compatible function
        The PySpark dataframe function which will be called on a column, defined in the mapping
        of the Mapper class.
        Required input parameters are ``source_column`` and ``name``.
        Please see the note about required input parameter of custom data types for more information!

    Note
    ----
    Required input parameter of custom data types:

    **source_column** (:py:class:`pyspark.sql.Column`) - This is where your logic will be applied.
      The mapper transformer takes care of calling this method with the right column so you can just
      handle it like an object which you would get from ``df["some_attribute"]``.

    **name** (:any:`str`) - The name how the resulting column will be named. Nested attributes are not
      supported. The Mapper transformer takes care of calling this method with the right column name.
    """

    if not function_name.startswith("_generate_select_expression_for_"):
        function_name = "_generate_select_expression_for_" + function_name
    current_module = sys.modules[__name__]
    setattr(current_module, function_name, func)


def _get_select_expression_for_custom_type(source_column, name, data_type):
    """ Internal method for calling functions dynamically """
    try:
        if hasattr(spq, data_type):
            return getattr(spq, data_type)()(source_column, name)

        else:
            current_module = sys.modules[__name__]
            function_name = "_generate_select_expression_for_" + data_type
            return getattr(current_module, function_name)(source_column, name)
    except AttributeError as e:
        error_msg = (
            "Spooq could not find a Select Expression Generator Method \n"
            + "for the custom DataType: "
            + '"'
            + data_type
            + '"'
            + "\n"
            + "Original Error Message: "
            + str(e)
        )
        raise AttributeError(error_msg)


def _generate_select_expression_for_as_is(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.as_is` directly instead.
    """
    return spq.as_is()(source_column, name)


def _generate_select_expression_for_keep(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.as_is` directly instead.
    """
    return spq.as_is()(source_column, name)


def _generate_select_expression_for_no_change(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.as_is` directly instead.
    """
    return spq.as_is()(source_column, name)


def test_generate_select_expression_without_casting(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.as_is` directly instead.
    """
    return spq.as_is()(source_column, name)


def _generate_select_expression_for_json_string(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_json_string` directly instead.
    """
    return spq.to_json_string()(source_column, name)


def _generate_select_expression_for_timestamp_ms_to_ms(source_column, name):
    """
    Deprecated!

    Please just use :dt.:`~spooq.transformer.mapper_transformations.as_is` with ``"long"`` as data_type.
    This method doesn't do any cleansing anymore!
    """
    return spq.as_is(cast="long")(source_column, name)


def _generate_select_expression_for_timestamp_ms_to_s(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.apply` with a lambda instead.
    """
    return spq.apply(func=lambda val: F.lit(val).cast("double") / 1000.0, cast="long")(source_column, name)


def _generate_select_expression_for_timestamp_s_to_ms(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.apply` with a lambda instead.
    """
    return spq.apply(func=lambda val: F.lit(val).cast("double") * 1000.0, cast="long")(source_column, name)


def _generate_select_expression_for_timestamp_s_to_s(source_column, name):
    """
    Deprecated!

    Please just use :dt.:`~spooq.transformer.mapper_transformations.as_is` with ``"long"`` as data_type.
    This method doesn't do any cleansing anymore!
    """
    return spq.as_is(cast="long")(source_column, name)


def _generate_select_expression_for_StringNull(source_column, name):  # noqa: N802
    """
    Deprecated!

    Please just use ``F.lit(None)`` as source_column and ``"string"`` as data_type!
    """

    return F.lit(None).cast("string").alias(name)


def _generate_select_expression_for_IntNull(source_column, name):  # noqa: N802
    """
    Deprecated!

    Please just use ``F.lit(None)`` as source_column and ``"int"`` as data_type!
    """

    return F.lit(None).cast("int").alias(name)


def _generate_select_expression_for_StringBoolean(source_column, name):  # noqa: N802
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.has_value` instead.

    Used for Anonymizing.
    The column's value will be replaced by ``"1"`` if it is:

        * not NULL and
        * not an empty string

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame(
    >>>     [Row(email=u'tsivorn1@who.int'),
    >>>      Row(email=u''),
    >>>      Row(email=u'gisaksen4@skype.com')]
    >>> )
    >>>
    >>> mapping = [("email", "email", "StringBoolean")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(email=u'1'), Row(email=None), Row(email=u'1')]
    """
    return (
        F.when(source_column.isNull(), F.lit(None))
            .when(source_column == "", F.lit(None))
            .otherwise("1")
            .cast("string")
            .alias(name)
    )


def _generate_select_expression_for_IntBoolean(source_column, name):  # noqa: N802
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.has_value` instead.

    Used for Anonymizing.
    The column's value will be replaced by ``1`` if it contains a non-NULL value.

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame(
    >>>     [Row(facebook_id=3047288),
    >>>      Row(facebook_id=0),
    >>>      Row(facebook_id=None)]
    >>> )
    >>>
    >>> mapping = [("facebook_id", "facebook_id", "IntBoolean")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(facebook_id=1), Row(facebook_id=1), Row(facebook_id=None)]

    Note
    ----
    ``0`` (zero) or negative numbers are still considered as valid values and therefore converted to ``1``.
    """
    return F.when(source_column.isNull(), F.lit(None)).otherwise(1).cast("int").alias(name)


def _generate_select_expression_for_TimestampMonth(source_column, name):  # noqa: N802
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.apply` instead.
    """
    _truncate_day = partial(F.trunc, format="month")
    return spq.apply(
        source_column=source_column,
        name=name,
        func=_truncate_day,
        cast="timestamp"
    )


def _generate_select_expression_for_meters_to_cm(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.meters_to_cm` directly instead.
    """
    return spq.meters_to_cm()(source_column, name)


def _generate_select_expression_for_has_value(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.has_value` directly instead.
    """
    return spq.has_value()(source_column, name)


def _generate_select_expression_for_unix_timestamp_ms_to_spark_timestamp(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_timestamp` directly instead.
    """
    return spq.to_timestamp()(source_column, name)


def _generate_select_expression_for_extended_string_to_int(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_num` directly instead
    and define the ``cast`` as ``"int"``.
    """
    return spq.to_num(cast="int")(source_column, name)


def _generate_select_expression_for_extended_string_to_long(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_num` directly instead
    and define the ``cast`` as ``"long"``.
    """
    return spq.to_num(cast="long")(source_column, name)


def _generate_select_expression_for_extended_string_to_float(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_num` directly instead
    and define the ``cast`` as ``"float"``.
    """
    return spq.to_num(cast="float")(source_column, name)


def _generate_select_expression_for_extended_string_to_double(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_num` directly instead
    and define the ``cast`` as ``"double"``.
    """
    return spq.to_num(cast="double")(source_column, name)


def _generate_select_expression_for_extended_string_to_boolean(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_bool` directly instead.
    """
    return spq.to_bool()(source_column, name)


def _generate_select_expression_for_extended_string_to_timestamp(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_timestamp`.
    """
    return spq.to_timestamp()(source_column, name)


def _generate_select_expression_for_extended_string_to_date(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_timestamp` and cast to date instead.
    """
    return spq.to_timestamp(cast="date")(source_column, name)


def _generate_select_expression_for_extended_string_unix_timestamp_ms_to_timestamp(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_timestamp`.
    """
    return spq.to_timestamp()(source_column, name)


def _generate_select_expression_for_extended_string_unix_timestamp_ms_to_date(source_column, name):
    """
    Deprecated!

    Please use :dt.:`~spooq.transformer.mapper_transformations.to_timestamp` and cast to date instead.
    """
    return spq.to_timestamp(cast="date")(source_column, name)
