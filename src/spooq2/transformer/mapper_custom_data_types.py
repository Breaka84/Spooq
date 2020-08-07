"""This is a collection of module level methods to construct a specific
PySpark DataFrame query for custom defined data types.

These methods are not meant to be called directly but via the
the :py:class:`~spooq2.transformer.mapper.Mapper` transformer.
Please see that particular class on how to apply custom data types.

For injecting your **own custom data types**, please have a visit to the
:py:meth:`add_custom_data_type` method!
"""
from __future__ import division
import sys
if sys.version_info.major > 2:
    # This is needed for python 2 as otherwise pyspark raises an exception for following method:
    # _to_json
    # ToDo: Check if Python3 works without this import / overwrite
    from builtins import str

from past.utils import old_div
import sys
import json
from pyspark.sql import functions as F
from pyspark.sql import types as sql_types

__all__ = ["add_custom_data_type"]

MIN_TIMESTAMP_MS = 0  # 01.01.1970 00:00:00
MAX_TIMESTAMP_MS = 4102358400000  # 31.12.2099 00:00:00

MIN_TIMESTAMP_SEC = 0
MAX_TIMESTAMP_SEC = old_div(MAX_TIMESTAMP_MS, 1000)


def add_custom_data_type(function_name, func):
    """
    Registers a custom data type in runtime to be used with the :py:class:`~spooq2.transformer.mapper.Mapper` transformer.

    Example
    -------
    >>> import spooq2.transformer.mapper_custom_data_types as custom_types
    >>> import spooq2.transformer as T
    >>> from pyspark.sql import Row, functions as F, types as sql_types

    >>> def hello_world(source_column, name):
    >>>     "A UDF (User Defined Function) in Python"
    >>>     def _to_hello_world(col):
    >>>         if not col:
    >>>             return None
    >>>         else:
    >>>             return "Hello World"
    >>>
    >>>     udf_hello_world = F.udf(_to_hello_world, sql_types.StringType())
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
    current_module = sys.modules[__name__]
    function_name = "_generate_select_expression_for_" + data_type
    try:
        function = getattr(current_module, function_name)
    except AttributeError as e:
        raise AttributeError(
            "Spooq could not find a Select Expression Generator Method \n"
            + "for the custom DataType: "
            + '"'
            + data_type
            + '"'
            + "\n"
            + "Original Error Message: "
            + str(e)
        )

    return function(source_column, name)


def _generate_select_expression_for_as_is(source_column, name):
    """ alias for `_generate_select_expression_without_casting` """
    return _generate_select_expression_without_casting(source_column, name)


def _generate_select_expression_for_keep(source_column, name):
    """ alias for `_generate_select_expression_without_casting` """
    return _generate_select_expression_without_casting(source_column, name)


def _generate_select_expression_for_no_change(source_column, name):
    """ alias for `_generate_select_expression_without_casting` """
    return _generate_select_expression_without_casting(source_column, name)


def _generate_select_expression_without_casting(source_column, name):
    """
    Returns a column without casting. This is especially useful if you need to
    keep a complex data type, like an array, list or a struct.

    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df.head(3)
    [Row(friends=[Row(first_name=None, id=3993, last_name=None), Row(first_name=u'Ru\xf2', id=17484, last_name=u'Trank')]),
     Row(friends=[]),
     Row(friends=[Row(first_name=u'Daphn\xe9e', id=16707, last_name=u'Lyddiard'), Row(first_name=u'Ad\xe9la\xefde', id=17429, last_name=u'Wisdom')])]
    >>> mapping = [("my_friends", "friends", "as_is")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(my_friends=[Row(first_name=None, id=3993, last_name=None), Row(first_name=u'Ru\xf2', id=17484, last_name=u'Trank')]),
     Row(my_friends=[]),
     Row(my_friends=[Row(first_name=u'Daphn\xe9e', id=16707, last_name=u'Lyddiard'), Row(first_name=u'Ad\xe9la\xefde', id=17429, last_name=u'Wisdom')])]


    """
    return source_column.alias(name)


def _generate_select_expression_for_json_string(source_column, name):
    """
    Returns a column as json compatible string.
    Nested hierarchies are supported.
    The unicode representation of a column will be returned if an error occurs.

    Example
    -------
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df.head(3)
    [Row(friends=[Row(first_name=None, id=3993, last_name=None), Row(first_name=u'Ru\xf2', id=17484, last_name=u'Trank')]),
     Row(friends=[]),
     Row(friends=[Row(first_name=u'Daphn\xe9e', id=16707, last_name=u'Lyddiard'), Row(first_name=u'Ad\xe9la\xefde', id=17429, last_name=u'Wisdom')])]    >>> mapping = [("friends_json", "friends", "json_string")]
    >>> mapping = [("friends_json", "friends", "json_string")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(friends_json=u'[{"first_name": null, "last_name": null, "id": 3993}, {"first_name": "Ru\\u00f2", "last_name": "Trank", "id": 17484}]'),
     Row(friends_json=None),
     Row(friends_json=u'[{"first_name": "Daphn\\u00e9e", "last_name": "Lyddiard", "id": 16707}, {"first_name": "Ad\\u00e9la\\u00efde", "last_name": "Wisdom", "id": 17429}]')]
    """

    def _to_json(col):
        if not col:
            return None
        try:
            if isinstance(col, list):
                return json.dumps([x.asDict(recursive=True) for x in col])
            else:
                return json.dumps(col.asDict(recursive=True))
        except (AttributeError, TypeError):
            return str(col)

    udf_to_json = F.udf(_to_json, sql_types.StringType())
    return udf_to_json(source_column).alias(name)


def _generate_select_expression_for_timestamp_ms_to_ms(source_column, name):
    """
    This Constructor is used for unix timestamps. The values are cleaned
    next to casting and renaming.
    If the values are not between `01.01.1970` and `31.12.2099`,
    NULL will be returned.
    Cast to :any:`pyspark.sql.types.LongType`

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame([
    >>>     Row(time_sec=1581540839000),  # 02/12/2020 @ 8:53pm (UTC)
    >>>     Row(time_sec=-4887839000),    # Invalid!
    >>>     Row(time_sec=4737139200000)   # 02/12/2120 @ 12:00am (UTC)
    >>> ])
    >>>
    >>> mapping = [("unix_ts", "time_sec", "timestamp_ms_to_ms")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(unix_ts=1581540839000), Row(unix_ts=None), Row(unix_ts=None)]

    Note
    ----
    *input*  in **milli seconds**
    *output* in **milli seconds**
    """

    return (
        F.when(source_column.between(MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS), source_column)
        .otherwise(F.lit(None))
        .cast(sql_types.LongType())
        .alias(name)
    )


def _generate_select_expression_for_timestamp_ms_to_s(source_column, name):
    """
    This Constructor is used for unix timestamps. The values are cleaned
    next to casting and renaming.
    If the values are not between `01.01.1970` and `31.12.2099`,
    NULL will be returned.
    Cast to :any:`pyspark.sql.types.LongType`

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame([
    >>>     Row(time_sec=1581540839000),  # 02/12/2020 @ 8:53pm (UTC)
    >>>     Row(time_sec=-4887839000),    # Invalid!
    >>>     Row(time_sec=4737139200000)   # 02/12/2120 @ 12:00am (UTC)
    >>> ])
    >>>
    >>> mapping = [("unix_ts", "time_sec", "timestamp_ms_to_s")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(unix_ts=1581540839), Row(unix_ts=None), Row(unix_ts=None)]

    Note
    ----
    *input*  in **milli seconds**
    *output* in **seconds**
    """

    return (
        F.when(
            source_column.between(MIN_TIMESTAMP_MS, MAX_TIMESTAMP_MS),
            source_column / 1000,
        )
        .otherwise(F.lit(None))
        .cast(sql_types.LongType())
        .alias(name)
    )


def _generate_select_expression_for_timestamp_s_to_ms(source_column, name):
    """
    This Constructor is used for unix timestamps. The values are cleaned
    next to casting and renaming.
    If the values are not between `01.01.1970` and `31.12.2099`,
    NULL will be returned.
    Cast to :any:`pyspark.sql.types.LongType`

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame([
    >>>     Row(time_sec=1581540839),  # 02/12/2020 @ 8:53pm (UTC)
    >>>     Row(time_sec=-4887839),    # Invalid!
    >>>     Row(time_sec=4737139200)   # 02/12/2120 @ 12:00am (UTC)
    >>> ])
    >>>
    >>> mapping = [("unix_ts", "time_sec", "timestamp_s_to_ms")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(unix_ts=1581540839000), Row(unix_ts=None), Row(unix_ts=None)]

    Note
    ----
    *input*  in **seconds**
    *output* in **milli seconds**
    """

    return (
        F.when(
            source_column.between(MIN_TIMESTAMP_SEC, MAX_TIMESTAMP_SEC),
            source_column * 1000,
        )
        .otherwise(F.lit(None))
        .cast(sql_types.LongType())
        .alias(name)
    )


def _generate_select_expression_for_timestamp_s_to_s(source_column, name):
    """
    This Constructor is used for unix timestamps. The values are cleaned
    next to casting and renaming.
    If the values are not between `01.01.1970` and `31.12.2099`,
    NULL will be returned.
    Cast to :any:`pyspark.sql.types.LongType`

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame([
    >>>     Row(time_sec=1581540839),  # 02/12/2020 @ 8:53pm (UTC)
    >>>     Row(time_sec=-4887839),    # Invalid!
    >>>     Row(time_sec=4737139200)   # 02/12/2120 @ 12:00am (UTC)
    >>> ])
    >>>
    >>> mapping = [("unix_ts", "time_sec", "timestamp_s_to_ms")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(unix_ts=1581540839), Row(unix_ts=None), Row(unix_ts=None)]

    Note
    ----
    *input*  in **seconds**
    *output* in **seconds**
    """
    return (
        F.when(source_column.between(MIN_TIMESTAMP_SEC, MAX_TIMESTAMP_SEC), source_column)
        .otherwise(F.lit(None))
        .cast(sql_types.LongType())
        .alias(name)
    )


def _generate_select_expression_for_StringNull(source_column, name):  # noqa: N802
    """
    Used for Anonymizing.
    Input values will be ignored and replaced by NULL,
    Cast to :any:`pyspark.sql.types.StringType`

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame(
    >>>     [Row(email=u'tsivorn1@who.int'),
    >>>      Row(email=u''),
    >>>      Row(email=u'gisaksen4@skype.com')]
    >>> )
    >>>
    >>> mapping = [("email", "email", "StringNull")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(email=None), Row(email=None), Row(email=None)]
    """

    return F.lit(None).cast(sql_types.StringType()).alias(name)


def _generate_select_expression_for_IntNull(source_column, name):  # noqa: N802
    """
    Used for Anonymizing.
    Input values will be ignored and replaced by NULL,
    Cast to :any:`pyspark.sql.types.IntegerType`

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame(
    >>>     [Row(facebook_id=3047288),
    >>>      Row(facebook_id=0),
    >>>      Row(facebook_id=57815)]
    >>> )
    >>>
    >>> mapping = [("facebook_id", "facebook_id", "IntNull")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(facebook_id=None), Row(facebook_id=None), Row(facebook_id=None)]
    """

    return F.lit(None).cast(sql_types.IntegerType()).alias(name)


def _generate_select_expression_for_StringBoolean(source_column, name):  # noqa: N802
    """
    Used for Anonymizing.
    The column's value will be replaced by `"1"` if it is:

        * not NULL and
        * not an empty string

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
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
        .cast(sql_types.StringType())
        .alias(name)
    )


def _generate_select_expression_for_IntBoolean(source_column, name):  # noqa: N802
    """
    Used for Anonymizing.
    The column's value will be replaced by `1` if it contains a non-NULL value.

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
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
    `0` (zero) or negative numbers are still considered as valid values and therefore converted to `1`.
    """
    return (
        F.when(source_column.isNull(), F.lit(None))
        .otherwise(1)
        .cast(sql_types.IntegerType())
        .alias(name)
    )


def _generate_select_expression_for_TimestampMonth(source_column, name):  # noqa: N802
    """
    Used for Anonymizing. Can be used to keep the age but obscure the explicit birthday.
    This custom datatype requires a :any:`pyspark.sql.types.TimestampType` column as input.
    The datetime value will be set to the first day of the month.

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from datetime import datetime
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame(
    >>>     [Row(birthday=datetime(2019, 2, 9, 2, 45)),
    >>>      Row(birthday=None),
    >>>      Row(birthday=datetime(1988, 1, 31, 8))]
    >>> )
    >>>
    >>> mapping = [("birthday", "birthday", "TimestampMonth")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(birthday=datetime.datetime(2019, 2, 1, 0, 0)),
     Row(birthday=None),
     Row(birthday=datetime.datetime(1988, 1, 1, 0, 0))]
    """
    return F.trunc(source_column, "month").cast(sql_types.TimestampType()).alias(name)


def _generate_select_expression_for_meters_to_cm(source_column, name):
    """
    Convert meters to cm and cast the result to an IntegerType.

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame(
    >>>     [Row(size_in_m=1.80),
    >>>      Row(size_in_m=1.65),
    >>>      Row(size_in_m=2.05)]
    >>> )
    >>>
    >>> mapping = [("size_in_cm", "size_in_m", "meters_to_cm")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(size_in_cm=180),
     Row(size_in_cm=165),
     Row(size_in_cm=205)]
    """
    return (source_column * 100).cast(sql_types.IntegerType()).alias(name)


def _generate_select_expression_for_unix_timestamp_ms_to_spark_timestamp(source_column, name):
    """
    Convert unix timestamps in milliseconds to a Spark TimeStampType. It is assumed that the
    timezone is already set to UTC in spark / java to avoid implicit timezone conversions.

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq2.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame(
    >>>     [Row(unix_timestamp_in_ms=1591627696951),
    >>>      Row(unix_timestamp_in_ms=1596812952000),
    >>>      Row(unix_timestamp_in_ms=946672200000)]
    >>> )
    >>>
    >>> mapping = [("spark_timestamp", "unix_timestamp_in_ms", "unix_timestamp_ms_to_spark_timestamp")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(spark_timestamp=datetime.datetime(2020, 6, 8, 16, 48, 16, 951000)),
     Row(spark_timestamp=datetime.datetime(2020, 8, 7, 17, 9, 12)),
     Row(spark_timestamp=datetime.datetime(1999, 12, 31, 21, 30))]
    """
    return (source_column / 1000).cast(sql_types.TimestampType()).alias(name)
