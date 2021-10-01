"""This is a collection of module level methods to construct a specific
PySpark DataFrame query for custom defined data types.

These methods are not meant to be called directly but via the
the :py:class:`~spooq.transformer.mapper.Mapper` transformer.
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
from pyspark.sql import types as T

from spooq.transformer import mapper_transformations as spq_trans

__all__ = ["add_custom_data_type"]

MIN_TIMESTAMP_MS = 0  # 01.01.1970 00:00:00
MAX_TIMESTAMP_MS = 4102358400000  # 31.12.2099 00:00:00

MIN_TIMESTAMP_SEC = 0
MAX_TIMESTAMP_SEC = old_div(MAX_TIMESTAMP_MS, 1000)


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
        if hasattr(spq_trans, data_type):
            return getattr(spq_trans, data_type)()(source_column, name)

        else:
            current_module = sys.modules[__name__]
            function_name = "_generate_select_expression_for_" + data_type
            return getattr(current_module, function_name)(source_column, name)
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


def _generate_select_expression_for_as_is(source_column, name):
    return spq_trans.as_is()(source_column, name)


def _generate_select_expression_for_keep(source_column, name):
    return spq_trans.as_is()(source_column, name)


def _generate_select_expression_for_no_change(source_column, name):
    return spq_trans.as_is()(source_column, name)


def _generate_select_expression_for_json_string(source_column, name):
    return spq_trans.json_string()(source_column, name)


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
    >>> from spooq.transformer import Mapper
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
    return spq_trans.unix_timestamp_to_unix_timestamp(input_time_unit="ms", output_time_unit="ms")(source_column, name)


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
    >>> from spooq.transformer import Mapper
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
    return spq_trans.unix_timestamp_to_unix_timestamp(input_time_unit="ms", output_time_unit="sec")(source_column, name)



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
    >>> from spooq.transformer import Mapper
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
    return spq_trans.unix_timestamp_to_unix_timestamp(input_time_unit="sec", output_time_unit="ms")(source_column, name)



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
    >>> from spooq.transformer import Mapper
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
    return spq_trans.unix_timestamp_to_unix_timestamp(input_time_unit="sec", output_time_unit="sec")(source_column, name)


def _generate_select_expression_for_StringNull(source_column, name):  # noqa: N802




def _generate_select_expression_for_IntNull(source_column, name):  # noqa: N802




def _generate_select_expression_for_StringBoolean(source_column, name):  # noqa: N802




def _generate_select_expression_for_IntBoolean(source_column, name):  # noqa: N802




def _generate_select_expression_for_TimestampMonth(source_column, name):  # noqa: N802




def _generate_select_expression_for_meters_to_cm(source_column, name):




def _generate_select_expression_for_has_value(source_column, name):




def _generate_select_expression_for_unix_timestamp_ms_to_spark_timestamp(source_column, name):



def _generate_select_expression_for_extended_string_to_int(source_column, name):
    return spq_trans.extended_string_to_number()(source_column, name, output_type=T.IntegerType())


def _generate_select_expression_for_extended_string_to_long(source_column, name):
    """
    More robust conversion from StringType to LongType.
    Is able to additionally handle (compared to implicit Spark conversion):

    * Preceding whitespace
    * Trailing whitespace
    * Preceeding and trailing whitespace
    * underscores as thousand separators

    Hint
    ----
    Please have a look at the tests to get a better feeling how it behaves under
    tests/unit/transformer/test_mapper_custom_data_types.py::TestExtendedStringConversions and
    tests/data/test_fixtures/mapper_custom_data_types_fixtures.py

    Example
    -------
    >>> from spooq.transformer import Mapper
    >>>
    >>> input_df.head(3)
    [Row(input_string="  21474836470 "),
     Row(input_string="Hello"),
     Row(input_string="21_474_836_470")]
    >>> mapping = [("output_value", "input_string", "extended_string_to_long")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=21474836470),
     Row(input_string=None),
     Row(input_string=21474836470)]
    """
    return spq_trans.extended_string_to_number()(source_column, name, output_type=T.LongType())


def _generate_select_expression_for_extended_string_to_float(source_column, name):
    """
    More robust conversion from StringType to FloatType.
    Is able to additionally handle (compared to implicit Spark conversion):

    * Preceding whitespace
    * Trailing whitespace
    * Preceeding and trailing whitespace
    * underscores as thousand separators

    Hint
    ----
    Please have a look at the tests to get a better feeling how it behaves under
    tests/unit/transformer/test_mapper_custom_data_types.py::TestExtendedStringConversions and
    tests/data/test_fixtures/mapper_custom_data_types_fixtures.py

    Example
    -------
    >>> from spooq.transformer import Mapper
    >>>
    >>> input_df.head(3)
    [Row(input_string="  836470.819 "),
     Row(input_string="Hello"),
     Row(input_string="836_470.819")]
    >>> mapping = [("output_value", "input_string", "extended_string_to_float")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=836470.819),
     Row(input_string=None),
     Row(input_string=836470.819)]
    """
    return spq_trans.extended_string_to_number()(source_column, name, output_type=T.FloatType())


def _generate_select_expression_for_extended_string_to_double(source_column, name):
    """
    More robust conversion from StringType to DoubleType.
    Is able to additionally handle (compared to implicit Spark conversion):

    * Preceding whitespace
    * Trailing whitespace
    * Preceeding and trailing whitespace
    * underscores as thousand separators

    Hint
    ----
    Please have a look at the tests to get a better feeling how it behaves under
    tests/unit/transformer/test_mapper_custom_data_types.py::TestExtendedStringConversions and
    tests/data/test_fixtures/mapper_custom_data_types_fixtures.py

    Example
    -------
    >>> from spooq.transformer import Mapper
    >>>
    >>> input_df.head(3)
    [Row(input_string="  21474838464.70 "),
     Row(input_string="Hello"),
     Row(input_string="21_474_838_464.70")]
    >>> mapping = [("output_value", "input_string", "extended_string_to_double")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=21474838464.7),
     Row(input_string=None),
     Row(input_string=21474838464.70)]
    """
    return spq_trans.extended_string_to_number()(source_column, name, output_type=T.DoubleType())


def _generate_select_expression_for_extended_string_to_boolean(source_column, name):


def _generate_select_expression_for_extended_string_to_timestamp(source_column, name):




def _generate_select_expression_for_extended_string_to_date(source_column, name):

    return _generate_select_expression_for_extended_string_to_timestamp(source_column, name).cast(T.DateType())


def _generate_select_expression_for_extended_string_unix_timestamp_ms_to_timestamp(source_column, name):




def _generate_select_expression_for_extended_string_unix_timestamp_ms_to_date(source_column, name):

    return _generate_select_expression_for_extended_string_unix_timestamp_ms_to_timestamp(source_column, name).cast(
        T.DateType()
    )
