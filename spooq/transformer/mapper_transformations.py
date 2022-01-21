"""
TODO: make output_type optional for all methods
This is a collection of module level methods to construct a specific
PySpark DataFrame query for custom defined data types.

These methods are not meant to be called directly but via the
the :py:class:`~spooq.transformer.mapper.Mapper` transformer.
Please see that particular class on how to apply custom data types.

For injecting your **own custom data types**, please have a visit to the
:py:meth:`add_custom_data_type` method!
"""
import re
from functools import partial
from typing import Any, Union
import json

import IPython
from pyspark.sql import functions as F, types as T
from pyspark.sql.column import Column


COLUMN_NAME_PATTERN = re.compile(".*\'(.*)\'")


def _coalesce_source_columns(source_column, alt_src_cols):
    if isinstance(alt_src_cols, str):
        alt_src_cols = [alt_src_cols]
    return F.coalesce(source_column, *[F.col(col) for col in alt_src_cols])


def _get_executable_function(inner_func, source_column, name, **kwargs):
    direct_call = True

    if isinstance(source_column, str):
        name = name or source_column.split(".")[-1]
        source_column = F.col(source_column)
    elif isinstance(source_column, Column):
        name = name or re.search(COLUMN_NAME_PATTERN, source_column.name()).group(1)
    else:
        direct_call = False

    if direct_call:
        return inner_func(source_column, name, **kwargs)
    else:
        return partial(inner_func, **kwargs)


def as_is(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Returns a column without casting. This is especially useful if you need to
    keep a complex data type, like an array, list or a struct.

    >>> from spooq.transformer import Mapper
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

    def _inner_func(source_column, name, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)
        if output_type:
            source_column = source_column.cast(output_type)
        return source_column.alias(name)

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", False),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def to_json_string(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Returns a column as json compatible string.
    Nested hierarchies are supported.
    The unicode representation of a column will be returned if an error occurs.

    Example
    -------
    >>> from spooq.transformer import Mapper
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

    def _inner_func(source_column, name, output_type):
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

        udf_to_json = F.udf(_to_json, output_type)
        return udf_to_json(source_column).alias(name)

    args = dict(
        output_type=kwargs.get("output_type", T.StringType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def unix_timestamp_to_unix_timestamp(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Converts a unix timestamp (number) between milli seconds and seconds
    and casts it to a :any:`pyspark.sql.types.LongType`.

    Parameters
    ----------
        input_time_unit (str) :  Defines the time unit of the source value. Either "ms" or "sec"
        output_time_unit (str) :  Defines the time unit of the target value. Either "ms" or "sec"

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq.transformer import Mapper
    >>> from spooq.transformer import mapper_transformations as spq_trans
    >>>
    >>> input_df = spark.createDataFrame([
    >>>     Row(time_sec=1581540839000),  # 2020-02-12 21:53:59
    >>>     Row(time_sec=-4887839000),    # 1969-11-05 11:16:01
    >>>     Row(time_sec=4737139200000)   # 2120-02-12 01:00:00
    >>> ])
    >>>
    >>> mapping = [
    >>>     ("unix_ts", "time_sec", spq_trans.unix_timestamp_to_unix_timestamp(input_time_unit="ms",
    >>>                                                                        output_time_unit="sec")
    >>>     ),
    >>> ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(unix_ts=1581540839), Row(unix_ts=-4887839000), Row(unix_ts=4737139200)]
    """

    def _inner_func(source_column, name, input_time_unit, output_time_unit, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)
        if input_time_unit == "ms":
            source_column = source_column / 1_000.0

        output_column = source_column.cast(T.DoubleType())

        if output_time_unit == "ms":
            output_column = output_column * 1_000.0

        return output_column.cast(output_type).alias(name)

    args = dict(
        input_time_unit=kwargs.get("input_time_unit", "ms"),
        output_time_unit=kwargs.get("output_time_unit", "sec"),
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.LongType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def spark_timestamp_to_first_of_month(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Used for Anonymizing. Can be used to keep the age but obscure the explicit birthday.
    This custom datatype requires a :any:`pyspark.sql.types.TimestampType` column as input.
    The datetime value will be set to the first day of the month.

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from datetime import datetime
    >>> from spooq.transformer import Mapper
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

    def _inner_func(source_column, name, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)
        return F.trunc(source_column, "month").cast(output_type).alias(name)

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.DateType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def meters_to_cm(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Convert meters to cm and cast the result to an IntegerType.

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq.transformer import Mapper
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

    def _inner_func(source_column, name, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)
        return (source_column * 100).cast(output_type).alias(name)

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.IntegerType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def has_value(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Returns True if the source_column is
        - not NULL and
        - not "" (empty string)

    otherwise it returns False

    Warning
    -------
    This means that it will return True for values which would indicate a False value. Like "false" or 0!!!

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq.transformer import Mapper
    >>>
    >>> input_df = spark.createDataFrame(
    >>>     [Row(input_key=1.80),
    >>>      Row(input_key=None),
    >>>      Row(input_key="some text"),
    >>>      Row(input_key="")]
    >>> )
    >>>
    >>> mapping = [("input_key", "result", "has_value")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(4)
    [Row(result=True),
     Row(result=False),
     Row(result=True),
     Row(result=False)]

    """

    def _inner_func(source_column, name, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)
        return (
            F.when((source_column.isNotNull()) & (source_column.cast(T.StringType()) != ""), F.lit(True))
            .otherwise(F.lit(False))
            .cast(output_type)
            .alias(name)
        )

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.BooleanType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def str_to_num(source_column=None, name=None, **kwargs: Any) -> Union[partial, Column]:
    """
    Todo: update docstring
    More robust conversion from StringType to IntegerType.
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
    >>> from pyspark.sql import types as T
    >>> from spooq.transformer import Mapper
    >>> from spooq.transformer import mapper_transformations as spq_trans
    >>>
    >>> input_df.head(3)
    [Row(input_string="  123456 "),
     Row(input_string="Hello"),
     Row(input_string="123_456")]
    >>> mapping = [("output_value", "input_string", spq_trans.str_to_num(output_type=T.IntegerType()))]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=123456),
     Row(input_string=None),
     Row(input_string=123456)]
    """

    def _inner_func(source_column, name, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)
        return F.regexp_replace(F.trim(source_column), "_", "").cast(output_type).alias(name)

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.LongType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def str_to_bool(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    More robust conversion from StringType to BooleanType.
    Is able to additionally handle (compared to implicit Spark conversion):

    * Preceding whitespace
    * Trailing whitespace
    * Preceeding and trailing whitespace

    Warning
    ---------
    This does not handle numbers (cast as string) the same way as numbers (cast as number) to boolean conversion!
    F.e.

    * 100 to boolean => True
    * "100" to str_to_bool => False
    * "100" to boolean => False

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
    [Row(input_string="  true "),
     Row(input_string="0"),
     Row(input_string="y")]
    >>> mapping = [("output_value", "input_string", "str_to_bool")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=True),
     Row(input_string=False),
     Row(input_string=True)]
    """

    def _inner_func(source_column, name, true_values, false_values, case_sensitive, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)
        if case_sensitive:
            true_condition = F.trim(source_column).isin(true_values)
            false_condition = F.trim(source_column).isin(false_values)
        else:
            true_values = [str(val).lower() for val in true_values]
            false_values = [str(val).lower() for val in false_values]
            true_condition = F.lower(F.trim(source_column)).isin(true_values)
            false_condition = F.lower(F.trim(source_column)).isin(false_values)

        return (
            (
                F.when(true_condition, F.lit(True))
                .when(false_condition, F.lit(False))
                .otherwise(F.trim(source_column).cast(T.BooleanType()))
            )
            .cast(output_type)
            .alias(name)
        )

    args = dict(
        case_sensitive=kwargs.get("case_sensitive", False),
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.BooleanType()),
    )

    if kwargs.get("replace_default_values"):
        args["true_values"] = kwargs.get("true_values", ["on", "enabled"])
        args["false_values"] = kwargs.get("false_values", ["off", "disabled"])
    else:
        args["true_values"] = kwargs.get("true_values", []) + ["on", "enabled"]
        args["false_values"] = kwargs.get("false_values", []) + ["off", "disabled"]

    return _get_executable_function(_inner_func, source_column, name, **args)


def str_to_timestamp(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    More robust conversion from StringType to TimestampType. It is assumed that the
    timezone is already set to UTC in spark / java to avoid implicit timezone conversions.

    The conversion can handle unix timestamps in seconds and in milliseconds:
        - Timestamps in the range [-MAX_TIMESTAMP_S, MAX_TIMESTAMP_S] are treated as seconds
        - Timestamps in the range [-inf, -MAX_TIMESTAMP_S) and (MAX_TIMESTAMP_S, inf] are treated as milliseconds
        - There is a time interval (1970-01-01 +- ~2.5 months)where we can not distinguish correctly between s and ms
          (e.g. 3974400000 would be treated as seconds (2095-12-11T00:00:00) as the value is smaller than
          MAX_TIMESTAMP_S, but it could also be a valid date in Milliseconds (1970-02-16T00:00:00)

    Is able to additionally handle (compared to implicit Spark conversion):
    * Preceding whitespace
    * Trailing whitespace
    * Preceeding and trailing whitespace

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
    [Row(input_string="2020-08-12T12:43:14+0000"),
     Row(input_string="1597069446"),
     Row(input_string="2020-08-12")]
    >>> mapping = [("output_value", "input_string", "str_to_timestamp")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=datetime.datetime(2020, 8, 12, 12, 43, 14)),
     Row(input_string=datetime.datetime(2020, 8, 10, 14, 24, 6)),
     Row(input_string=datetime.datetime(2020, 8, 12, 0, 0, 0))]
    """

    def _inner_func(source_column, name, max_timestamp_sec, input_format, output_format, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)

        if input_format:
            output_col = (
                F.unix_timestamp(
                    source_column.cast(T.StringType()),
                    input_format
                ).cast(
                    T.TimestampType()
                ).cast(
                    output_type
                ).alias(
                    name
                )
            )
        else:
            output_col = (
                F.when(
                    F.abs(F.trim(source_column).cast(T.LongType())).between(0, max_timestamp_sec),
                    F.trim(source_column).cast(T.LongType()).cast(T.TimestampType()),
                )
                .when(
                    F.abs(F.trim(source_column).cast(T.LongType())) > max_timestamp_sec,
                    (F.trim(source_column) / 1000).cast(T.TimestampType()),
                )
                .otherwise(F.trim(source_column))
                .cast(output_type)
            )
        if output_format:
            output_col = F.date_format(output_col, output_format)
        return output_col.alias(name)

    args = dict(
        max_timestamp_sec=kwargs.get("max_timestamp_sec", 4102358400),  # 2099-12-31 01:00:00
        input_format=kwargs.get("input_format", False),
        output_format=kwargs.get("output_format", False),
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.TimestampType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def str_to_array(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Converts a string containing an array of integers to an array of integers
    If conversion is not possible, the value will be set to null
    Example: "[1,2,3,item1]" --> [1,2,3,null]
    """

    def _inner_func(source_column, name, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)

        return (
            F.split(F.regexp_replace(source_column, r"^\s*\[*\s*|\s*\]*\s*$", ""), r"\s*,\s*")
            .cast(T.ArrayType(output_type))
            .alias(name)
        )

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.StringType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def map_values(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Map input values to specified output values

    Examples
    --------
    map = {
        "runtastic": "running",
        "results":   "training",
    }
    ==>
    F.when(batch_df.app_branch == "runtastic", "running")
        .when(batch_df.app_branch == "results", "training")
        .otherwise(batch_df.app_branch)

    """

    def _inner_func(source_column, name, mapping, default, case_sensitive, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)

        if isinstance(default, str) and default == "source_column":
            default = source_column
        if isinstance(default, str):
            default = F.lit(default)

        keys = list(mapping.keys())
        if not case_sensitive:
            when_clause = F.when(F.lower(source_column) == str(keys[0]).lower(), mapping[keys[0]])
            for key in keys[1:]:
                when_clause = when_clause.when(F.lower(source_column) == str(key).lower(), mapping[key])
        else:
            when_clause = F.when(source_column.cast(T.StringType()) == keys[0], mapping[keys[0]])
            for key in keys[1:]:
                when_clause = when_clause.when(source_column.cast(T.StringType()) == key, mapping[key])

        when_clause = when_clause.otherwise(default.cast(T.StringType()))

        return when_clause.cast(output_type).alias(name)

    try:
        mapping = kwargs["mapping"]
    except TypeError:
        raise TypeError("'map_values' is missing the mapping dict (f.e. mapping=dict(results='training'))")
    if len(mapping.keys()) < 1:
        raise ValueError("'map_values' received an empty map (f.e. mapping=dict(results='training'))")

    args = dict(
        mapping=mapping,
        default=kwargs.get("default", "source_column"),
        case_sensitive=kwargs.get("case_sensitive", False),
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.StringType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def apply_func(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Applies a custom function
    """

    def _inner_func(source_column, name, func, alt_src_cols, output_type):
        if alt_src_cols:
            source_column = _coalesce_source_columns(source_column, alt_src_cols)
        return func(source_column).cast(output_type).alias(name)

    try:
        func = kwargs["func"]
    except TypeError:
        raise TypeError("'apply_func' transformation is missing the custom function (f.e. func=F.lower)")

    args = dict(
        func=func,
        alt_src_cols=kwargs.get("alt_src_cols", False),
        output_type=kwargs.get("output_type", T.StringType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)