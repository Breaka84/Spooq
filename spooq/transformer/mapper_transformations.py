"""This is a collection of module level methods to construct a specific
PySpark DataFrame query for custom defined data types.

These methods are not meant to be called directly but via the
the :py:class:`~spooq.transformer.mapper.Mapper` transformer.
Please see that particular class on how to apply custom data types.

For injecting your **own custom data types**, please have a visit to the
:py:meth:`add_custom_data_type` method!
"""
from functools import partial
from typing import Any
import json
import spooq.transformer.mapper_custom_data_types as custom_string_transformations
from pyspark.sql import functions as F, types as T


def as_is(**kwargs: Any) -> partial:
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
    def _inner_func(source_column, name):
        return source_column.alias(name)

    return partial(_inner_func)


def json_string(**kwargs: Any) -> partial:
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
    def _inner_func(source_column, name):
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

        udf_to_json = F.udf(_to_json, T.StringType())
        return udf_to_json(source_column).alias(name)

    return partial(_inner_func)


def unix_timestamp_to_unix_timestamp(**kwargs: Any) -> partial:
    """
    ToDo: Make proper Docstring
    """
    def _inner_func(source_column, name, input_time_unit, output_time_unit):
        if input_time_unit == "ms":
            source_column = source_column / 1_000

        output_column = source_column.cast(T.LongType())

        if output_time_unit == "ms":
            output_column = output_column * 1_000

        return output_column.alias(name)

    args = dict(
        input_time_unit=kwargs.get("input_time_unit", "ms"),
        output_time_unit=kwargs.get("output_time_unit", "sec"),
    )

    return partial(_inner_func, **args)


def StringNull(**kwargs: Any) -> partial:
    """
    Used for Anonymizing.
    Input values will be ignored and replaced by NULL,
    Cast to :any:`pyspark.sql.types.StringType`

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
    >>> mapping = [("email", "email", "StringNull")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(email=None), Row(email=None), Row(email=None)]
    """

    def _inner_func(source_column, name):
        return F.lit(None).cast(T.StringType()).alias(name)

    return partial(_inner_func)


def IntNull(**kwargs: Any) -> partial:
    """
    Used for Anonymizing.
    Input values will be ignored and replaced by NULL,
    Cast to :any:`pyspark.sql.types.IntegerType`

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq.transformer import Mapper
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

    def _inner_func(source_column, name):
        return F.lit(None).cast(T.IntegerType()).alias(name)

    return partial(_inner_func)


def StringBoolean(**kwargs: Any) -> partial:
    """
    Used for Anonymizing.
    The column's value will be replaced by `"1"` if it is:

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
    def _inner_func(source_column, name):
        return (
            F.when(source_column.isNull(), F.lit(None))
                .when(source_column == "", F.lit(None))
                .otherwise("1")
                .cast(T.StringType())
                .alias(name)
        )

    return partial(_inner_func)


def IntBoolean(**kwargs: Any) -> partial:
    """
    Used for Anonymizing.
    The column's value will be replaced by `1` if it contains a non-NULL value.

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
    `0` (zero) or negative numbers are still considered as valid values and therefore converted to `1`.
    """
    def _inner_func(source_column, name):
        return F.when(source_column.isNull(), F.lit(None)).otherwise(1).cast(T.IntegerType()).alias(name)

    return partial(_inner_func)


def TimestampMonth(**kwargs: Any) -> partial:
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
    def _inner_func(source_column, name):
        return F.trunc(source_column, "month").cast(T.TimestampType()).alias(name)

    return partial(_inner_func)


def meters_to_cm(**kwargs: Any) -> partial:
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
    def _inner_func(source_column, name):
        return (source_column * 100).cast(T.IntegerType()).alias(name)

    return partial(_inner_func)


def has_value(**kwargs: Any) -> partial:
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
    def _inner_func(source_column, name):
        return (
            F.when((source_column.isNotNull()) & (source_column.cast(T.StringType()) != ""), F.lit(True))
                .otherwise(F.lit(False))
                .alias(name)
        )

    return partial(_inner_func)


def unix_timestamp_ms_to_spark_timestamp(**kwargs: Any) -> partial:
    """
    Convert unix timestamps in milliseconds to a Spark TimeStampType. It is assumed that the
    timezone is already set to UTC in spark / java to avoid implicit timezone conversions.

    Example
    -------
    >>> from pyspark.sql import Row
    >>> from spooq.transformer import Mapper
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
    def _inner_func(source_column, name):
        return (source_column / 1000).cast(T.TimestampType()).alias(name)

    return partial(_inner_func)


def extended_string_to_number(**kwargs: Any) -> partial:
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
    >>> mapping = [("output_value", "input_string", spq_trans.extended_string_to_number(output_type=T.IntegerType()))]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=123456),
     Row(input_string=None),
     Row(input_string=123456)]
    """
    def _inner_func(source_column, name, output_type):
        return F.regexp_replace(F.trim(source_column), "_", "").cast(output_type).alias(name)

    args = dict(
        output_type=kwargs.get("output_type", T.LongType()),
    )

    return partial(_inner_func, **args)


def extended_string_to_boolean(**kwargs: Any) -> partial:
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
    * "100" to extended_string_to_boolean => False
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
    >>> mapping = [("output_value", "input_string", "extended_string_to_boolean")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=True),
     Row(input_string=False),
     Row(input_string=True)]
    """
    def _inner_func(source_column, name, true_values, false_values):
        return (
            F.when(F.trim(source_column).isin(true_values), F.lit(True))
                .when(F.trim(source_column).isin(false_values), F.lit(False))
                .otherwise(F.trim(source_column).cast(T.BooleanType()))
        ).alias(name)

    args = dict(
        true_values=kwargs.get("true_values", ["on", "enabled"]),
        false_values=kwargs.get("false_values", ["off", "disabled"]),
    )

    return partial(_inner_func, **args)


def extended_string_to_timestamp(**kwargs: Any) -> partial:
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
    >>> mapping = [("output_value", "input_string", "extended_string_to_timestamp")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=datetime.datetime(2020, 8, 12, 12, 43, 14)),
     Row(input_string=datetime.datetime(2020, 8, 10, 14, 24, 6)),
     Row(input_string=datetime.datetime(2020, 8, 12, 0, 0, 0))]
    """
    def _inner_func(source_column, name, max_timestamp_sec,):
        return (
            F.when(
                F.abs(F.trim(source_column).cast(T.LongType())).between(0, max_timestamp_sec),
                F.trim(source_column).cast(T.LongType()).cast(T.TimestampType()),
            ).when(
                F.abs(F.trim(source_column).cast(T.LongType())) > max_timestamp_sec,
                (F.trim(source_column) / 1000).cast(T.TimestampType()),
                ).otherwise(
                F.trim(source_column).cast(T.TimestampType())
            ).alias(name)
        )

    args = dict(
        max_timestamp_sec=kwargs.get("max_timestamp_sec", 4102358400),  # 2099-12-31 01:00:00
    )

    return partial(_inner_func, **args)


def extended_string_to_date(**kwargs: Any) -> partial:
    """
    More robust conversion from StringType to DateType. It is assumed that the
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
    >>> mapping = [("output_value", "input_string", "extended_string_to_date")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=datetime.datetime(2020, 8, 12)),
     Row(input_string=datetime.datetime(2020, 8, 10)),
     Row(input_string=datetime.datetime(2020, 8, 12))]
    """

    return extended_string_to_timestamp(**kwargs).cast(T.DataType())


def extended_string_unix_timestamp_ms_to_timestamp(**kwargs: Any) -> partial:
    """
    More robust conversion from StringType to TimestampType. It is assumed that the
    timezone is already set to UTC in spark / java to avoid implicit timezone conversions.
    Is able to additionally handle (compared to implicit Spark conversion):

    * Unix timestamps in milliseconds
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
     Row(input_string="1597069446000"),
     Row(input_string="2020-08-12")]
    >>> mapping = [("output_value", "input_string", "extended_string_to_timestamp")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=datetime.datetime(2020, 8, 12, 12, 43, 14)),
     Row(input_string=datetime.datetime(2020, 8, 10, 14, 24, 6)),
     Row(input_string=datetime.datetime(2020, 8, 12, 0, 0, 0))]
    """
    return extended_string_to_timestamp(**kwargs)


def extended_string_unix_timestamp_ms_to_date(**kwargs: Any) -> partial:
    """
    More robust conversion from StringType to DateType. It is assumed that the
    timezone is already set to UTC in spark / java to avoid implicit timezone conversions and that
    unix timestamps are in **milli seconds**

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
     Row(input_string="1597069446000"),
     Row(input_string="2020-08-12")]
    >>> mapping = [("output_value", "input_string", "extended_string_to_date")]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.head(3)
    [Row(input_string=datetime.datetime(2020, 8, 12)),
     Row(input_string=datetime.datetime(2020, 8, 10)),
     Row(input_string=datetime.datetime(2020, 8, 12))]
    """
    return extended_string_to_timestamp(**kwargs).cast(T.DateType())

