"""
This is a collection of module level functions to be applied to a DataFrame.
These methods can be used with the :py:class:`~spooq.transformer.mapper.Mapper` transformer
or directly within a ``select`` or ``withColumn`` statement.

All functions support following generic functionalities:
    alt_src_cols: Alternative source columns that will be used within a coalesce function if provided
    cast: Explicit casting after the transformation (sane defaults are set for each function)

``to_str`` is the exception with a hardcoded cast that cannot be changed

All examples assume following code has been executed before:

>>> from pyspark.sql import Row
>>> from pyspark.sql import functions as F, types as T
>>> from spooq.transformer import Mapper
>>> from spooq.transformer import mapper_transformations as spq
"""
import re
from functools import partial
from typing import Any, Union, List, Callable
import json

from pyspark.sql import functions as F, types as T
from pyspark.sql.column import Column


COLUMN_NAME_PATTERN = re.compile(r".*\'(.*)\'")


def _coalesce_source_columns(source_column, alt_src_cols):
    if alt_src_cols is None or alt_src_cols is False:
        return source_column
    if not isinstance(alt_src_cols, (list, tuple, set)):
        return F.coalesce(source_column, alt_src_cols)
    return F.coalesce(source_column, *alt_src_cols)


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


def as_is(source_column: Union[str, Column] = None, name: str = None, **kwargs) -> Union[partial, Column]:
    """
    Returns a renamed column without any casting. This is especially useful if you need to
    keep a complex data type (f.e. array, list or struct).

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#as-is

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> no casting, same return data type as input data type
        Applies provided datatype on output column (``.cast(cast)``)

    Examples
    --------
    >>> input_df = spark.createDataFrame([
    ...     Row(friends=[Row(first_name="Gianni", id=3993, last_name="Weber"),
    ...                  Row(first_name="Arielle", id=17484, last_name="Greaves")]),
    ... ])
    >>> mapping = [("my_friends", "friends", spq.as_is)]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +--------------------------------------------------+
    |my_friends                                        |
    +--------------------------------------------------+
    |[[Gianni, 3993, Weber], [Arielle, 17484, Greaves]]|
    +--------------------------------------------------+
    >>> input_df.select(spq.as_is("friends.first_name")).show(truncate=False)
    +-----------------+
    |first_name       |
    +-----------------+
    |[Gianni, Arielle]|
    +-----------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, alt_src_cols, cast):
        source_column = _coalesce_source_columns(source_column, alt_src_cols)
        if cast:
            source_column = source_column.cast(cast)
        return source_column.alias(name)

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", False),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def to_num(source_column=None, name=None, **kwargs: Any) -> Union[partial, Column]:
    """
    More robust conversion from StringType to number data types (Default: LongType).
    This method is able to additionally handle (compared to implicit Spark conversion):

        * Preceding and/or trailing whitespace
        * underscores as thousand separators

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#to_num

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> T.LongType()
        Applies provided datatype on output column (``.cast(cast)``)

    Examples
    --------
    >>> input_df = spark.createDataFrame(
    ...     [
    ...         Row(input_string="  123456 "),
    ...         Row(input_string="Hello"),
    ...         Row(input_string="123_456")
    ...     ], schema="input_key string"
    ... )
    >>> mapping = [
    ...     ("original_value",    "input_key", spq.as_is),
    ...     ("transformed_value", "input_key", spq.to_num)
    ... ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +--------------+-----------------+
    |original_value|transformed_value|
    +--------------+-----------------+
    |  123456      |123456           |
    |Hello         |null             |
    |123_456       |123456           |
    +--------------+-----------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, alt_src_cols, cast):
        source_column = _coalesce_source_columns(source_column, alt_src_cols)
        return F.regexp_replace(F.trim(source_column), "_", "").cast(cast).alias(name)

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", T.LongType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def to_bool(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    More robust conversion from StringType to BooleanType.
    This method is able to additionally handle (compared to implicit Spark conversion):

        * Preceding and/or trailing whitespace
        * Define additional strings for true/false values ("on"/"off", "enabled"/"disabled" are added by default)

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#to_bool

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    case_sensitive : Bool, default -> False
        Defines whether the case for the additional true/false lookup values is considered
    true_values : list, default -> ["on", "enabled"]
        A list of values that should result in a ``True`` value if they are found in the source column
    false_values : list, default -> ["off", "disabled"]
        A list of values that should result in a ``False`` value if they are found in the source column
    replace_default_values : Bool, default -> False
        Defines whether additionally provided true/false values replace or extend the default list
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> T.BooleanType()
        Applies provided datatype on output column (``.cast(cast)``)

    Warning
    ---------
    Spark (and Spooq) handles number to boolean conversions depending on the input datatype!
    Please see this table for clarification:

    +-------+----------+-----------------+-------------+
    | Input            | Result                        |
    +-------+----------+-----------------+-------------+
    | Value | Datatype | Cast to Boolean | spq.to_bool |
    +=======+==========+=================+=============+
    |  -1   | int      | True            | NULL        |
    +-------+----------+-----------------+-------------+
    |  -1   | str      | NULL            | NULL        |
    +-------+----------+-----------------+-------------+
    |   0   | int      | False           | False       |
    +-------+----------+-----------------+-------------+
    |   0   | str      | False           | False       |
    +-------+----------+-----------------+-------------+
    |   1   | int      | True            | True        |
    +-------+----------+-----------------+-------------+
    |   1   | str      | True            | True        |
    +-------+----------+-----------------+-------------+
    | 100   | int      | True            | NULL        |
    +-------+----------+-----------------+-------------+
    | 100   | str      | NULL            | NULL        |
    +-------+----------+-----------------+-------------+

    Examples
    --------
    >>> input_df = spark.createDataFrame(
    ...     [
    ...         Row(input_string="  false "),
    ...         Row(input_string="123"),
    ...         Row(input_string="1"),
    ...         Row(input_string="Enabled"),
    ...         Row(input_string="n")
    ...     ], schema="input_key string"
    ... )
    >>> mapping = [
    ...     ("original_value",    "input_key", spq.as_is),
    ...     ("transformed_value", "input_key", spq.to_bool)
    ... ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +--------------+-----------------+
    |original_value|transformed_value|
    +--------------+-----------------+
    |  false       |false            |
    |123           |null             |
    |1             |true             |
    |Enabled       |true             |
    |n             |false            |
    +--------------+-----------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, true_values, false_values, case_sensitive, alt_src_cols, cast):
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
            .cast(cast)
            .alias(name)
        )

    args = dict(
        case_sensitive=kwargs.get("case_sensitive", False),
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", T.BooleanType()),
    )

    if kwargs.get("replace_default_values"):
        args["true_values"] = kwargs.get("true_values", ["on", "enabled"])
        args["false_values"] = kwargs.get("false_values", ["off", "disabled"])
    else:
        args["true_values"] = kwargs.get("true_values", []) + ["on", "enabled"]
        args["false_values"] = kwargs.get("false_values", []) + ["off", "disabled"]

    return _get_executable_function(_inner_func, source_column, name, **args)


def to_timestamp(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    More robust conversion from StringType to TimestampType (or as a formatted string).
    This method supports following input types:

        * Unix timestamps in seconds
        * Unix timestamps in milliseconds
        * Timestamps in any format supported by Spark
        * Timestamps in any custom format (via ``input_format``)
        * Preceding and/or trailing whitespace

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#to_timestamp

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    max_timestamp_sec : int, default -> 4102358400 (=> 2099-12-31 01:00:00)
        Defines the range in which unix timestamps are still considered as seconds (compared to milliseconds)
    input_format : [str, Bool], default -> False
        Spooq tries to convert the input string with the provided pattern (via ``F.unix_timestamp()``)
    output_format : [str, Bool], default -> False
        The output can be formatted according to the provided pattern (via ``F.date_format()``)
    min_timestamp_ms : int, default -> -62135514321000 (=> Year 1)
        Defines the overall allowed range to keep the timestamps within Python's ``datetime`` library limits
    max_timestamp_ms : int, default -> 253402210800000 (=> Year 9999)
        Defines the overall allowed range to keep the timestamps within Python's ``datetime`` library limits
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> T.TimestampType()
        Applies provided datatype on output column (``.cast(cast)``)

    Warning
    ---------
    * Timestamps in the range (-inf, -max_timestamp_sec) and (max_timestamp_sec, inf) are treated as milliseconds
    * There is a time interval (1970-01-01 +- ~2.5 months) where we can not distinguish correctly between s and ms
      (e.g. 3974400000 would be treated as seconds (2095-12-11T00:00:00) as the value is smaller than
      MAX_TIMESTAMP_S, but it could also be a valid date in Milliseconds (1970-02-16T00:00:00)

    Examples
    --------
    >>> input_df = spark.createDataFrame(
    ...     [
    ...         Row(input_string="2020-08-12T12:43:14+0000"),
    ...         Row(input_string="1597069446"),
    ...         Row(input_string="1597069446000"),
    ...         Row(input_string="2020-08-12"),
    ...     ], schema="input_key string"
    ... )
    >>> mapping = [
    ...     ("original_value",    "input_key", spq.as_is),
    ...     ("transformed_value", "input_key", spq.to_timestamp)
    ... ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +------------------------+-------------------+
    |original_value          |transformed_value  |
    +------------------------+-------------------+
    |2020-08-12T12:43:14+0000|2020-08-12 14:43:14|
    |1597069446              |2020-08-10 16:24:06|
    |1597069446000           |2020-08-10 16:24:06|
    |2020-08-12              |2020-08-12 00:00:00|
    +------------------------+-------------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(
            source_column,
            name, max_timestamp_sec, input_format, output_format, min_timestamp_ms, max_timestamp_ms, alt_src_cols, cast):
        source_column = _coalesce_source_columns(source_column, alt_src_cols)

        if input_format:
            output_col = (
                F.unix_timestamp(
                    source_column.cast(T.StringType()),
                    input_format
                ).cast(
                    T.TimestampType()
                ).alias(
                    name
                )
            )
        else:
            output_col = (
                F.when(
                    ~F.trim(source_column).cast(T.LongType()).between(min_timestamp_ms, max_timestamp_ms),
                    F.lit(None),
                )
                .when(
                    F.abs(F.trim(source_column).cast(T.LongType())).between(0, max_timestamp_sec),
                    F.trim(source_column).cast(T.LongType()).cast(T.TimestampType()),
                )
                .when(
                    F.abs(F.trim(source_column).cast(T.LongType())) > max_timestamp_sec,
                    (F.trim(source_column) / 1000).cast(T.TimestampType()),
                )
                .otherwise(F.trim(source_column))
            )
        if output_format:
            output_col = F.date_format(output_col, output_format)
            cast = T.StringType()
        return output_col.cast(cast).alias(name)

    args = dict(
        max_timestamp_sec=kwargs.get("max_timestamp_sec", 4102358400),
        input_format=kwargs.get("input_format", False),
        output_format=kwargs.get("output_format", False),
        min_timestamp_ms=kwargs.get("min_timestamp_ms", -62135514321000),
        max_timestamp_ms=kwargs.get("max_timestamp_ms", 253402210800000),
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", T.TimestampType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def str_to_array(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Splits a string into a list (ArrayType).

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#str_to_array

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> T.StringType()
        Applies provided datatype on the elements of the output array (``.cast(T.ArrayType(cast))``)

    Examples
    --------
    >>> input_df = spark.createDataFrame(
    ...     [
    ...         Row(input_column="[item1,item2,3]"),
    ...         Row(input_column="item1,it[e]m2,it em3"),
    ...         Row(input_column="    item1,   item2    ,   item3")
    ...     ], schema="input_key string"
    ... )
    >>> mapping = [
    ...     ("original_value",    "input_key", spq.as_is),
    ...     ("transformed_value", "input_key", spq.str_to_array)
    ... ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.printSchema()
    root
     |-- original_value: string (nullable = true)
     |-- transformed_value: array (nullable = true)
     |    |-- element: string (containsNull = true)
    >>> output_df.show(truncate=False)
    +-------------------------------+------------------------+
    |original_value                 |transformed_value       |
    +-------------------------------+------------------------+
    |[item1,item2,3]                |[item1, item2, 3]       |
    |item1,it[e]m2,it em3           |[item1, it[e]m2, it em3]|
    |    item1,   item2    ,   item3|[item1, item2, item3]   |
    +-------------------------------+------------------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, alt_src_cols, cast):
        source_column = _coalesce_source_columns(source_column, alt_src_cols)

        if isinstance(cast, str):
            output_type = f"array<{cast}>"
        else:
            output_type = T.ArrayType(cast)

        return (
            F.split(F.regexp_replace(source_column, r"^\s*\[*\s*|\s*\]*\s*$", ""), r"\s*,\s*")
            .cast(output_type)
            .alias(name)
        )

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", T.StringType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def map_values(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Maps input values to specified output values.

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#map_values

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    mapping : dict
        Dictionary containing lookup / substitute value pairs.
    default : [str, Column, Any], default -> "source_column"
        Defines what will be returned if no matching lookup value was found.
    ignore_case : bool, default -> True
        Only relevant for "equals" and "sql_like" comparison operators.
    pattern_type : str, default -> "equals"
        Please choose among ['equals', 'regex' and 'sql_like'] for the comparison of input value and mapping key.
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> T.StringType()
        Applies provided datatype on output column (``.cast(cast)``)

    Hint
    ----
    Maybe this table helps you to better understand what happens behind the curtains:

    +---------------+------------+---------------------------------------------------------------------------+
    | lookup        | substitute | mode         | Internal Spark Logic                                       |
    +===============+============+=====================================+==================+==================+
    | whitelist     | allowlist  | "equals"     | F.when(                                                    |
    |               |            |              |     F.col("input_column") == "whitelist",                  |
    |               |            |              |     F.lit("allowlist")                                     |
    |               |            |              | ).otherwise(                                               |
    |               |            |              |     F.col("input_column")                                  |
    |               |            |              | )                                                          |
    +---------------+------------+---------------------------------------------------------------------------+
    | %whitelist%   | allowlist  | "sql_like"   | F.when(                                                    |
    |               |            |              |     F.col("input_column").like("%whitelist%",              |
    |               |            |              |     F.lit("allowlist")                                     |
    |               |            |              | ).otherwise(                                               |
    |               |            |              |     F.col("input_column")                                  |
    |               |            |              | )                                                          |
    +---------------+------------+---------------------------------------------------------------------------+
    | .*whitelist.* | allowlist  | "regex   "   | F.when(                                                    |
    |               |            |              |     F.col("input_column").rlike(".*whitelist.*",           |
    |               |            |              |     F.lit("allowlist")                                     |
    |               |            |              | ).otherwise(                                               |
    |               |            |              |     F.col("input_column")                                  |
    |               |            |              | )                                                          |
    +---------------+------------+---------------------------------------------------------------------------+

    Examples
    --------
    >>> input_df = spark.createDataFrame(
    ...     [
    ...         ("allowlist", ),
    ...         ("WhiteList", ),
    ...         ("blocklist", ),
    ...         ("blacklist", ),
    ...         ("Blacklist", ),
    ...         ("Shoppinglist", ),
    ...     ], schema="input_key string"
    ... )
    >>> substitute_mapping = {"whitelist": "allowlist", "blacklist": "blocklist"}
    >>> mapping = [
    ...     ("original_value",    "input_key", spq.as_is),
    ...     ("transformed_value", "input_key", spq.map_values(mapping=substitute_mapping))
    ... ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +--------------+-----------------+
    |original_value|transformed_value|
    +--------------+-----------------+
    |allowlist     |allowlist        |
    |WhiteList     |allowlist        |
    |blocklist     |blocklist        |
    |blacklist     |blocklist        |
    |Blacklist     |blocklist        |
    |Shoppinglist  |Shoppinglist     |
    +--------------+-----------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, mapping, default, ignore_case, pattern_type, alt_src_cols, cast):
        source_column = _coalesce_source_columns(source_column, alt_src_cols)

        if isinstance(default, str) and default == "source_column":
            default = source_column
        elif not isinstance(default, Column):
            default = F.lit(default)

        if ignore_case and pattern_type != "regex":
            mapping = {str(key).lower(): F.lit(value).cast(cast) for key, value in mapping.items()}
            source_column = F.lower(source_column)
        else:
            mapping = {key: F.lit(value).cast(cast) for key, value in mapping.items()}

        keys = list(mapping.keys())
        if pattern_type == "equals":
            when_clause = F.when(source_column == keys[0], mapping[keys[0]])
            for key in keys[1:]:
                when_clause = when_clause.when(source_column.cast(T.StringType()) == key, mapping[key])

        elif pattern_type == "sql_like":
            when_clause = F.when(source_column.like(keys[0]), mapping[keys[0]])
            for key in keys[1:]:
                when_clause = when_clause.when(source_column.like(key), mapping[key])

        elif pattern_type == "regex":
            when_clause = F.when(source_column.rlike(keys[0]), mapping[keys[0]])
            for key in keys[1:]:
                when_clause = when_clause.when(source_column.rlike(key), mapping[key])

        else:
            raise ValueError(f"pattern_type <{pattern_type}> not recognized. "
                             "Please choose among ['equals' (default), 'regex' and 'sql_like']")

        when_clause = when_clause.otherwise(default.cast(cast))

        return when_clause.alias(name)

    try:
        mapping = kwargs["mapping"]
    except TypeError:
        raise TypeError("'map_values' is missing the mapping dict (f.e. mapping=dict(results='training'))")
    if len(mapping.keys()) < 1:
        raise ValueError("'map_values' received an empty map (f.e. mapping=dict(results='training'))")

    args = dict(
        mapping=mapping,
        default=kwargs.get("default", "source_column"),
        ignore_case=kwargs.get("ignore_case", True),
        pattern_type=kwargs.get("pattern_type", "equals"),
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", T.StringType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def meters_to_cm(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Converts meters to cm and casts the result to an IntegerType.

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#meters_to_cm

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> T.IntegerType()
        Applies provided datatype on output column (``.cast(cast)``)

    Examples
    --------
    >>> input_df = spark.createDataFrame([
    ...     Row(size_in_m=1.80),
    ...     Row(size_in_m=1.65),
    ...     Row(size_in_m=2.05)
    ... ])
    >>> mapping = [("size_in_cm", "size_in_m", spq.meters_to_cm)]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +----------+
    |size_in_cm|
    +----------+
    |180       |
    |165       |
    |204       |
    +----------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, alt_src_cols, cast):
        source_column = _coalesce_source_columns(source_column, alt_src_cols)
        return (source_column * 100).cast(cast).alias(name)

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", T.IntegerType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def has_value(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Returns True if the source_column is
        - not NULL and
        - not "" (empty string)
        - otherwise it returns False

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#has_value

    Warning
    -------
    This means that it will return True for values which would indicate a False value. Like "false" or 0!!!

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> T.BooleanType()
        Applies provided datatype on output column (``.cast(cast)``)

    Examples
    --------
    >>> input_df = spark.createDataFrame(
    ...     [
    ...         Row(input_key=False),
    ...         Row(input_key=None),
    ...         Row(input_key="some text"),
    ...         Row(input_key="")
    ...     ], schema="input_key string"
    ... )
    >>> mapping = [
    ...     ("original_value", "input_key", spq.as_is),
    ...     ("does_it_have_value", "input_key", spq.has_value)
    ... ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +--------------+------------------+
    |original_value|does_it_have_value|
    +--------------+------------------+
    |false         |true              |
    |null          |false             |
    |some text     |true              |
    |              |false             |
    +--------------+------------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, alt_src_cols, cast):
        source_column = _coalesce_source_columns(source_column, alt_src_cols)
        return (
            F.when((source_column.isNotNull()) & (source_column.cast(T.StringType()) != ""), F.lit(True))
            .otherwise(F.lit(False))
            .cast(cast)
            .alias(name)
        )

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", T.BooleanType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def apply(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Applies a function / partial

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#apply

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    func : Callable
        Function that takes the source column as single argument
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> no casting
        Applies provided datatype on output column (``.cast(cast)``)

    Examples
    --------
    >>> input_df = spark.createDataFrame(
    ...     [
    ...         ("F", ),
    ...         ("f", ),
    ...         ("x", ),
    ...         ("X", ),
    ...         ("m", ),
    ...         ("M", ),
    ...     ], schema="input_key string"
    ... )
    >>> mapping = [
    ...     ("original_value",    "input_key", spq.as_is),
    ...     ("transformed_value", "input_key", spq.apply(func=F.lower))
    ... ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +--------------+-----------------+
    |original_value|transformed_value|
    +--------------+-----------------+
    |F             |f                |
    |f             |f                |
    |x             |x                |
    |X             |x                |
    |m             |m                |
    |M             |m                |
    +--------------+-----------------+

    >>> input_df = spark.createDataFrame(
    ...     [
    ...         ("sarajishvilileqso@gmx.at", ),
    ...         ("jnnqn@astrinurdin.art", ),
    ...         ("321aw@hotmail.com", ),
    ...         ("techbrenda@hotmail.com", ),
    ...         ("sdsxcx@gmail.com", ),
    ...     ], schema="input_key string"
    ... )
    ...
    >>> def _has_hotmail(source_column):
    ...     return F.when(
    ...         source_column.cast(T.StringType()).endswith("@hotmail.com"),
    ...         F.lit(True)
    ...     ).otherwise(F.lit(False))
    ...
    >>> mapping = [
    ...     ("original_value",    "input_key", spq.as_is),
    ...     ("transformed_value", "input_key", spq.apply(func=_has_hotmail))
    ... ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +------------------------+-----------------+
    |original_value          |transformed_value|
    +------------------------+-----------------+
    |sarajishvilileqso@gmx.at|false            |
    |jnnqn@astrinurdin.art   |false            |
    |321aw@hotmail.com       |true             |
    |techbrenda@hotmail.com  |true             |
    |sdsxcx@gmail.com        |false            |
    +------------------------+-----------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, func, alt_src_cols, cast):
        source_column = _coalesce_source_columns(source_column, alt_src_cols)
        source_column = func(source_column)
        if cast:
            source_column = source_column.cast(cast)
        return source_column.alias(name)

    try:
        func = kwargs["func"]
    except TypeError:
        raise TypeError("'apply' transformation is missing the custom function (f.e. func=F.lower)")

    args = dict(
        func=func,
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", False),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def to_json_string(source_column=None, name=None, **kwargs: Any) -> partial:
    """
    Returns a column as json compatible string. Nested hierarchies are supported.
    This function also supports NULL and strings as input in comparison to Spark's built-in ``to_json``.
    The unicode representation of a column will be returned if an error occurs.

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#to_json_string

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> no casting, same return data type as input data type
        Applies provided datatype on output column (``.cast(cast)``)

    Examples
    --------
    >>> input_df = spark.createDataFrame([
    ...     Row(friends=[Row(first_name="Gianni", id=3993, last_name="Weber"),
    ...                  Row(first_name="Arielle", id=17484, last_name="Greaves")]),
    ... ])
    >>> mapping = [("friends_json", "friends", spq.to_json_string)]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +----------------------------------------------------------------------------------------------------------------------------+
    |friends_json                                                                                                                |
    +----------------------------------------------------------------------------------------------------------------------------+
    |[{"first_name": "Gianni", "id": 3993, "last_name": "Weber"}, {"first_name": "Arielle", "id": 17484, "last_name": "Greaves"}]|
    +----------------------------------------------------------------------------------------------------------------------------+
    >>> input_df.select(spq.to_json_string("friends.first_name")).show(truncate=False)
    +---------------------+
    |first_name           |
    +---------------------+
    |['Gianni', 'Arielle']|
    +---------------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how you called it. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, alt_src_cols, cast):
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

        source_column = _coalesce_source_columns(source_column, alt_src_cols)

        udf_to_json = F.udf(_to_json, cast)
        return udf_to_json(source_column).alias(name)

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=kwargs.get("cast", T.StringType()),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)

#
# def unix_timestamp_to_unix_timestamp(source_column=None, name=None, **kwargs: Any) -> partial:
#     """
#     Converts a unix timestamp (number) between milliseconds and seconds
#     and casts it to a :any:`pyspark.sql.types.LongType`.
#
#     https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#unix_timestamp_to_unix_timestamp
#
#     Parameters
#     ----------
#     source_column : str or Column
#         Input column. Can be a name, pyspark column or pyspark function
#     name : str, default -> derived from input column
#         Name of the output column. (``.alias(name)``)
#
#     Keyword Arguments
#     -----------------
#     input_time_unit : str, default -> "ms"
#         Defines the time unit of the source value.
#         Possible Values: ["ms", "sec"]
#     output_time_unit : str, default -> "sec"
#         Defines the time unit of the target value.
#         Possible Values: ["ms", "sec"]
#     alt_src_cols : str, default -> no coalescing, only source_column
#         Coalesce with source_column and columns from this parameter.
#     cast : T.DataType(), default -> no casting, same return data type as input data type
#         Applies provided datatype on output column (``.cast(cast)``)
#
#     Examples
#     --------
#     >>> input_df = spark.createDataFrame([
#     ...     Row(time_ms=1581540839000),  # 2020-02-12 21:53:59
#     ...     Row(time_ms=-4887839000),    # 1969-11-05 11:16:01
#     ...     Row(time_ms=4737139200000)   # 2120-02-12 01:00:00
#     ... ])
#     >>> mapping = [
#     ...     ("unix_ts", "time_ms", spq.unix_timestamp_to_unix_timestamp(input_time_unit="ms", output_time_unit="sec"))
#     ... ]
#     >>> output_df = Mapper(mapping).transform(input_df)
#     >>> output_df.show(truncate=False)
#     +----------+
#     |unix_ts   |
#     +----------+
#     |1581540839|
#     |-4887839  |
#     |4737139200|
#     +----------+
#     >>> input_df.select(
#     ...     spq.unix_timestamp_to_unix_timestamp("time_ms", name="timestamp", cast=T.TimestampType())
#     ... ).show(truncate=False)
#     +-------------------+
#     |timestamp            |
#     +-------------------+
#     |2020-02-12 21:53:59|
#     |1969-11-05 11:16:01|
#     |2120-02-12 01:00:00|
#     +-------------------+
#
#     Returns
#     -------
#     partial or Column
#         This method returns a suitable type depending on how you called it. This ensures compability
#         with Spooq's mapper transformer - with or without explicit parameters as well as direct calls via select,
#         withColumn, where, ...
#     """
#
#     def _inner_func(source_column, name, input_time_unit, output_time_unit, alt_src_cols, cast):
#         if alt_src_cols:
#             source_column = _coalesce_source_columns(source_column, alt_src_cols)
#         if input_time_unit == "ms":
#             source_column = source_column / 1_000.0
#
#         output_column = source_column.cast(T.DoubleType())
#
#         if output_time_unit == "ms":
#             output_column = output_column * 1_000.0
#
#         return output_column.cast(cast).alias(name)
#
#     args = dict(
#         input_time_unit=kwargs.get("input_time_unit", "ms"),
#         output_time_unit=kwargs.get("output_time_unit", "sec"),
#         alt_src_cols=kwargs.get("alt_src_cols", False),
#         cast=kwargs.get("cast", T.LongType()),
#     )
#
#     return _get_executable_function(_inner_func, source_column, name, **args)


# Convenience Methods #

def to_str(source_column=None, name=None, **kwargs: Any) -> Union[partial, Column]:
    """
    Convenience transformation that only casts to string.

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#to_str

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.
    cast : T.DataType(), default -> T.StringType()
        Applies provided datatype on output column (``.cast(cast)``)

    Examples
    --------
    >>> input_df = spark.createDataFrame(
    ...     [
    ...         Row(input_string=123456),
    ...         Row(input_string=-123456),
    ...     ], schema="input_key int"
    ... )
    >>> mapping = [
    ...     ("original_value",    "input_key", spq.as_is),
    ...     ("transformed_value", "input_key", spq.to_str)
    ... ]
    >>> output_df = Mapper(mapping).transform(input_df)
    >>> output_df.show(truncate=False)
    +--------------+-----------------+
    |original_value|transformed_value|
    +--------------+-----------------+
    |123456        |123456           |
    |-123456       |-123456          |
    +--------------+-----------------+

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    def _inner_func(source_column, name, alt_src_cols):
        source_column = _coalesce_source_columns(source_column, alt_src_cols)
        return source_column.cast(T.StringType()).alias(name)

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
    )

    return _get_executable_function(_inner_func, source_column, name, **args)


def to_int(source_column=None, name=None, **kwargs: Any) -> Union[partial, Column]:
    """
    Syntactic sugar for calling ``to_num(cast=T.IntegerType())``

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#to_num

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=T.IntegerType(),
    )

    return to_num(source_column, name, **args)


def to_long(source_column=None, name=None, **kwargs: Any) -> Union[partial, Column]:
    """
    Syntactic sugar for calling ``to_num(cast=T.LongType())``

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#to_num

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=T.LongType(),
    )

    return to_num(source_column, name, **args)


def to_float(source_column=None, name=None, **kwargs: Any) -> Union[partial, Column]:
    """
    Syntactic sugar for calling ``to_num(cast=T.FloatType())``

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#to_num

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=T.FloatType(),
    )

    return to_num(source_column, name, **args)


def to_double(source_column=None, name=None, **kwargs: Any) -> Union[partial, Column]:
    """
    Syntactic sugar for calling ``to_num(cast=T.DoubleType())``

    https://spooq.rtfd.io/en/latest/transformer/mapper_transformations.html#to_num

    Parameters
    ----------
    source_column : str or Column
        Input column. Can be a name, pyspark column or pyspark function
    name : str, default -> derived from input column
        Name of the output column. (``.alias(name)``)

    Keyword Arguments
    -----------------
    alt_src_cols : str, default -> no coalescing, only source_column
        Coalesce with source_column and columns from this parameter.

    Returns
    -------
    partial or Column
        This method returns a suitable type depending on how it was called. This ensures compability
        with Spooq's mapper transformer - with or without explicit parameters - as well as direct calls via select,
        withColumn, where, ...
    """

    args = dict(
        alt_src_cols=kwargs.get("alt_src_cols", False),
        cast=T.DoubleType(),
    )

    return to_num(source_column, name, **args)
