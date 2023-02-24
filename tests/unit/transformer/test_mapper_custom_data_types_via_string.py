from builtins import object
import json
import datetime

import pytest
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql import types as T
import semver

import spooq.transformer.mapper_custom_data_types as custom_types
from spooq.transformer import Mapper
from tests.data.test_fixtures.mapper_custom_data_types_fixtures import (
    fixtures_for_has_value,
    fixtures_for_str_to_int,
    fixtures_for_str_to_long,
    fixtures_for_str_to_float,
    fixtures_for_str_to_double,
    fixtures_for_to_bool_default,
    fixtures_for_extended_string_to_timestamp_spark2,
    fixtures_for_extended_string_unix_timestamp_ms_to_timestamp_spark2,
    fixtures_for_extended_string_to_date_spark2,
    fixtures_for_extended_string_unix_timestamp_ms_to_date_spark2,
    fixtures_for_to_timestamp_default,
    fixtures_for_extended_string_unix_timestamp_ms_to_timestamp,
    fixtures_for_extended_string_to_date,
    fixtures_for_extended_string_unix_timestamp_ms_to_date,
    fixtures_for_timestamp_ms_to_s,
    fixtures_for_timestamp_s_to_ms,
)
from tests.helpers.skip_conditions import only_spark2, only_spark3


def get_spark_data_type(input_value):
    return {
        "str": T.StringType(),
        "int": T.LongType(),
        "bool": T.BooleanType(),
        "float": T.DoubleType(),
        "NoneType": T.NullType(),
    }[type(input_value).__name__]


def parameter_to_string_id(val):
    return "<" + str(val) + ">"


def parameters_to_string_id(actual_value, expected_value):
    return " actual: <{a}> ({a_cls}) -> expected: <{e}>  ({e_cls})".format(
        a=actual_value, a_cls=str(type(actual_value)), e=expected_value, e_cls=str(type(expected_value))
    )


def get_input_df(spark_session, spark_context, source_key, input_value):
    input_json = json.dumps({"attributes": {"data": {source_key: input_value}}})
    return spark_session.read.json(spark_context.parallelize([input_json]))


class TestDynamicallyCallMethodsByDataTypeName(object):
    def test_exception_is_raised_if_data_type_not_found(self):
        source_column, name = "element.key", "element_key"
        data_type = "NowhereToBeFound"

        with pytest.raises(AttributeError):
            custom_types._get_select_expression_for_custom_type(source_column, name, data_type)


class TestMiscConversions(object):
    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), [
        ("only some text",
         "only some text"),
        (None,
         None),
        ({"key": "value"},
         Row(key="value")),
        ({"key": {"other_key": "value"}},
         Row(key=Row(other_key="value"))),
        ({"age": 18,"weight": 75},
         Row(age=18, weight=75)),
        ({"list_of_friend_ids": [12, 75, 44, 76]},
         Row(list_of_friend_ids=[12, 75, 44, 76])),
        ([{"weight": "75"}, {"weight": "76"}, {"weight": "73"}],
         [Row(weight="75"), Row(weight="76"), Row(weight="73")]),
        ({"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]},
         Row(list_of_friend_ids=[Row(id=12), Row(id=75), Row(id=44), Row(id=76)])),
    ])
    # fmt: on
    def test_generate_select_expression_without_casting(self, input_value, value, spark_session, spark_context):
        source_key, name = "demographics", "statistics"
        mapping = [
            (name, f"attributes.data.{source_key}", "as_is"),
        ]
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        output_df = Mapper(mapping).transform(input_df)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), [
        ("only some text",
         "only some text"),
        (None,
         None),
        ({"key": "value"},
         '{"key": "value"}'),
        ({"key": {"other_key": "value"}},
         '{"key": {"other_key": "value"}}'),
        ({"age": 18, "weight": 75},
         '{"age": 18, "weight": 75}'),
        ({"list_of_friend_ids": [12, 75, 44, 76]},
         '{"list_of_friend_ids": [12, 75, 44, 76]}'),
        ([{"weight": "75"}, {"weight": "76"}, {"weight": "73"}],
         '[{"weight": "75"}, {"weight": "76"}, {"weight": "73"}]'),
        ({"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]},
         '{"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]}')
    ])
    # fmt: on
    def test_generate_select_expression_for_json_string(self, input_value, value, spark_session, spark_context):
        source_key, name = "demographics", "statistics"
        mapping = [
            (name, f"attributes.data.{source_key}", "json_string"),
        ]
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        output_df = Mapper(mapping).transform(input_df)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "string", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=[
            (1.80,    180),
            (2.,      200),
            (-1.0,   -100),
            (0.0,       0),
            (0,         0),
            (2,       200),
            (-4,     -400),
            ("1.80",  180),
            ("2.",    200),
            ("-1.0", -100),
            ("0.0",     0),
            (None,   None),
            ("one",  None),
        ])
    # fmt: on
    def test_generate_select_expression_for_meters_to_cm(self, input_value, expected_value, spark_session):
        input_df = spark_session.createDataFrame(
            data=[Row(input_key=input_value)],
            schema=T.StructType([T.StructField("input_key", get_spark_data_type(input_value), True)]),
        )
        output_df = Mapper(mapping=[("output_column", "input_key", "meters_to_cm")]).transform(input_df)
        assert output_df.first().output_column == expected_value, "Processing of column value"
        assert output_df.schema.fieldNames() == ["output_column"], "Renaming of column"
        assert output_df.schema["output_column"].dataType.typeName() == "integer", "Casting of column"

    @pytest.mark.parametrize(argnames=("input_value", "expected_value"), argvalues=fixtures_for_has_value)
    def test_generate_select_expression_for_has_value(self, input_value, expected_value, spark_session):
        input_df = spark_session.createDataFrame(
            data=[Row(input_key=input_value)],
            schema=T.StructType([T.StructField("input_key", get_spark_data_type(input_value), True)]),
        )
        output_df = Mapper(mapping=[("output_column", "input_key", "has_value")]).transform(input_df)
        assert output_df.first().output_column == expected_value, "Processing of column value"
        assert output_df.schema.fieldNames() == ["output_column"], "Renaming of column"
        assert output_df.schema["output_column"].dataType.typeName() == "boolean", "Casting of column"


class TestExtendedStringConversions(object):
    @staticmethod
    def create_input_df(input_value, spark_session):
        return spark_session.createDataFrame(
            data=[Row(input_key=input_value)],
            schema=T.StructType([T.StructField("input_key", get_spark_data_type(input_value), True)]),
        )

    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_str_to_int,
        ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_str_to_int],
    )
    def test_extended_string_to_int(self, spark_session, input_value, expected_value):
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_int")]).transform(input_df)
        assert output_df.first().output_key == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.IntegerType)

    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_str_to_long,
        ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_str_to_long],
    )
    def test_extended_string_to_long(self, spark_session, input_value, expected_value):
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_long")]).transform(input_df)
        assert output_df.first().output_key == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.LongType)

    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_str_to_float,
        ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_str_to_float],
    )
    def test_extended_string_to_float(self, spark_session, input_value, expected_value):
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_float")]).transform(input_df)
        actual_value = output_df.first().output_key
        if actual_value is not None:
            assert pytest.approx(actual_value) == expected_value
        else:
            assert actual_value == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.FloatType)

    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_str_to_double,
        ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_str_to_double],
    )
    def test_extended_string_to_double(self, spark_session, input_value, expected_value):
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_double")]).transform(input_df)
        actual_value = output_df.first().output_key
        if actual_value is not None:
            assert pytest.approx(actual_value) == expected_value
        else:
            assert actual_value == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.DoubleType)

    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_to_bool_default,
        ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_to_bool_default],
    )
    def test_extended_string_to_boolean(self, spark_session, input_value, expected_value):
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(mapping=[("output_key", "input_key", "to_bool")]).transform(input_df)
        assert output_df.first().output_key == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.BooleanType)

    @only_spark2
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_extended_string_to_timestamp_spark2,
        ids=[
            parameters_to_string_id(actual, expected)
            for actual, expected in fixtures_for_extended_string_to_timestamp_spark2
        ],
    )
    def test_extended_string_to_timestamp_spark2(self, spark_session, input_value, expected_value):
        # test uses timezone set to GMT / UTC (pytest.ini)!
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(mapping=[("output_key", "input_key", "to_timestamp")]).transform(input_df)
        # workaround via pandas necessary due to bug with direct conversion
        # to python datetime wrt timezone conversions (https://issues.apache.org/jira/browse/SPARK-32123)
        try:
            output_pd_df = output_df.toPandas()
            actual_value = output_pd_df.iloc[0]["output_key"].to_pydatetime()
        except ValueError:
            # If input is in milliseconds it will still be stored in the DF but cannot be collected in Python
            actual_value = "out_of_range_for_python"
        except AttributeError:
            # `.to_pydatetime()` can only be used on datetimes and throws AttributeErrors on other objects / None
            actual_value = None
        assert actual_value == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.TimestampType)

    @only_spark3
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_to_timestamp_default,
        ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_to_timestamp_default],
    )
    def test_extended_string_to_timestamp(self, spark_session, spark_context, input_value, expected_value):
        # test uses timezone set to GMT / UTC (pytest.ini)!
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(mapping=[("output_key", "input_key", "to_timestamp")]).transform(input_df)
        actual_value = output_df.select(F.date_format("output_key", "yyyy-MM-dd HH:mm:ss:SSSSSS").alias("output_key")).first().output_key

        if isinstance(expected_value, dict):
            expected_value = next(
                expected_value[version]
                for version
                in expected_value.keys()
                if semver.match(spark_context.version, version)
            )
        if isinstance(expected_value, str):
            expected_value_ = expected_value
        elif isinstance(expected_value, datetime.datetime):
            expected_value_ = datetime.datetime.strftime(expected_value, "%Y-%m-%d %H:%M:%S:%f")
        else:
            expected_value_ = None

        assert expected_value_ == actual_value
        assert isinstance(output_df.schema["output_key"].dataType, T.TimestampType)

    @only_spark2
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_extended_string_unix_timestamp_ms_to_timestamp_spark2,
        ids=[
            parameters_to_string_id(actual, expected)
            for actual, expected in fixtures_for_extended_string_unix_timestamp_ms_to_timestamp_spark2
        ],
    )
    def test_extended_string_unix_timestamp_ms_to_timestamp_spark2(self, spark_session, input_value, expected_value):
        # test uses timezone set to GMT / UTC (pytest.ini)!
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(
            mapping=[("output_key", "input_key", "extended_string_unix_timestamp_ms_to_timestamp")]
        ).transform(input_df)
        # workaround via pandas necessary due to bug with direct conversion
        # to python datetime wrt timezone conversions (https://issues.apache.org/jira/browse/SPARK-32123)
        try:
            output_pd_df = output_df.toPandas()
            actual_value = output_pd_df.iloc[0]["output_key"].to_pydatetime()
            error_message = f"actual_value: {actual_value}, expected value: {expected_value}"
            assert actual_value.toordinal() == expected_value.toordinal(), error_message

        except AttributeError:
            # `.to_pydatetime()` can only be used on datetimes and throws AttributeErrors on None
            assert expected_value is None
        assert isinstance(output_df.schema["output_key"].dataType, T.TimestampType)

    @only_spark3
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_extended_string_unix_timestamp_ms_to_timestamp,
        ids=[
            parameters_to_string_id(actual, expected)
            for actual, expected in fixtures_for_extended_string_unix_timestamp_ms_to_timestamp
        ],
    )
    def test_extended_string_unix_timestamp_ms_to_timestamp(self, spark_session, input_value, expected_value):
        # test uses timezone set to GMT / UTC (pytest.ini)!
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(
            mapping=[("output_key", "input_key", "extended_string_unix_timestamp_ms_to_timestamp")]
        ).transform(input_df)
        # workaround via pandas necessary due to bug with direct conversion
        # to python datetime wrt timezone conversions (https://issues.apache.org/jira/browse/SPARK-32123)
        output_pd_df = output_df.toPandas()
        output_value = output_pd_df.iloc[0]["output_key"]
        if isinstance(output_value, (type(pd.NaT))):
            actual_value = None
        else:
            actual_value = output_value.to_pydatetime().toordinal()
            expected_value = expected_value.toordinal()
        assert actual_value == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.TimestampType)

    @only_spark2
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_extended_string_to_date_spark2,
        ids=[
            parameters_to_string_id(actual, expected)
            for actual, expected in fixtures_for_extended_string_to_date_spark2
        ],
    )
    def test_extended_string_to_date_spark2(self, spark_session, input_value, expected_value):
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_date")]).transform(input_df)
        try:
            actual_value = output_df.first().output_key
        except ValueError:
            # If input is in milliseconds it will still be stored in the DF but cannot be collected in Python
            actual_value = "out_of_range_for_python"
        assert actual_value == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.DateType)

    @only_spark3
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_extended_string_to_date,
        ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_extended_string_to_date],
    )
    def test_extended_string_to_date(self, spark_session, input_value, expected_value):
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_date")]).transform(input_df)
        actual_value = output_df.first().output_key
        assert actual_value == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.DateType)

    @only_spark2
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_extended_string_unix_timestamp_ms_to_date_spark2,
        ids=[
            parameters_to_string_id(actual, expected)
            for actual, expected in fixtures_for_extended_string_unix_timestamp_ms_to_date_spark2
        ],
    )
    def test_extended_string_unix_timestamp_ms_to_date_spark2(self, spark_session, input_value, expected_value):
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(
            mapping=[("output_key", "input_key", "extended_string_unix_timestamp_ms_to_date")]
        ).transform(input_df)
        actual_value = output_df.first().output_key
        assert actual_value == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.DateType)

    @only_spark3
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=fixtures_for_extended_string_unix_timestamp_ms_to_date,
        ids=[
            parameters_to_string_id(actual, expected)
            for actual, expected in fixtures_for_extended_string_unix_timestamp_ms_to_date
        ],
    )
    def test_extended_string_unix_timestamp_ms_to_date(self, spark_session, input_value, expected_value):
        input_df = self.create_input_df(input_value, spark_session)
        output_df = Mapper(
            mapping=[("output_key", "input_key", "extended_string_unix_timestamp_ms_to_date")]
        ).transform(input_df)
        actual_value = output_df.first().output_key
        assert actual_value == expected_value
        assert isinstance(output_df.schema["output_key"].dataType, T.DateType)


class TestAnonymizingMethods(object):
    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), [
        ("my_first_mail@myspace.com", "1"),
        ("",                          None),
        (None,                        None),
        (" ",                         "1"),
        (100,                         "1"),
        (0,                           "1")],
    )
    # fmt: on
    def test_generate_select_expression_for_StringBoolean(self, input_value, value, spark_session, spark_context):
        source_key, name = "email_address", "mail"
        mapping = [
            (name, f"attributes.data.{source_key}", "StringBoolean"),
        ]
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        output_df = Mapper(mapping).transform(input_df)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "string", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), [
        ("my_first_mail@myspace.com", None),
        ("",                          None),
        (None,                        None),
        (" ",                         None),
        (100,                         None),
        (0,                           None)],
    )
    # fmt: on
    def test_generate_select_expression_for_StringNull(self, input_value, value, spark_session, spark_context):
        source_key, name = "email_address", "mail"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        result_column = custom_types._generate_select_expression_for_StringNull(
            source_column=input_df["attributes"]["data"][source_key], name=name
        )
        output_df = input_df.select(result_column)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "string", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), [
        (12345,             1),
        ("",                1),
        ("some text",       1),
        (None,              None),
        (0,                 1),
        (1,                 1),
        (-1,                1),
        (5445.23,           1),
        (float("inf"),      1),
        (-1 * float("inf"), 1)
    ])
    # fmt: on
    def test_generate_select_expression_for_IntBoolean(self, input_value, value, spark_session, spark_context):
        source_key, name = "user_id", "id"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        result_column = custom_types._generate_select_expression_for_IntBoolean(
            source_column=input_df["attributes"]["data"][source_key], name=name
        )
        output_df = input_df.select(result_column)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "integer", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), [
        (12345,             None),
        ("",                None),
        ("some text",       None),
        (None,              None),
        (0,                 None),
        (1,                 None),
        (-1,                None),
        (5445.23,           None),
        (float("inf"),      None),
        (-1 * float("inf"), None),
    ])
    # fmt: on
    def test_generate_select_expression_for_IntNull(self, input_value, value, spark_session, spark_context):
        source_key, name = "user_id", "id"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        result_column = custom_types._generate_select_expression_for_IntNull(
            source_column=input_df["attributes"]["data"][source_key], name=name
        )
        output_df = input_df.select(result_column)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "integer", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @only_spark2
    @pytest.mark.parametrize(("input_value", "value"), [
        (None,         None),
        ("1955-09-41", None),
        ("1969-04-03", "1969-04-01"),
        ("1985-03-07", "1985-03-01"),
        ("1998-06-10", "1998-06-01"),
        ("1967-05-16", "1967-05-01"),
        ("1953-01-01", "1953-01-01"),
        ("1954-11-06", "1954-11-01"),
        ("1978-09-05", "1978-09-01"),
        ("1999-05-23", "1999-05-01"),
    ])
    # fmt: on
    def test_generate_select_expression_for_TimestampMonth_spark2(
        self, input_value, value, spark_session, spark_context
    ):
        source_key, name = "day_of_birth", "birthday"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        input_df = input_df.withColumn(
            source_key, F.to_utc_timestamp(input_df["attributes"]["data"][source_key], "yyyy-MM-dd")
        )
        result_column = custom_types._generate_select_expression_for_TimestampMonth(
            source_column=input_df[source_key], name=name
        )
        output_df = input_df.select(result_column)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "timestamp", "Casting of column"
        output_value = output_df.first()[name]
        if output_value:
            output_value = datetime.date.strftime(output_value, format("%Y-%m-%d"))
        else:
            output_value = None
        assert output_value == value, "Processing of column value"

    # fmt: off
    @only_spark3
    @pytest.mark.parametrize(("input_value", "value"), [
        (None,         None),
        ("1955-09-41", None),
        ("1969-04-03", "1969-04-01"),
        ("1985-03-07", "1985-03-01"),
        ("1998-06-10", "1998-06-01"),
        ("1967-05-16", "1967-05-01"),
        ("1953-01-01", "1953-01-01"),
        ("1954-11-06", "1954-11-01"),
        ("1978-09-05", "1978-09-01"),
        ("1999-05-23", "1999-05-01"),
    ])
    # fmt: on
    def test_generate_select_expression_for_TimestampMonth(self, input_value, value, spark_session, spark_context):
        source_key, name = "day_of_birth", "birthday"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        input_df = input_df.withColumn(source_key, input_df["attributes"]["data"][source_key].cast(T.DateType()))
        result_column = custom_types._generate_select_expression_for_TimestampMonth(
            source_column=input_df[source_key], name=name
        )
        output_df = input_df.select(result_column)
        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "timestamp", "Casting of column"
        output_value = output_df.first()[name]
        if output_value:
            output_value = datetime.date.strftime(output_value, format("%Y-%m-%d"))
        else:
            output_value = None
        assert output_value == value, "Processing of column value"


class TestTimestampMethods(object):
    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), [
        (              -1,              -1),
        (               0,               0),
        (               1,               1),
        (            None,            None),
        (   5049688276000,   5049688276000),
        (   3469296996000,   3469296996000),
        (   7405162940000,   7405162940000),
        (   2769601503000,   2769601503000),
        ( "2769601503000",   2769601503000),
        (  -1429593275000,  -1429593275000),
        (   3412549669000,   3412549669000),
    ])
    # fmt: on
    def test_generate_select_expression_for_timestamp_ms_to_ms(self, input_value, value, spark_session, spark_context):
        source_key, name = "updated_at", "updated_at_ms"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        result_column = custom_types._generate_select_expression_for_timestamp_ms_to_ms(
            source_column=input_df["attributes"]["data"][source_key], name=name
        )
        output_df = input_df.select(result_column)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "long", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), fixtures_for_timestamp_ms_to_s)
    # fmt: on
    def test_generate_select_expression_for_timestamp_ms_to_s(self, input_value, value, spark_session, spark_context):
        source_key, name = "updated_at", "updated_at_ms"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        result_column = custom_types._generate_select_expression_for_timestamp_ms_to_s(
            source_column=input_df["attributes"]["data"][source_key], name=name
        )
        output_df = input_df.select(result_column)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "long", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), fixtures_for_timestamp_s_to_ms)
    # fmt: on
    def test_generate_select_expression_for_timestamp_s_to_ms(self, input_value, value, spark_session, spark_context):
        source_key, name = "updated_at", "updated_at_ms"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        result_column = custom_types._generate_select_expression_for_timestamp_s_to_ms(
            source_column=input_df["attributes"]["data"][source_key], name=name
        )
        output_df = input_df.select(result_column)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "long", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), [
        (           1,            1),
        (           0,            0),
        (          -1,           -1),
        (        None,         None),
        (  4102358400,   4102358400),
        (  5049688276,   5049688276),
        (  3469296996,   3469296996),
        (  7405162940,   7405162940),
        (  2769601503,   2769601503),
        ( -1429593275,  -1429593275),
        (  3412549669,   3412549669),
        ("2769601503",   2769601503),
    ])
    # fmt: on
    def test_generate_select_expression_for_timestamp_s_to_s(self, input_value, value, spark_session, spark_context):
        source_key, name = "updated_at", "updated_at_ms"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)
        result_column = custom_types._generate_select_expression_for_timestamp_s_to_s(
            source_column=input_df["attributes"]["data"][source_key], name=name
        )
        output_df = input_df.select(result_column)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "long", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    # fmt: off
    @pytest.mark.parametrize(
        argnames="input_value, expected_value",
        argvalues=[
            (1591627696951, "2020-06-08 14:48:16.951"),
            (0, "1970-01-01 00:00:00"),
            (-1, "1969-12-31 23:59:59"),
            (1, "1970-01-01 00:00:01"),
        ]
    )
    # fmt: on
    def test_generate_select_expression_for_unix_timestamp_ms_to_spark_timestamp(self, input_value, expected_value, spark_session):
        input_df = spark_session.createDataFrame(
            [Row(input_column=input_value)], schema=T.StructType([T.StructField("input_column", T.LongType(), True)])
        )
        output_df = Mapper(
            mapping=[("output_column", "input_column", "unix_timestamp_ms_to_spark_timestamp")]
        ).transform(input_df)
        assert output_df.select(F.col("output_column").cast("string")).first().output_column == expected_value, "Processing of column value"
        assert output_df.schema.fieldNames() == ["output_column"], "Renaming of column"
        assert output_df.schema["output_column"].dataType.typeName() == "timestamp", "Casting of column"


class TestAddCustomDataTypeInRuntime(object):
    @staticmethod
    def _generate_select_expression_for_HelloWorld(source_column, name):
        from pyspark.sql import types as sql_types
        from pyspark.sql.functions import udf

        def _to_hello_world(col):
            if not col:
                return None
            else:
                return "Hello World"

        udf_hello_world = udf(_to_hello_world, sql_types.StringType())
        return udf_hello_world(source_column).alias(name)

    def test_custom_data_type_is_added(self, mocker):
        source_column, name = "element.key", "element_key"
        custom_types.add_custom_data_type(
            function_name="_generate_select_expression_for_HelloWorld",
            func=self._generate_select_expression_for_HelloWorld,
        )

        mocked_function = mocker.patch.object(custom_types, "_generate_select_expression_for_HelloWorld")
        custom_types._get_select_expression_for_custom_type(source_column, name, "HelloWorld")

        mocked_function.assert_called_once_with(source_column, name)

    # fmt: off
    @pytest.mark.parametrize(("input_value", "value"), [
        ("Some other string", "Hello World"),
        ("",                  None),
        (None,                None),
        (" ",                 "Hello World"),
        (100,                 "Hello World"),
        (0,                   None)
    ])
    # fmt: on
    def test_custom_data_type_is_applied(self, input_value, value, spark_session, spark_context):
        custom_types.add_custom_data_type(
            function_name="_generate_select_expression_for_HelloWorld",
            func=self._generate_select_expression_for_HelloWorld,
        )

        source_key, name, data_type = "key_name", "key", "HelloWorld"
        input_df = get_input_df(spark_session, spark_context, source_key, input_value)

        result_column = custom_types._get_select_expression_for_custom_type(
            source_column=input_df["attributes"]["data"][source_key], name=name, data_type=data_type
        )

        output_df = input_df.select(result_column)

        assert output_df.schema.fieldNames() == [name], "Renaming of column"
        assert output_df.schema[name].dataType.typeName() == "string", "Casting of column"
        assert output_df.first()[name] == value, "Processing of column value"

    def test_multiple_columns_are_accessed(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                Row(first_name="David", last_name="Eigenstuhler"),
                Row(first_name="Katharina", last_name="Hohensinn"),
                Row(first_name="Nora", last_name="Hohensinn"),
            ]
        )
        input_values = input_df.rdd.map(lambda x: x.asDict()).collect()
        expected_values = [d["first_name"] + "_" + d["last_name"] for d in input_values]

        def _first_and_last_name(source_column, name):
            return F.concat_ws("_", source_column, F.col("last_name")).alias(name)

        custom_types.add_custom_data_type(function_name="fullname", func=_first_and_last_name)

        output_df = Mapper([("full_name", "first_name", "fullname")]).transform(input_df)

        output_values = output_df.rdd.map(lambda x: x.asDict()["full_name"]).collect()

        assert expected_values == output_values

    def test_function_name_is_shortened(self, spark_session):
        input_df = spark_session.createDataFrame(
            [
                Row(first_name="David"),
                Row(first_name="Katharina"),
                Row(first_name="Nora"),
            ]
        )
        input_values = input_df.rdd.map(lambda x: x.asDict()["first_name"]).collect()
        expected_values = [fn.lower() for fn in input_values]

        def _lowercase(source_column, name):
            return F.lower(source_column).alias(name)

        custom_types.add_custom_data_type(function_name="lowercase", func=_lowercase)

        output_df = Mapper([("first_name", "first_name", "lowercase")]).transform(input_df)
        output_values = output_df.rdd.map(lambda x: x.asDict()["first_name"]).collect()

        assert expected_values == output_values
