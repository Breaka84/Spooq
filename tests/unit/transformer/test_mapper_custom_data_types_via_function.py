import json
import IPython
import pytest
from chispa import assert_df_equality
import datetime
from pyspark.sql import Row
from pyspark.sql import functions as F, types as T

from spooq.transformer import mapper_transformations as spq
from spooq.transformer import Mapper
from ...data.test_fixtures.mapper_custom_data_types_fixtures import (
    get_ids_for_fixture,
    fixtures_for_spark_sql_object,
    fixtures_for_as_is,
    fixtures_for_json_string,
    fixtures_for_timestamp_ms_to_s,
    fixtures_for_timestamp_s_to_ms,
    fixtures_for_has_value,
    fixtures_for_extended_string_to_int,
    fixtures_for_extended_string_to_long,
    fixtures_for_extended_string_to_float,
    fixtures_for_extended_string_to_double,
    fixtures_for_extended_string_to_boolean,
    fixtures_for_extended_string_to_timestamp_spark2,
    fixtures_for_extended_string_unix_timestamp_ms_to_timestamp_spark2,
    fixtures_for_extended_string_to_date_spark2,
    fixtures_for_extended_string_unix_timestamp_ms_to_date_spark2,
    fixtures_for_extended_string_to_timestamp,
    fixtures_for_extended_string_unix_timestamp_ms_to_timestamp,
    fixtures_for_extended_string_to_date,
    fixtures_for_extended_string_unix_timestamp_ms_to_date,
)


@pytest.fixture()
def input_df(request, spark_session, spark_context):
    input_json = json.dumps({"attributes": {"data": {"some_attribute": request.param}}})
    return spark_session.read.json(spark_context.parallelize([input_json]))


@pytest.fixture()
def expected_df(request, spark_session, spark_context):
    input_json = json.dumps({"mapped_name": request.param})
    return spark_session.read.json(spark_context.parallelize([input_json]))


class TestAdHocSparkSqlFunctions:
    @staticmethod
    def get_spark_data_type(input_value):
        return {
            "str": T.StringType(),
            "int": T.LongType(),
            "bool": T.BooleanType(),
            "float": T.DoubleType(),
            "NoneType": T.NullType(),
        }[type(input_value).__name__]

    @pytest.mark.parametrize(
        argnames=("input_value_1", "input_value_2", "mapper_function", "expected_value"),
        argvalues=fixtures_for_spark_sql_object,
    )
    def test_spark_sql_object(self, spark_session, input_value_1, input_value_2, mapper_function, expected_value):
        spark_schema = T.StructType(
            [
                T.StructField(
                    "nested",
                    T.StructType(
                        [
                            T.StructField("input_key_1", self.get_spark_data_type(input_value_1)),
                            T.StructField("input_key_2", self.get_spark_data_type(input_value_2)),
                        ]
                    ),
                )
            ]
        )
        input_df = spark_session.createDataFrame(
            data=[Row(nested=Row(input_key_1=input_value_1, input_key_2=input_value_2))], schema=spark_schema
        )
        output_df = Mapper(mapping=[("output_key", mapper_function, spq.as_is)]).transform(input_df)
        actual = output_df.first().output_key
        if isinstance(expected_value, datetime.datetime):
            assert (expected_value - datetime.timedelta(seconds=30)) < actual < datetime.datetime.now()
        else:
            assert actual == expected_value


class TestGenericFunctionality:

    @pytest.fixture(scope="class")
    def input_df(self, spark_session):
        return spark_session.createDataFrame([
            Row(
                str_1=None, str_2="Hello",
                str_bool_1=None, str_bool_2="True",
                int_1=None, int_2=1637335255,
                float_1=None, float_2=1.80,
                str_int_1=None, str_int_2="1637335255",
                str_ts_1=None, str_ts_2="2020-08-12 12:43:14",
                int_date_1=None, int_date_2=20201007,
                str_array_1=None, str_array_2="1,2,3",
                str_key_1=None, str_key_2="Y")
            ],
            schema=(
                "str_1 STRING, str_2 STRING, "
                "str_bool_1 STRING, str_bool_2 STRING, "
                "int_1 LONG, int_2 LONG, "
                "float_1 FLOAT, float_2 FLOAT, "
                "str_int_1 STRING, str_int_2 STRING, "
                "str_ts_1 STRING, str_ts_2 STRING, "
                "int_date_1 LONG, int_date_2 LONG, "
                "str_array_1 STRING, str_array_2 STRING, "
                "str_key_1 STRING, str_key_2 STRING")
        )

    def test_different_syntax_options(self, input_df, spark_session):
        # fmt:off
        mapping = [
            ("function",                  "str_2",        spq.as_is),
            ("function_call",             "str_2",        spq.as_is()),
            ("function_call_with_params", "str_2",        spq.as_is(output_type=T.StringType())),
            ("function_as_source",        F.col("str_2"), spq.as_is),
            ("literal_as_source",         F.lit("Hi!"),   T.StringType()),
        ]
        # fmt:on

        expected_df = spark_session.createDataFrame([
            Row(
                function="Hello",
                function_call="Hello",
                function_call_with_params="Hello",
                function_as_source="Hello",
                literal_as_source="Hi!"
                )
            ],
            schema=(
                "function STRING, "
                "function_call STRING, "
                "function_call_with_params STRING, "
                "function_as_source STRING, "
                "literal_as_source STRING"
                )
        )

        output_df = Mapper(mapping).transform(input_df)
        assert_df_equality(expected_df, output_df, ignore_nullable=True)

    def test_alternative_source_columns(self, spark_session, input_df):
        # fmt:off
        mapping = [
            ("as_is",                "str_1",        spq.as_is(alt_src_cols="str_2")),
            ("unix_to_unix",         "int_1",        spq.unix_timestamp_to_unix_timestamp(alt_src_cols="int_2")),
            ("first_of_month",       "str_ts_1",     spq.spark_timestamp_to_first_of_month(alt_src_cols="str_ts_2")),
            ("m_to_cm",              "float_1",      spq.meters_to_cm(alt_src_cols="float_2")),
            ("has_val",              "int_1",        spq.has_value(alt_src_cols="int_2")),
            ("str_to_num",           "str_int_1",    spq.extended_string_to_number(alt_src_cols="str_int_2")),
            ("str_to_bool",          "str_bool_1",   spq.extended_string_to_boolean(alt_src_cols="str_bool_2")),
            ("str_to_timestamp",     "str_ts_1",     spq.extended_string_to_timestamp(alt_src_cols="str_ts_2",
                                                                                      date_format="yyyy-MM-dd HH:mm")),
            ("custom_to_timestamp",  "int_date_1",   spq.custom_time_format_to_timestamp(alt_src_cols="int_date_2",
                                                                                         input_format="yyyyMMdd",
                                                                                         output_type=T.StringType())),
            ("str_to_array",         "str_array_1",  spq.string_to_array(alt_src_cols="str_array_2")),
            ("apply_func",           "str_1",        spq.apply_function(alt_src_cols="str_2", func=F.lower)),
            ("map_vals",             "str_key_1",    spq.map_values(alt_src_cols="str_key_2", mapping={"Y": "Yes"})),
        ]
        # fmt:on

        expected_df = spark_session.createDataFrame(
            [
                Row(
                    as_is="Hello",
                    unix_to_unix=1637335,
                    first_of_month=datetime.date(2020, 8, 1),
                    m_to_cm=180,
                    has_val=True,
                    str_to_num=1637335255,
                    str_to_bool=True,
                    str_to_timestamp="2020-08-12 12:43",
                    custom_to_timestamp="2020-10-07 00:00:00",
                    str_to_array=[1, 2, 3],
                    apply_func="hello",
                    map_vals="Yes",
                )
            ],
            schema=(
                "as_is STRING, "
                "unix_to_unix LONG, "
                "first_of_month DATE, "
                "m_to_cm INTEGER, "
                "has_val BOOLEAN, "
                "str_to_num LONG, "
                "str_to_bool BOOLEAN, "
                "str_to_timestamp STRING, "
                "custom_to_timestamp STRING, "
                "str_to_array ARRAY<STRING>, "
                "apply_func STRING, "
                "map_vals STRING"
            )
        )

        output_df = Mapper(mapping).transform(input_df)
        assert_df_equality(expected_df, output_df, ignore_nullable=True)

    def test_output_type_casting(self, input_df):
        # fmt:off
        mapping = [
            ("as_is",                "str_2",        spq.as_is(output_type=T.StringType())),
            ("unix_to_unix",         "int_2",        spq.unix_timestamp_to_unix_timestamp(output_type=T.StringType())),
            ("first_of_month",       "str_ts_2",     spq.spark_timestamp_to_first_of_month(output_type=T.StringType())),
            ("m_to_cm",              "float_2",      spq.meters_to_cm(output_type=T.StringType())),
            ("has_val",              "int_2",        spq.has_value(output_type=T.StringType())),
            ("str_to_num",           "str_int_2",    spq.extended_string_to_number(output_type=T.StringType())),
            ("str_to_bool",          "str_bool_2",   spq.extended_string_to_boolean(output_type=T.StringType())),
            ("str_to_timestamp",     "str_ts_2",     spq.extended_string_to_timestamp(output_type=T.StringType(),
                                                                                      date_format="yyyy-MM-dd HH:mm")),
            ("custom_to_timestamp",  "int_date_2",   spq.custom_time_format_to_timestamp(output_type=T.StringType(),
                                                                                         input_format="yyyyMMdd")),
            ("str_to_array",         "str_array_2",  spq.string_to_array(output_type=T.StringType())),
            ("apply_func",           "str_2",        spq.apply_function(output_type=T.StringType(), func=F.lower)),
            ("map_vals",             "str_key_2",    spq.map_values(output_type=T.StringType(), mapping={"Y": "Yes"})),
        ]
        # fmt:on

        output_df = Mapper(mapping).transform(input_df)
        for col in output_df.schema.fields:
            if "array" in col.name:
                assert col.jsonValue()["type"]["elementType"] == "string"
            else:
                assert isinstance(col.dataType, T.StringType)


class TestToJsonString:
    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_json_string,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_json_string),
    )
    def test_to_json_string(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.to_json_string())]
        output_df = Mapper(mapping).transform(input_df)
        assert_df_equality(expected_df, output_df)

    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_json_string,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_json_string),
    )
    def test_if_out_of_the_box_function_behaves_the_same(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.apply_function(func=F.to_json))]
        input_value = input_df.first().attributes.data.asDict(True)["some_attribute"]
        if isinstance(input_value, (type(None), str)):
            pytest.xfail("Not supported by pyspark's `to_json` function")

        output_df = Mapper(mapping).transform(input_df)
        assert_df_equality(
            expected_df.select(F.regexp_replace(F.col("mapped_name"), " ", "").alias("mapped_name")), output_df
        )


class TestUnixTimestampToUnixTimestamp:
    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_timestamp_ms_to_s,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_timestamp_ms_to_s),
    )
    def test_unix_timestamp_ms_to_s(self, input_df, expected_df):
        mapping = [(
            "mapped_name",
            "attributes.data.some_attribute",
            spq.unix_timestamp_to_unix_timestamp(input_time_unit="ms", output_time_unit="sec")
        )]
        output_df = Mapper(mapping).transform(input_df)
        expected_df = expected_df.select(F.col("mapped_name").alias("mapped_name").cast(T.LongType()))
        assert_df_equality(expected_df, output_df)

    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_timestamp_s_to_ms,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_timestamp_s_to_ms),
    )
    def test_unix_timestamp_s_to_ms(self, input_df, expected_df):
        mapping = [(
            "mapped_name",
            "attributes.data.some_attribute",
            spq.unix_timestamp_to_unix_timestamp(input_time_unit="sec", output_time_unit="ms")
        )]
        output_df = Mapper(mapping).transform(input_df)
        expected_df = expected_df.select(F.col("mapped_name").alias("mapped_name").cast(T.LongType()))
        assert_df_equality(expected_df, output_df)


class TestSparkTimestampToFirstOfMonth:
    pass


class TestMetersToCm:
    pass


class TestHasValue:
    pass


class TestExtendedStringToNumber:
    pass


class TestExtendedStringToBoolean:
    pass


class TestExtendedStringToTimestamp:
    pass


class TestCustomTimeFormatToTimestamp:
    pass


class TestStringToArray:
    pass


class TestApplyFunction:
    pass


class TestMapValues:
    pass


# class TestMiscConversions(object):
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         ("only some text",
#          "only some text"),
#         (None,
#          None),
#         ({"key": "value"},
#          Row(key="value")),
#         ({"key": {"other_key": "value"}},
#          Row(key=Row(other_key="value"))),
#         ({"age": 18,"weight": 75},
#          Row(age=18, weight=75)),
#         ({"list_of_friend_ids": [12, 75, 44, 76]},
#          Row(list_of_friend_ids=[12, 75, 44, 76])),
#         ([{"weight": "75"}, {"weight": "76"}, {"weight": "73"}],
#          [Row(weight="75"), Row(weight="76"), Row(weight="73")]),
#         ({"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]},
#          Row(list_of_friend_ids=[Row(id=12), Row(id=75), Row(id=44), Row(id=76)])),
#     ])
#     # fmt: on
#     def test_generate_select_expression_without_casting(self, input_value, value, spark_session, spark_context):
#         source_key, name = "demographics", "statistics"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_without_casting(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         ("only some text",
#         "only some text"),
#         (None,
#          None),
#         ({"key": "value"},
#          '{"key": "value"}'),
#         ({"key": {"other_key": "value"}},
#          '{"key": {"other_key": "value"}}'),
#         ({"age": 18, "weight": 75},
#          '{"age": 18, "weight": 75}'),
#         ({"list_of_friend_ids": [12, 75, 44, 76]},
#          '{"list_of_friend_ids": [12, 75, 44, 76]}'),
#         ([{"weight": "75"}, {"weight": "76"}, {"weight": "73"}],
#          '[{"weight": "75"}, {"weight": "76"}, {"weight": "73"}]'),
#         ({"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]},
#          '{"list_of_friend_ids": [{"id": 12}, {"id": 75}, {"id": 44}, {"id": 76}]}')
#     ])
#     # fmt: on
#     def test_generate_select_expression_for_json_string(self, input_value, value, spark_session, spark_context):
#         source_key, name = "demographics", "statistics"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_for_json_string(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "string", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=[
#             (1.80,    180),
#             (2.,      200),
#             (-1.0,   -100),
#             (0.0,       0),
#             (0,         0),
#             (2,       200),
#             (-4,     -400),
#             ("1.80",  180),
#             ("2.",    200),
#             ("-1.0", -100),
#             ("0.0",     0),
#             (None,   None),
#             ("one",  None),
#         ])
#     # fmt: on
#     def test_generate_select_expression_for_meters_to_cm(self, input_value, expected_value, spark_session):
#         input_df = spark_session.createDataFrame(
#             data=[Row(input_key=input_value)],
#             schema=T.StructType([T.StructField("input_key", get_spark_data_type(input_value), True)]),
#         )
#         output_df = Mapper(mapping=[("output_column", "input_key", "meters_to_cm")]).transform(input_df)
#         assert output_df.first().output_column == expected_value, "Processing of column value"
#         assert output_df.schema.fieldNames() == ["output_column"], "Renaming of column"
#         assert output_df.schema["output_column"].dataType.typeName() == "integer", "Casting of column"
#
#     @pytest.mark.parametrize(argnames=("input_value", "expected_value"), argvalues=fixtures_for_has_value)
#     def test_generate_select_expression_for_has_value(self, input_value, expected_value, spark_session):
#         input_df = spark_session.createDataFrame(
#             data=[Row(input_key=input_value)],
#             schema=T.StructType([T.StructField("input_key", get_spark_data_type(input_value), True)]),
#         )
#         output_df = Mapper(mapping=[("output_column", "input_key", "has_value")]).transform(input_df)
#         assert output_df.first().output_column == expected_value, "Processing of column value"
#         assert output_df.schema.fieldNames() == ["output_column"], "Renaming of column"
#         assert output_df.schema["output_column"].dataType.typeName() == "boolean", "Casting of column"
#
#
# class TestExtendedStringConversions(object):
#     @staticmethod
#     def create_input_df(input_value, spark_session):
#         return spark_session.createDataFrame(
#             data=[Row(input_key=input_value)],
#             schema=T.StructType([T.StructField("input_key", get_spark_data_type(input_value), True)]),
#         )
#
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_to_int,
#         ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_extended_string_to_int],
#     )
#     def test_extended_string_to_int(self, spark_session, input_value, expected_value):
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_int")]).transform(input_df)
#         assert output_df.first().output_key == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.IntegerType)
#
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_to_long,
#         ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_extended_string_to_long],
#     )
#     def test_extended_string_to_long(self, spark_session, input_value, expected_value):
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_long")]).transform(input_df)
#         assert output_df.first().output_key == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.LongType)
#
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_to_float,
#         ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_extended_string_to_float],
#     )
#     def test_extended_string_to_float(self, spark_session, input_value, expected_value):
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_float")]).transform(input_df)
#         actual_value = output_df.first().output_key
#         if actual_value is not None:
#             assert pytest.approx(actual_value) == expected_value
#         else:
#             assert actual_value == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.FloatType)
#
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_to_double,
#         ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_extended_string_to_double],
#     )
#     def test_extended_string_to_double(self, spark_session, input_value, expected_value):
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_double")]).transform(input_df)
#         actual_value = output_df.first().output_key
#         if actual_value is not None:
#             assert pytest.approx(actual_value) == expected_value
#         else:
#             assert actual_value == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.DoubleType)
#
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_to_boolean,
#         ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_extended_string_to_boolean],
#     )
#     def test_extended_string_to_boolean(self, spark_session, input_value, expected_value):
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_boolean")]).transform(input_df)
#         assert output_df.first().output_key == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.BooleanType)
#
#     @only_spark2
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_to_timestamp_spark2,
#         ids=[
#             parameters_to_string_id(actual, expected)
#             for actual, expected in fixtures_for_extended_string_to_timestamp_spark2
#         ],
#     )
#     def test_extended_string_to_timestamp_spark2(self, spark_session, input_value, expected_value):
#         # test uses timezone set to GMT / UTC (pytest.ini)!
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_timestamp")]).transform(input_df)
#         # workaround via pandas necessary due to bug with direct conversion
#         # to python datetime wrt timezone conversions (https://issues.apache.org/jira/browse/SPARK-32123)
#         try:
#             output_pd_df = output_df.toPandas()
#             actual_value = output_pd_df.iloc[0]["output_key"].to_pydatetime()
#         except ValueError:
#             # If input is in milliseconds it will still be stored in the DF but cannot be collected in Python
#             actual_value = "out_of_range_for_python"
#         except AttributeError:
#             # `.to_pydatetime()` can only be used on datetimes and throws AttributeErrors on other objects / None
#             actual_value = None
#         assert actual_value == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.TimestampType)
#
#     @only_spark3
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_to_timestamp,
#         ids=[
#             parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_extended_string_to_timestamp
#         ],
#     )
#     def test_extended_string_to_timestamp(self, spark_session, input_value, expected_value):
#         # test uses timezone set to GMT / UTC (pytest.ini)!
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_timestamp")]).transform(input_df)
#         # workaround via pandas necessary due to bug with direct conversion
#         # to python datetime wrt timezone conversions (https://issues.apache.org/jira/browse/SPARK-32123)
#         output_pd_df = output_df.toPandas()
#         output_value = output_pd_df.iloc[0]["output_key"]
#         if isinstance(output_value, type(pd.NaT)):
#             actual_value = None
#         else:
#             actual_value = output_value.to_pydatetime()
#         assert actual_value == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.TimestampType)
#
#     @only_spark2
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_unix_timestamp_ms_to_timestamp_spark2,
#         ids=[
#             parameters_to_string_id(actual, expected)
#             for actual, expected in fixtures_for_extended_string_unix_timestamp_ms_to_timestamp_spark2
#         ],
#     )
#     def test_extended_string_unix_timestamp_ms_to_timestamp_spark2(self, spark_session, input_value, expected_value):
#         # test uses timezone set to GMT / UTC (pytest.ini)!
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(
#             mapping=[("output_key", "input_key", "extended_string_unix_timestamp_ms_to_timestamp")]
#         ).transform(input_df)
#         # workaround via pandas necessary due to bug with direct conversion
#         # to python datetime wrt timezone conversions (https://issues.apache.org/jira/browse/SPARK-32123)
#         try:
#             output_pd_df = output_df.toPandas()
#             actual_value = output_pd_df.iloc[0]["output_key"].to_pydatetime()
#             assert (
#                 actual_value.toordinal() == expected_value.toordinal(),
#                 "actual_value: {act_val}, expected value: {expected_val}".format(
#                     act_val=actual_value, expected_val=expected_value
#                 ),
#             )
#         except AttributeError:
#             # `.to_pydatetime()` can only be used on datetimes and throws AttributeErrors on None
#             assert expected_value is None
#         assert isinstance(output_df.schema["output_key"].dataType, T.TimestampType)
#
#     @only_spark3
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_unix_timestamp_ms_to_timestamp,
#         ids=[
#             parameters_to_string_id(actual, expected)
#             for actual, expected in fixtures_for_extended_string_unix_timestamp_ms_to_timestamp
#         ],
#     )
#     def test_extended_string_unix_timestamp_ms_to_timestamp(self, spark_session, input_value, expected_value):
#         # test uses timezone set to GMT / UTC (pytest.ini)!
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(
#             mapping=[("output_key", "input_key", "extended_string_unix_timestamp_ms_to_timestamp")]
#         ).transform(input_df)
#         # workaround via pandas necessary due to bug with direct conversion
#         # to python datetime wrt timezone conversions (https://issues.apache.org/jira/browse/SPARK-32123)
#         output_pd_df = output_df.toPandas()
#         output_value = output_pd_df.iloc[0]["output_key"]
#         if isinstance(output_value, type(pd.NaT)):
#             actual_value = None
#         else:
#             actual_value = output_value.to_pydatetime().toordinal()
#             expected_value = expected_value.toordinal()
#         assert actual_value == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.TimestampType)
#
#     @only_spark2
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_to_date_spark2,
#         ids=[
#             parameters_to_string_id(actual, expected)
#             for actual, expected in fixtures_for_extended_string_to_date_spark2
#         ],
#     )
#     def test_extended_string_to_date_spark2(self, spark_session, input_value, expected_value):
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_date")]).transform(input_df)
#         try:
#             actual_value = output_df.first().output_key
#         except ValueError:
#             # If input is in milliseconds it will still be stored in the DF but cannot be collected in Python
#             actual_value = "out_of_range_for_python"
#         assert actual_value == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.DateType)
#
#     @only_spark3
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_to_date,
#         ids=[parameters_to_string_id(actual, expected) for actual, expected in fixtures_for_extended_string_to_date],
#     )
#     def test_extended_string_to_date(self, spark_session, input_value, expected_value):
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(mapping=[("output_key", "input_key", "extended_string_to_date")]).transform(input_df)
#         actual_value = output_df.first().output_key
#         assert actual_value == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.DateType)
#
#     @only_spark2
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_unix_timestamp_ms_to_date_spark2,
#         ids=[
#             parameters_to_string_id(actual, expected)
#             for actual, expected in fixtures_for_extended_string_unix_timestamp_ms_to_date_spark2
#         ],
#     )
#     def test_extended_string_unix_timestamp_ms_to_date_spark2(self, spark_session, input_value, expected_value):
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(
#             mapping=[("output_key", "input_key", "extended_string_unix_timestamp_ms_to_date")]
#         ).transform(input_df)
#         actual_value = output_df.first().output_key
#         assert actual_value == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.DateType)
#
#     @only_spark3
#     @pytest.mark.parametrize(
#         argnames=("input_value", "expected_value"),
#         argvalues=fixtures_for_extended_string_unix_timestamp_ms_to_date,
#         ids=[
#             parameters_to_string_id(actual, expected)
#             for actual, expected in fixtures_for_extended_string_unix_timestamp_ms_to_date
#         ],
#     )
#     def test_extended_string_unix_timestamp_ms_to_date(self, spark_session, input_value, expected_value):
#         input_df = self.create_input_df(input_value, spark_session)
#         output_df = Mapper(
#             mapping=[("output_key", "input_key", "extended_string_unix_timestamp_ms_to_date")]
#         ).transform(input_df)
#         actual_value = output_df.first().output_key
#         assert actual_value == expected_value
#         assert isinstance(output_df.schema["output_key"].dataType, T.DateType)
#
#
# class TestAnonymizingMethods(object):
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         ("my_first_mail@myspace.com", "1"),
#         ("",                          None),
#         (None,                        None),
#         (" ",                         "1"),
#         (100,                         "1"),
#         (0,                           "1")],
#     )
#     # fmt: on
#     def test_generate_select_expression_for_StringBoolean(self, input_value, value, spark_session, spark_context):
#         source_key, name = "email_address", "mail"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_for_StringBoolean(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "string", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         ("my_first_mail@myspace.com", None),
#         ("",                          None),
#         (None,                        None),
#         (" ",                         None),
#         (100,                         None),
#         (0,                           None)],
#     )
#     # fmt: on
#     def test_generate_select_expression_for_StringNull(self, input_value, value, spark_session, spark_context):
#         source_key, name = "email_address", "mail"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_for_StringNull(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "string", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         (12345,             1),
#         ("",                1),
#         ("some text",       1),
#         (None,              None),
#         (0,                 1),
#         (1,                 1),
#         (-1,                1),
#         (5445.23,           1),
#         (float("inf"),      1),
#         (-1 * float("inf"), 1)
#     ])
#     # fmt: on
#     def test_generate_select_expression_for_IntBoolean(self, input_value, value, spark_session, spark_context):
#         source_key, name = "user_id", "id"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_for_IntBoolean(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "integer", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         (12345,             None),
#         ("",                None),
#         ("some text",       None),
#         (None,              None),
#         (0,                 None),
#         (1,                 None),
#         (-1,                None),
#         (5445.23,           None),
#         (float("inf"),      None),
#         (-1 * float("inf"), None),
#     ])
#     # fmt: on
#     def test_generate_select_expression_for_IntNull(self, input_value, value, spark_session, spark_context):
#         source_key, name = "user_id", "id"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_for_IntNull(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "integer", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @only_spark2
#     @pytest.mark.parametrize(("input_value", "value"), [
#         (None,         None),
#         ("1955-09-41", None),
#         ("1969-04-03", "1969-04-01"),
#         ("1985-03-07", "1985-03-01"),
#         ("1998-06-10", "1998-06-01"),
#         ("1967-05-16", "1967-05-01"),
#         ("1953-01-01", "1953-01-01"),
#         ("1954-11-06", "1954-11-01"),
#         ("1978-09-05", "1978-09-01"),
#         ("1999-05-23", "1999-05-01"),
#     ])
#     # fmt: on
#     def test_generate_select_expression_for_TimestampMonth_spark2(
#         self, input_value, value, spark_session, spark_context
#     ):
#         source_key, name = "day_of_birth", "birthday"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         input_df = input_df.withColumn(
#             source_key, F.to_utc_timestamp(input_df["attributes"]["data"][source_key], "yyyy-MM-dd")
#         )
#         result_column = custom_types._generate_select_expression_for_TimestampMonth(
#             source_column=input_df[source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "timestamp", "Casting of column"
#         output_value = output_df.first()[name]
#         if output_value:
#             output_value = datetime.date.strftime(output_value, format("%Y-%m-%d"))
#         else:
#             output_value = None
#         assert output_value == value, "Processing of column value"
#
#     # fmt: off
#     @only_spark3
#     @pytest.mark.parametrize(("input_value", "value"), [
#         (None,         None),
#         ("1955-09-41", None),
#         ("1969-04-03", "1969-04-01"),
#         ("1985-03-07", "1985-03-01"),
#         ("1998-06-10", "1998-06-01"),
#         ("1967-05-16", "1967-05-01"),
#         ("1953-01-01", "1953-01-01"),
#         ("1954-11-06", "1954-11-01"),
#         ("1978-09-05", "1978-09-01"),
#         ("1999-05-23", "1999-05-01"),
#     ])
#     # fmt: on
#     def test_generate_select_expression_for_TimestampMonth(self, input_value, value, spark_session, spark_context):
#         source_key, name = "day_of_birth", "birthday"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         input_df = input_df.withColumn(source_key, input_df["attributes"]["data"][source_key].cast(T.DateType()))
#         result_column = custom_types._generate_select_expression_for_TimestampMonth(
#             source_column=input_df[source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "timestamp", "Casting of column"
#         output_value = output_df.first()[name]
#         if output_value:
#             output_value = datetime.date.strftime(output_value, format("%Y-%m-%d"))
#         else:
#             output_value = None
#         assert output_value == value, "Processing of column value"
#
#
# class TestTimestampMethods(object):
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         (0,              0),              # minimum valid timestamp
#         (-1,             None),           # minimum valid timestamp - 1 ms
#         (None,           None),
#         (4102358400000,  4102358400000),  # maximum valid timestamp
#         (4102358400001,  None),           # maximum valid timestamp + 1 ms
#         (5049688276000,  None),
#         (3469296996000,  3469296996000),
#         (7405162940000,  None),
#         (2769601503000,  2769601503000),
#         (-1429593275000, None),
#         (3412549669000,  3412549669000),
#     ])
#     # fmt: on
#     def test_generate_select_expression_for_timestamp_ms_to_ms(self, input_value, value, spark_session, spark_context):
#         source_key, name = "updated_at", "updated_at_ms"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_for_timestamp_ms_to_ms(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "long", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         (0,              0),           # minimum valid timestamp
#         (-1,             None),        # minimum valid timestamp - 1 ms
#         (None,           None),
#         (4102358400000,  4102358400),  # maximum valid timestamp
#         (4102358400001,  None),        # maximum valid timestamp + 1 ms
#         (5049688276000,  None),
#         (3469296996000,  3469296996),
#         (7405162940000,  None),
#         (2769601503000,  2769601503),
#         (-1429593275000, None),
#         (3412549669000,  3412549669),
#     ])
#     # fmt: on
#     def test_generate_select_expression_for_timestamp_ms_to_s(self, input_value, value, spark_session, spark_context):
#         source_key, name = "updated_at", "updated_at_ms"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_for_timestamp_ms_to_s(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "long", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         (0,           0),              # minimum valid timestamp
#         (-1,          None),           # minimum valid timestamp - 1 s
#         (None,        None),
#         (4102358400,  4102358400000),  # maximum valid timestamp
#         (4102358401,  None),           # maximum valid timestamp + 1 s
#         (5049688276,  None),
#         (3469296996,  3469296996000),
#         (7405162940,  None),
#         (2769601503,  2769601503000),
#         (-1429593275, None),
#         (3412549669,  3412549669000),
#     ])
#     # fmt: on
#     def test_generate_select_expression_for_timestamp_s_to_ms(self, input_value, value, spark_session, spark_context):
#         source_key, name = "updated_at", "updated_at_ms"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_for_timestamp_s_to_ms(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "long", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         (0,           0),           # minimum valid timestamp
#         (-1,          None),        # minimum valid timestamp - 1 s
#         (None,        None),
#         (4102358400,  4102358400),  # maximum valid timestamp
#         (4102358401,  None),        # maximum valid timestamp + 1 s
#         (5049688276,  None),
#         (3469296996,  3469296996),
#         (7405162940,  None),
#         (2769601503,  2769601503),
#         (-1429593275, None),
#         (3412549669,  3412549669),
#     ])
#     # fmt: on
#     def test_generate_select_expression_for_timestamp_s_to_s(self, input_value, value, spark_session, spark_context):
#         source_key, name = "updated_at", "updated_at_ms"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#         result_column = custom_types._generate_select_expression_for_timestamp_s_to_s(
#             source_column=input_df["attributes"]["data"][source_key], name=name
#         )
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "long", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     # fmt: off
#     @pytest.mark.parametrize(
#         argnames="input_value",
#         argvalues=[1591627696951, 0, -1, 1]
#     )
#     # fmt: on
#     def test_generate_select_expression_for_unix_timestamp_ms_to_spark_timestamp(self, input_value, spark_session):
#         input_df = spark_session.createDataFrame(
#             [Row(input_column=input_value)], schema=T.StructType([T.StructField("input_column", T.LongType(), True)])
#         )
#         output_df = Mapper(
#             mapping=[("output_column", "input_column", "unix_timestamp_ms_to_spark_timestamp")]
#         ).transform(input_df)
#         expected_value = datetime.datetime.fromtimestamp(input_value / 1000.0)
#         assert output_df.first().output_column == expected_value, "Processing of column value"
#         assert output_df.schema.fieldNames() == ["output_column"], "Renaming of column"
#         assert output_df.schema["output_column"].dataType.typeName() == "timestamp", "Casting of column"
#
#
# class TestAddCustomDataTypeInRuntime(object):
#     @staticmethod
#     def _generate_select_expression_for_HelloWorld(source_column, name):
#         from pyspark.sql import types as sql_types
#         from pyspark.sql.functions import udf
#
#         def _to_hello_world(col):
#             if not col:
#                 return None
#             else:
#                 return "Hello World"
#
#         udf_hello_world = udf(_to_hello_world, sql_types.StringType())
#         return udf_hello_world(source_column).alias(name)
#
#     def test_custom_data_type_is_added(self, mocker):
#         source_column, name = "element.key", "element_key"
#         custom_types.add_custom_data_type(
#             function_name="_generate_select_expression_for_HelloWorld",
#             func=self._generate_select_expression_for_HelloWorld,
#         )
#
#         mocked_function = mocker.patch.object(custom_types, "_generate_select_expression_for_HelloWorld")
#         custom_types._get_select_expression_for_custom_type(source_column, name, "HelloWorld")
#
#         mocked_function.assert_called_once_with(source_column, name)
#
#     # fmt: off
#     @pytest.mark.parametrize(("input_value", "value"), [
#         ("Some other string", "Hello World"),
#         ("",                  None),
#         (None,                None),
#         (" ",                 "Hello World"),
#         (100,                 "Hello World"),
#         (0,                   None)
#     ])
#     # fmt: on
#     def test_custom_data_type_is_applied(self, input_value, value, spark_session, spark_context):
#         custom_types.add_custom_data_type(
#             function_name="_generate_select_expression_for_HelloWorld",
#             func=self._generate_select_expression_for_HelloWorld,
#         )
#
#         source_key, name, data_type = "key_name", "key", "HelloWorld"
#         input_df = get_input_df(spark_session, spark_context, source_key, input_value)
#
#         result_column = custom_types._get_select_expression_for_custom_type(
#             source_column=input_df["attributes"]["data"][source_key], name=name, data_type=data_type
#         )
#
#         output_df = input_df.select(result_column)
#
#         assert output_df.schema.fieldNames() == [name], "Renaming of column"
#         assert output_df.schema[name].dataType.typeName() == "string", "Casting of column"
#         assert output_df.first()[name] == value, "Processing of column value"
#
#     def test_multiple_columns_are_accessed(self, spark_session):
#         input_df = spark_session.createDataFrame(
#             [
#                 Row(first_name="David", last_name="Eigenstuhler"),
#                 Row(first_name="Katharina", last_name="Hohensinn"),
#                 Row(first_name="Nora", last_name="Hohensinn"),
#             ]
#         )
#         input_values = input_df.rdd.map(lambda x: x.asDict()).collect()
#         expected_values = [d["first_name"] + "_" + d["last_name"] for d in input_values]
#
#         def _first_and_last_name(source_column, name):
#             return F.concat_ws("_", source_column, F.col("last_name")).alias(name)
#
#         custom_types.add_custom_data_type(function_name="fullname", func=_first_and_last_name)
#
#         output_df = Mapper([("full_name", "first_name", "fullname")]).transform(input_df)
#
#         output_values = output_df.rdd.map(lambda x: x.asDict()["full_name"]).collect()
#
#         assert expected_values == output_values
#
#     def test_function_name_is_shortened(self, spark_session):
#         input_df = spark_session.createDataFrame(
#             [
#                 Row(first_name="David"),
#                 Row(first_name="Katharina"),
#                 Row(first_name="Nora"),
#             ]
#         )
#         input_values = input_df.rdd.map(lambda x: x.asDict()["first_name"]).collect()
#         expected_values = [fn.lower() for fn in input_values]
#
#         def _lowercase(source_column, name):
#             return F.lower(source_column).alias(name)
#
#         custom_types.add_custom_data_type(function_name="lowercase", func=_lowercase)
#
#         output_df = Mapper([("first_name", "first_name", "lowercase")]).transform(input_df)
#         output_values = output_df.rdd.map(lambda x: x.asDict()["first_name"]).collect()
#
#         assert expected_values == output_values
