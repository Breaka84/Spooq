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
    fixtures_for_timestamp_to_first_of_month,
    fixtures_for_meters_to_cm,
    fixtures_for_has_value,
    fixtures_for_str_to_int,
    fixtures_for_str_to_long,
    fixtures_for_str_to_float,
    fixtures_for_str_to_double,
    fixtures_for_str_to_bool_default,
    fixtures_for_str_to_bool_true_values_added,
    fixtures_for_str_to_bool_false_values_added,
    fixtures_for_str_to_bool_additional_true_and_false_values,
    fixtures_for_str_to_bool_false_values_as_argument,
    fixtures_for_str_to_bool_true_values_as_argument,
    fixtures_for_str_to_bool_true_and_false_values_as_argument,
    fixtures_for_str_to_timestamp_custom_format,
    fixtures_for_str_to_timestamp_max_valid_timestamp,
    fixtures_for_custom_time_format_to_timestamp,
    fixtures_for_extended_string_to_timestamp_spark2,
    fixtures_for_extended_string_unix_timestamp_ms_to_timestamp_spark2,
    fixtures_for_extended_string_to_date_spark2,
    fixtures_for_extended_string_unix_timestamp_ms_to_date_spark2,
    fixtures_for_str_to_timestamp_default,
    fixtures_for_extended_string_unix_timestamp_ms_to_timestamp,
    fixtures_for_extended_string_to_date,
    fixtures_for_extended_string_unix_timestamp_ms_to_date,
)


@pytest.fixture()
def input_df(request, spark_session, spark_context):
    input_json = json.dumps({"attributes": {"data": {"some_attribute": request.param}}}, default=str)
    return spark_session.read.json(spark_context.parallelize([input_json]))


@pytest.fixture()
def expected_df(request, spark_session, spark_context):
    input_json = json.dumps({"mapped_name": request.param}, default=str)
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
            ("str_to_num",           "str_int_1",    spq.str_to_num(alt_src_cols="str_int_2")),
            ("str_to_bool",          "str_bool_1",   spq.str_to_bool(alt_src_cols="str_bool_2")),
            ("str_to_timestamp",     "str_ts_1",     spq.str_to_timestamp(alt_src_cols="str_ts_2",
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
            ("str_to_num",           "str_int_2",    spq.str_to_num(output_type=T.StringType())),
            ("str_to_bool",          "str_bool_2",   spq.str_to_bool(output_type=T.StringType())),
            ("str_to_timestamp",     "str_ts_2",     spq.str_to_timestamp(output_type=T.StringType(),
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

    def test_direct_call(self, spark_session, input_df):
        output_df = input_df.select(
            spq.as_is("str_2", "as_is"),
            spq.unix_timestamp_to_unix_timestamp("int_2", "unix_to_unix"),
            spq.spark_timestamp_to_first_of_month("str_ts_2", "first_of_month"),
            spq.meters_to_cm("float_2", "m_to_cm"),
            spq.has_value("int_2", "has_val"),
            spq.str_to_num("str_int_2", "str_to_num"),
            spq.str_to_bool("str_bool_2", "str_to_bool"),
            spq.str_to_timestamp("str_ts_2", "str_to_timestamp", date_format="yyyy-MM-dd HH:mm"),
            spq.custom_time_format_to_timestamp("int_date_2", "custom_to_timestamp", input_format="yyyyMMdd", output_type=T.StringType()),
            spq.string_to_array("str_array_2", "str_to_array"),
            spq.apply_function("str_2", "apply_func", func=F.lower),
            spq.map_values("str_key_2", "map_vals", mapping={"Y": "Yes"}),
        )

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

        assert_df_equality(expected_df, output_df, ignore_nullable=True)


class TestAsIs:
    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_as_is,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_as_is),
    )
    def test_as_is(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.as_is())]
        output_df = Mapper(mapping).transform(input_df)
        assert_df_equality(expected_df, output_df)


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
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.LongType()))
        assert_df_equality(expected_df_, output_df)

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
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.LongType()))
        assert_df_equality(expected_df_, output_df)


class TestSparkTimestampToFirstOfMonth:
    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_timestamp_to_first_of_month,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_timestamp_to_first_of_month),
    )
    def test_spark_timestamp_to_first_of_month(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.spark_timestamp_to_first_of_month())]
        output_df = Mapper(mapping).transform(input_df)
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.DateType()))
        assert_df_equality(expected_df_, output_df)


class TestMetersToCm:
    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_meters_to_cm,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_meters_to_cm),
    )
    def test_meters_to_cm(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.meters_to_cm())]
        output_df = Mapper(mapping).transform(input_df)
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.IntegerType()))
        assert_df_equality(expected_df_, output_df)


class TestHasValue:
    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_has_value,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_has_value),
    )
    def test_has_value(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.has_value())]
        output_df = Mapper(mapping).transform(input_df)
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.BooleanType()))
        assert_df_equality(expected_df_, output_df, ignore_nullable=True)


class TestStringToNumber:
    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_str_to_int,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_str_to_int),
    )
    def test_str_to_int(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_num(output_type=T.IntegerType()))]
        output_df = Mapper(mapping).transform(input_df)
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.IntegerType()))
        assert_df_equality(expected_df_, output_df)

    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_str_to_long,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_str_to_long),
    )
    def test_str_to_long(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_num(output_type=T.LongType()))]
        output_df = Mapper(mapping).transform(input_df)
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.LongType()))
        assert_df_equality(expected_df_, output_df)

    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_str_to_float,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_str_to_float),
    )
    def test_str_to_float(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_num(output_type=T.FloatType()))]
        output_df = Mapper(mapping).transform(input_df)
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.FloatType()))
        assert_df_equality(expected_df_, output_df)

    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_str_to_double,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_str_to_double),
    )
    def test_str_to_double(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_num(output_type=T.DoubleType()))]
        output_df = Mapper(mapping).transform(input_df)
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.DoubleType()))
        assert_df_equality(expected_df_, output_df)


class TestStringToBoolean:
        @pytest.mark.parametrize(
            argnames="input_df, expected_df",
            argvalues=fixtures_for_str_to_bool_default,
            indirect=["input_df", "expected_df"],
            ids=get_ids_for_fixture(fixtures_for_str_to_bool_default),
        )
        def test_str_to_bool(self, input_df, expected_df):
            mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_bool())]
            output_df = Mapper(mapping).transform(input_df)
            expected_df_ = expected_df.select(F.col("mapped_name").cast(T.BooleanType()))
            assert_df_equality(expected_df_, output_df)

        @pytest.mark.parametrize(
            argnames="input_df, expected_df",
            argvalues=fixtures_for_str_to_bool_true_values_added,
            indirect=["input_df", "expected_df"],
            ids=get_ids_for_fixture(fixtures_for_str_to_bool_true_values_added),
        )
        def test_str_to_bool_with_additional_true_values(self, input_df, expected_df):
            mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_bool(
                true_values=["sure", "OK"]
            ))]
            output_df = Mapper(mapping).transform(input_df)
            expected_df_ = expected_df.select(F.col("mapped_name").cast(T.BooleanType()))
            assert_df_equality(expected_df_, output_df)

        @pytest.mark.parametrize(
            argnames="input_df, expected_df",
            argvalues=fixtures_for_str_to_bool_false_values_added,
            indirect=["input_df", "expected_df"],
            ids=get_ids_for_fixture(fixtures_for_str_to_bool_false_values_added),
        )
        def test_str_to_bool_with_additional_false_values(self, input_df, expected_df):
            mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_bool(
                false_values=["nope", "NOK"]
            ))]
            output_df = Mapper(mapping).transform(input_df)
            expected_df_ = expected_df.select(F.col("mapped_name").cast(T.BooleanType()))
            assert_df_equality(expected_df_, output_df)

        @pytest.mark.parametrize(
            argnames="input_df, expected_df",
            argvalues=fixtures_for_str_to_bool_additional_true_and_false_values,
            indirect=["input_df", "expected_df"],
            ids=get_ids_for_fixture(fixtures_for_str_to_bool_additional_true_and_false_values),
        )
        def test_str_to_bool_with_additional_true_and_false_values(self, input_df, expected_df):
            mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_bool(
                true_values=["sure", "OK"],
                false_values=["nope", "NOK"],
            ))]
            output_df = Mapper(mapping).transform(input_df)
            expected_df_ = expected_df.select(F.col("mapped_name").cast(T.BooleanType()))
            assert_df_equality(expected_df_, output_df)

        @pytest.mark.parametrize(
            argnames="input_df, expected_df",
            argvalues=fixtures_for_str_to_bool_true_values_as_argument,
            indirect=["input_df", "expected_df"],
            ids=get_ids_for_fixture(fixtures_for_str_to_bool_true_values_as_argument),
        )
        def test_str_to_bool_with_alternative_true_values(self, input_df, expected_df):
            mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_bool(
                true_values=["sure", "OK"],
                replace_default_values=True,
            ))]
            output_df = Mapper(mapping).transform(input_df)
            expected_df_ = expected_df.select(F.col("mapped_name").cast(T.BooleanType()))
            assert_df_equality(expected_df_, output_df)

        @pytest.mark.parametrize(
            argnames="input_df, expected_df",
            argvalues=fixtures_for_str_to_bool_false_values_as_argument,
            indirect=["input_df", "expected_df"],
            ids=get_ids_for_fixture(fixtures_for_str_to_bool_false_values_as_argument),
        )
        def test_str_to_bool_with_additional_false_values(self, input_df, expected_df):
            mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_bool(
                false_values=["nope", "NOK"],
                replace_default_values=True,
            ))]
            output_df = Mapper(mapping).transform(input_df)
            expected_df_ = expected_df.select(F.col("mapped_name").cast(T.BooleanType()))
            assert_df_equality(expected_df_, output_df)

        @pytest.mark.parametrize(
            argnames="input_df, expected_df",
            argvalues=fixtures_for_str_to_bool_true_and_false_values_as_argument,
            indirect=["input_df", "expected_df"],
            ids=get_ids_for_fixture(fixtures_for_str_to_bool_true_and_false_values_as_argument),
        )
        def test_str_to_bool_with_additional_true_and_false_values(self, input_df, expected_df):
            mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_bool(
                true_values=["sure", "OK"],
                false_values=["nope", "NOK"],
                replace_default_values=True,
            ))]
            output_df = Mapper(mapping).transform(input_df)
            expected_df_ = expected_df.select(F.col("mapped_name").cast(T.BooleanType()))
            assert_df_equality(expected_df_, output_df)


class TestStringToTimestamp:
    @pytest.mark.parametrize(
        argnames="input_df, expected_df",
        argvalues=fixtures_for_str_to_timestamp_default,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_str_to_timestamp_default),
    )
    def test_str_to_timestamp_default(self, input_df, expected_df):
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_timestamp())]
        output_df = Mapper(mapping).transform(input_df)
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.TimestampType()))
        assert_df_equality(expected_df_, output_df)

    @pytest.mark.parametrize(
        argnames="date_format, expected_string",
        argvalues=fixtures_for_str_to_timestamp_custom_format,
        ids=get_ids_for_fixture(fixtures_for_str_to_timestamp_custom_format),
    )
    def test_str_to_timestamp_custom_format(self, spark_session, date_format, expected_string):
        input_df = spark_session.createDataFrame([
            Row(attributes=Row(data=Row(some_attribute="2020-12-24 20:07:35.253")))
        ])
        mapping = [("mapped_name", "attributes.data.some_attribute", spq.str_to_timestamp(date_format=date_format))]
        output_df = Mapper(mapping).transform(input_df)
        expected_df = spark_session.createDataFrame([Row(mapped_name=expected_string)])
        assert_df_equality(expected_df, output_df)

    @pytest.mark.parametrize(
        argnames="max_valid_timestamp, expected_timestamp",
        argvalues=fixtures_for_str_to_timestamp_max_valid_timestamp,
        ids=get_ids_for_fixture(fixtures_for_str_to_timestamp_max_valid_timestamp),
    )
    def test_str_to_timestamp_max_valid_timestamp(self, spark_session, max_valid_timestamp, expected_timestamp):
        input_df = spark_session.createDataFrame([
            Row(attributes=Row(data=Row(some_attribute=1608840455)))
        ])
        mapping = [(
            "mapped_name",
            "attributes.data.some_attribute",
            spq.str_to_timestamp(max_timestamp_sec=max_valid_timestamp, output_type=T.StringType())
        )]
        output_df = Mapper(mapping).transform(input_df)
        expected_df = spark_session.createDataFrame([Row(mapped_name=expected_timestamp)])
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.StringType()))
        assert_df_equality(expected_df_, output_df)


class TestCustomTimeFormatToTimestamp:
    @pytest.mark.parametrize(
        argnames="input_df, input_format, expected_df",
        argvalues=fixtures_for_custom_time_format_to_timestamp,
        indirect=["input_df", "expected_df"],
        ids=get_ids_for_fixture(fixtures_for_custom_time_format_to_timestamp),
    )
    def test_custom_time_format_to_timestamp(self, input_df, input_format, expected_df):
        mapping = [(
            "mapped_name",
            "attributes.data.some_attribute",
            spq.custom_time_format_to_timestamp(input_format=input_format, output_type=T.StringType())
        )]
        output_df = Mapper(mapping).transform(input_df)
        expected_df_ = expected_df.select(F.col("mapped_name").cast(T.StringType()))
        assert_df_equality(expected_df_, output_df)


class TestStringToArray:
    pass


class TestApplyFunction:
    pass


class TestMapValues:
    pass

