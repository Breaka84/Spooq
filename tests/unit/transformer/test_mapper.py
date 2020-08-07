from builtins import str
from builtins import object
import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from spooq2.transformer import Mapper


@pytest.fixture(scope="module")
def transformer(mapping):
    return Mapper(mapping=mapping)


@pytest.fixture(scope="module")
def input_df(spark_session):
    return spark_session.read.parquet("data/schema_v1/parquetFiles")


@pytest.fixture(scope="module")
def mapped_df(input_df, transformer):
    return transformer.transform(input_df)


@pytest.fixture(scope="module")
def mapping():
    """
    root
    |-- _corrupt_record: string (nullable = true)
    |-- attributes: struct (nullable = true)
    |    |-- birthday: string (nullable = true)
    |    |-- email: string (nullable = true)
    |    |-- first_name: string (nullable = true)
    |    |-- friends: array (nullable = true)
    |    |    |-- element: struct (containsNull = true)
    |    |    |    |-- first_name: string (nullable = true)
    |    |    |    |-- id: long (nullable = true)
    |    |    |    |-- last_name: string (nullable = true)
    |    |-- gender: string (nullable = true)
    |    |-- ip_address: string (nullable = true)
    |    |-- last_name: string (nullable = true)
    |    |-- university: string (nullable = true)
    |-- guid: string (nullable = true)
    |-- id: long (nullable = true)
    |-- location: struct (nullable = true)
    |    |-- latitude: string (nullable = true)
    |    |-- longitude: string (nullable = true)
    |-- meta: struct (nullable = true)
    |    |-- created_at_ms: long (nullable = true)
    |    |-- created_at_sec: long (nullable = true)
    |    |-- version: long (nullable = true)
    |-- birthday: timestamp (nullable = true)
    """
    return [
        ("id",                 "id",                       "IntegerType"),
        ("guid",               "guid",                     "StringType()"),
        ("created_at",         "meta.created_at_sec",      "timestamp_s_to_s"),
        ("created_at_ms",      "meta.created_at_ms",       "timestamp_ms_to_ms"),
        ("version",            "meta.version",             "IntegerType()"),
        ("birthday",           "birthday",                 "TimestampType"),
        ("location_struct",    "location",                 "as_is"),
        ("latitude",           "location.latitude",        "DoubleType"),
        ("longitude",          "location.longitude",       "DoubleType"),
        ("birthday_str",       "attributes.birthday",      "StringType"),
        ("email",              "attributes.email",         "StringType"),
        ("myspace",            "attributes.myspace",       "StringType"),
        ("first_name",         "attributes.first_name",    "StringBoolean"),
        ("last_name",          "attributes.last_name",     "StringBoolean"),
        ("gender",             "attributes.gender",        "StringType"),
        ("ip_address",         "attributes.ip_address",    "StringType"),
        ("university",         "attributes.university",    "StringType"),
        ("friends",            "attributes.friends",       "no_change"),
        ("friends_json",       "attributes.friends",       "json_string"),
    ]


class TestBasicAttributes(object):
    """Basic attributes and parameters"""
    def test_logger(self, transformer):
        assert hasattr(transformer, 'logger')

    def test_name(self, transformer):
        assert transformer.name == 'Mapper'

    def test_str_representation(self, transformer):
        assert str(transformer) == 'Transformer Object of Class Mapper'


class TestShapeOfMappedDataFrame(object):
    def test_same_amount_of_records(self, input_df, mapped_df):
        """Amount of Rows is the same after the transformation"""
        assert mapped_df.count() == input_df.count()

    def test_same_amount_of_columns(self, mapping, mapped_df):
        """Amount of Columns of the mapped DF is according to the Mapping"""
        assert len(mapped_df.columns) == len(mapping)

    def test_columns_are_renamed(self, input_df, mapped_df, mapping):
        """Mapped DF has renamed the Columns according to the Mapping"""
        assert mapped_df.columns == [x[0] for x in mapping]

    def test_base_column_is_missing_in_input(self, input_df, transformer, mapping):
        input_df = input_df.drop("attributes")
        mapped_df = transformer.transform(input_df)
        assert mapped_df.columns == [x[0] for x in mapping]

    def test_struct_column_is_empty_in_input(self, input_df, transformer, mapping):
        input_df = input_df.withColumn("attributes", F.lit(None))
        mapped_df = transformer.transform(input_df)
        assert mapped_df.columns == [x[0] for x in mapping]


class TestDataTypesOfMappedDataFrame(object):
    @pytest.mark.parametrize(("column", "expected_data_type"), [
        ("id",                 "integer"),
        ("guid",               "string"),
        ("created_at",         "long"),
        ("created_at_ms",      "long"),
        ("birthday",           "timestamp"),
        ("location_struct",    "struct"),
        ("latitude",           "double"),
        ("longitude",          "double"),
        ("birthday_str",       "string"),
        ("email",              "string"),
        ("myspace",            "string"),
        ("first_name",         "string"),
        ("last_name",          "string"),
        ("gender",             "string"),
        ("ip_address",         "string"),
        ("university",         "string"),
        ("friends",            "array"),
        ("friends_json",       "string"),
    ])
    def test_data_type_of_mapped_column(self, column, expected_data_type,
                                        mapped_df):
        assert mapped_df.schema[column].dataType.typeName(
        ) == expected_data_type


class TestImplicitSparkDataTypeConversionsFromString(object):

    # fmt:off
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=[
            (   "123456",   123456),
            (  "-123456",  -123456),
            (       "-1",       -1),
            (        "0",        0),
            (     "NULL",     None),
            (     "null",     None),
            (     "None",     None),
            (       None,     None),
            (  "1234.56",     1234),
            ( "-1234.56",    -1234),
            (  "123,456",   123456),
            ( "-123,456",  -123456),
            (  "123_456",   123456),
            ( "-123_456",  -123456),
            ("   123456",   123456),
            ("123456   ",   123456),
            (" 123456  ",   123456),
        ])
    # fmt:on
    def test_string_to_int(self, spark_session, input_value, expected_value):
        input_df = spark_session.createDataFrame(
            [Row(input_key=input_value)],
            schema=T.StructType([T.StructField("input_key", T.StringType(), True)]))
        output_df = Mapper(mapping=[("output_key", "input_key", "IntegerType")]).transform(input_df)
        assert output_df.first().output_key == expected_value

    # fmt:off
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=[
            (     "21474836470",   21474836470),
            (    "-21474836470",  -21474836470),
            (              "-1",            -1),
            (               "0",             0),
            (            "NULL",          None),
            (            "null",          None),
            (            "None",          None),
            (              None,          None),
            (    "214748364.70",     214748364),
            (   "-214748364.70",    -214748364),
            (  "21,474,836,470",   21474836470),
            ( "-21,474,836,470",  -21474836470),
            (  "21_474_836_470",   21474836470),
            ( "-21_474_836_470",  -21474836470),
            (  "   21474836470",   21474836470),
            (  "21474836470   ",   21474836470),
            (  " 21474836470  ",   21474836470),
        ])
    # fmt:on
    def test_string_to_long(self, spark_session, input_value, expected_value):
        input_df = spark_session.createDataFrame(
            [Row(input_key=input_value)],
            schema=T.StructType([T.StructField("input_key", T.StringType(), True)]))
        output_df = Mapper(mapping=[("output_key", "input_key", "LongType")]).transform(input_df)
        assert output_df.first().output_key == expected_value

    # fmt:off
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=[
            (   "123456.0",   123456.0),
            (  "-123456.0",  -123456.0),
            (       "-1.0",       -1.0),
            (        "0.0",        0.0),
            (       "NULL",       None),
            (       "null",       None),
            (       "None",       None),
            (         None,       None),
            (    "1234.56",    1234.56),
            (   "-1234.56",   -1234.56),
            (  "123,456.7",   123456.7),
            ( "-123,456.7",  -123456.7),
            (  "123_456.7",   123456.7),
            ( "-123_456.7",  -123456.7),
            ("   123456.7",   123456.7),
            ("123456.7   ",   123456.7),
            (" 123456.7  ",   123456.7),
        ])
    # fmt:on
    def test_string_to_float(self, spark_session, input_value, expected_value):
        input_df = spark_session.createDataFrame(
            [Row(input_key=input_value)],
            schema=T.StructType([T.StructField("input_key", T.StringType(), True)]))
        output_df = Mapper(mapping=[("output_key", "input_key", "FloatType")]).transform(input_df)
        assert output_df.first().output_key == expected_value

    # fmt:off
    @pytest.mark.parametrize(
        argnames=("input_value", "expected_value"),
        argvalues=[
            (      "214748364.70",    214748364.70),
            (     "-214748364.70",   -214748364.70),
            (              "-1.0",            -1.0),
            (               "0.0",             0.0),
            (              "NULL",            None),
            (              "null",            None),
            (              "None",            None),
            (                None,            None),
            (       "21474836470",   21474836470.0),
            (      "-21474836470",  -21474836470.0),
            (  "21,474,836,470.7",   21474836470.7),
            ( "-21,474,836,470.7",  -21474836470.7),
            (  "21_474_836_470.7",   21474836470.7),
            ( "-21_474_836_470.7",  -21474836470.7),
            (  "   21474836470.7",   21474836470.7),
            (  "21474836470.7   ",   21474836470.7),
            (  " 21474836470.7  ",   21474836470.7),
        ])
    # fmt:on
    def test_string_to_double(self, spark_session, input_value, expected_value):
        input_df = spark_session.createDataFrame(
            [Row(input_key=input_value)],
            schema=T.StructType([T.StructField("input_key", T.StringType(), True)]))
        output_df = Mapper(mapping=[("output_key", "input_key", "DoubleType")]).transform(input_df)
        assert output_df.first().output_key == expected_value
