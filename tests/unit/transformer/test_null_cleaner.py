import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F, types as T
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StringType, StructType, StructField
from spooq.transformer.null_cleaner import NullCleaner


@pytest.fixture(scope="class")
def input_df(spark_session):
    return spark_session.createDataFrame([Row(b="positive"), Row(b=None), Row(b="positive")])


@pytest.fixture(scope="class")
def input_df_for_tests_with_multiple_cleansing_rules(spark_session):
    return spark_session.createDataFrame(
        [
            Row(a="stay", b="positive", c="or", d="healthy"),
            Row(a="stay", b=None, c="and", d="healthy"),
            Row(a="stay", b="positive", c=None, d="healthy"),
        ]
    )


@pytest.fixture(scope="class")
def cleansing_definitions_for_tests_with_multiple_cleansing_rules():
    return dict(
        b=dict(default="negative"),
        c=dict(default="and"),
    )


class TestBasicAttributes(object):
    """Basic attributes of the transformer instance"""

    def test_has_logger(self):
        assert hasattr(NullCleaner(), "logger")

    def test_has_name(self):
        assert NullCleaner().name == "NullCleaner"

    def test_has_str_representation(self):
        assert str(NullCleaner()) == "Transformer Object of Class NullCleaner"


class TestExceptionsRaisedAndDefaultParametersApplied:
    @pytest.fixture(scope="class")
    def expected_output_df(self, spark_session):
        return spark_session.createDataFrame([Row(b="positive"), Row(b=None), Row(b="positive")])

    def test_missing_default_parameter(self, input_df):
        """Missing default parameter"""
        cleaning_definition = dict(b=dict())
        with pytest.raises(ValueError) as excinfo:
            NullCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert "Null-based cleaning requires the field default." in str(
            excinfo.value
        )
        assert "Default parameter is not specified for column with name: b" in str(excinfo.value)


class TestCleaningOfNulls:

    def tests_replacement_of_null_fields(self, spark_session):
        """NullCleansed applies to values that are syntactically null (None) but not semantically null (eg. "")"""
        input_df = spark_session.createDataFrame(
            [
                Row(id=1, status="active"),
                Row(id=2, status=""),
                Row(id=3, status="None"),
                Row(id=4, status="inactive"),
                Row(id=5, status=None),
                Row(id=6, status="NULL"),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=1, status="active"),
                Row(id=2, status=""),
                Row(id=3, status="None"),
                Row(id=4, status="inactive"),
                Row(id=5, status="REPLACEMENT_VALUE"),
                Row(id=6, status="NULL"),
            ]
        )
        cleaning_definition = dict(status=dict(default="REPLACEMENT_VALUE"))
        output_df = NullCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert_df_equality(expected_output_df, output_df)


class TestCleansedValuesAreLoggedAsStruct:

    def test_single_cleansing_rule_log_as_struct(self, input_df, spark_session):
        schema = (
            StructType([
                StructField("b", StringType(), True),
                StructField("cleansed_values_null", StructType([StructField("b", StringType(), True)]), False),
            ])
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(b="positive", cleansed_values_null=Row(b=None)),
                Row(b="DEFAULT", cleansed_values_null=Row(b="null")),
                Row(b="positive", cleansed_values_null=Row(b=None)),
            ], schema=schema
        )
        cleansing_definitions = {"b": {"default": "DEFAULT"}}
        output_df = NullCleaner(cleansing_definitions, column_to_log_cleansed_values="cleansed_values_null").transform(
            input_df
        )
        assert_df_equality(expected_output_df, output_df)


class TestCleansedValuesAreLoggedAsMap:

    def test_single_cleansing_rule_log_as_map(self, input_df, spark_session):
        expected_output_schema = T.StructType(
            [
                T.StructField("b", T.StringType(), True),
                T.StructField("cleansed_values_null", T.MapType(T.StringType(), T.StringType(), True), True),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                ("positive", None),
                ("DEFAULT", {"b": "null"}),
                ("positive", None),
            ],
            schema=expected_output_schema,
        )
        cleansing_definitions = {"b": {"default": "DEFAULT"}}
        output_df = NullCleaner(
            cleansing_definitions, column_to_log_cleansed_values="cleansed_values_null", store_as_map=True
        ).transform(input_df)
        assert_df_equality(expected_output_df, output_df)

    def test_single_cleansing_boolean_log_as_map(self, input_df, spark_session):
        input_boolean_df = spark_session.createDataFrame([Row(b=True), Row(b=None), Row(b=False)])
        expected_output_schema = T.StructType(
            [
                T.StructField("b", T.BooleanType(), True),
                T.StructField("cleansed_values_null", T.MapType(T.StringType(), T.StringType(), True), True),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                (True, None),
                (False, {"b": "null"}),
                (False, None),
            ],
            schema=expected_output_schema,
        )
        cleansing_definitions = {"b": {"default": False}}
        output_df = NullCleaner(
            cleansing_definitions, column_to_log_cleansed_values="cleansed_values_null", store_as_map=True
        ).transform(input_boolean_df)
        assert_df_equality(expected_output_df, output_df)