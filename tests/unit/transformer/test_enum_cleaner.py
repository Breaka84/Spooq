import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F, types as T
from chispa.dataframe_comparer import assert_df_equality
import datetime as dt
import os

from spooq.transformer import EnumCleaner


@pytest.fixture(scope="class")
def input_df(spark_session):
    return spark_session.createDataFrame([Row(b="positive"), Row(b="negative"), Row(b="positive")])


@pytest.fixture(scope="class")
def input_df_for_tests_with_multiple_cleansing_rules(spark_session):
    return spark_session.createDataFrame(
        [
            Row(a="stay", b="positive", c="or", d="healthy"),
            Row(a="stay", b="negative", c="and", d="healthy"),
            Row(a="stay", b="positive", c="xor", d="healthy"),
        ]
    )


@pytest.fixture(scope="class")
def cleansing_definitions_for_tests_with_multiple_cleansing_rules():
    return dict(
        b=dict(elements=["positive"], mode="allow"),
        c=dict(elements=["xor"], mode="disallow", default="or"),
    )


class TestBasicAttributes(object):
    """Basic attributes of the transformer instance"""

    def test_has_logger(self):
        assert hasattr(EnumCleaner(), "logger")

    def test_has_name(self):
        assert EnumCleaner().name == "EnumCleaner"

    def test_has_str_representation(self):
        assert str(EnumCleaner()) == "Transformer Object of Class EnumCleaner"


class TestExceptionsRaisedAndDefaultParametersApplied:
    @pytest.fixture(scope="class")
    def expected_output_df(self, spark_session):
        return spark_session.createDataFrame([Row(b="positive"), Row(b=None), Row(b="positive")])

    def test_missing_elements_list(self, input_df):
        """Missing elements attribute in the cleaning definition dict raises an exception"""
        cleaning_definition = dict(b=dict(mode="allow", default="cleansed!"))
        with pytest.raises(ValueError) as excinfo:
            EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert "Enumeration-based cleaning requires a non-empty list of elements per cleaning rule!" in str(
            excinfo.value
        )
        assert "Spooq did not find such a list for column: b" in str(excinfo.value)

    def test_empty_elements_list(self, input_df):
        """An empty elements attribute in the cleaning definition dict raises an exception"""
        cleaning_definition = dict(b=dict(elements=[], mode="allow", default="cleansed!"))
        with pytest.raises(ValueError) as excinfo:
            EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert "Enumeration-based cleaning requires a non-empty list of elements per cleaning rule!" in str(
            excinfo.value
        )
        assert "Spooq did not find such a list for column: b" in str(excinfo.value)

    def test_missing_mode_raises_exception(self, input_df, expected_output_df):
        """Missing 'mode' attribute is set to the default: 'allow'"""
        cleaning_definition = dict(b=dict(elements=["positive"], default=None))
        with pytest.raises(RuntimeError) as excinfo:
            EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert "Please provide a `mode` attribute!" in str(excinfo.value)
        assert "column_name: b and cleaning_definition: {'elements': ['positive'], 'default': None}" in str(
            excinfo.value
        )

    def test_default_value_defaults_to_none(self, input_df, expected_output_df):
        """Missing 'default' attribute is set to the default: None"""
        cleaning_definition = dict(b=dict(elements=["positive"], mode="allow"))
        output_df = EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert_df_equality(expected_output_df, output_df)


class TestCleaningOfStrings:
    def test_active_inactive_status_allowed(self, spark_session):
        """Only 'active' and 'inactive' allowed, other values are set to 'cleansed!' (except None)"""
        input_df = spark_session.createDataFrame(
            [
                Row(id=1, status="active"),
                Row(id=2, status=""),
                Row(id=3, status="off"),
                Row(id=4, status="inactive"),
                Row(id=5, status=None),
                Row(id=6, status="aktiv"),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=1, status="active"),
                Row(id=2, status="cleansed!"),
                Row(id=3, status="cleansed!"),
                Row(id=4, status="inactive"),
                Row(id=5, status=None),
                Row(id=6, status="cleansed!"),
            ]
        )
        cleaning_definition = dict(status=dict(elements=["active", "inactive"], mode="allow", default="cleansed!"))
        output_df = EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert_df_equality(expected_output_df, output_df)

    def test_active_inactive_status_disallowed(self, spark_session):
        """'off', '' and None values are not allowed and set to 'inactive' (except for None -> works as expected)"""
        input_df = spark_session.createDataFrame(
            [
                Row(id=1, status="active"),
                Row(id=2, status=""),
                Row(id=3, status="off"),
                Row(id=4, status="inactive"),
                Row(id=5, status=None),
                Row(id=6, status="aktiv"),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=1, status="active"),
                Row(id=2, status="inactive"),
                Row(id=3, status="inactive"),
                Row(id=4, status="inactive"),
                Row(id=5, status=None),
                Row(id=6, status="aktiv"),
            ]
        )
        cleaning_definition = dict(status=dict(elements=["off", "", None], mode="disallow", default="inactive"))
        output_df = EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert_df_equality(expected_output_df, output_df)

    def test_nullify_almost_null_fields(self, spark_session):
        """Sets values to None which are semantically but not syntactically NULL"""
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
                Row(id=2, status=None),
                Row(id=3, status=None),
                Row(id=4, status="inactive"),
                Row(id=5, status=None),
                Row(id=6, status=None),
            ]
        )
        cleaning_definition = dict(status=dict(elements=["", "None", "NULL"], mode="disallow"))
        output_df = EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert_df_equality(expected_output_df, output_df)

    def test_keep_nulls(self, spark_session):
        """Allow only some elements and Null input values are ignored (works as expected)"""
        input_df = spark_session.createDataFrame(
            [
                Row(id=1, sex="f"),
                Row(id=2, sex=""),
                Row(id=3, sex="m"),
                Row(id=4, sex="x"),
                Row(id=5, sex=None),
                Row(id=6, sex="Don't want to tell"),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=1, sex="f"),
                Row(id=2, sex="cleansed!"),
                Row(id=3, sex="m"),
                Row(id=4, sex="x"),
                Row(id=5, sex=None),
                Row(id=6, sex="cleansed!"),
            ]
        )
        cleaning_definition = dict(sex=dict(elements=["f", "m", "x"], mode="allow", default="cleansed!"))
        output_df = EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert_df_equality(expected_output_df, output_df)


class TestCleaningOfIntegers:
    def test_version_numbers_allowed(self, spark_session):
        """Only the numbers 112 and 212 are allowed, other values are set to -1"""
        input_df = spark_session.createDataFrame(
            [
                Row(id=1, version=112),
                Row(id=2, version=None),
                Row(id=3, version=212),
                Row(id=4, version=220),
                Row(id=5, version=-112),
                Row(id=6, version=0),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=1, version=112),
                Row(id=2, version=None),
                Row(id=3, version=212),
                Row(id=4, version=-1),
                Row(id=5, version=-1),
                Row(id=6, version=-1),
            ]
        )
        cleaning_definition = dict(version=dict(elements=[112, 212], mode="allow", default=-1))
        output_df = EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert_df_equality(expected_output_df, output_df)

    def test_version_numbers_disallowed(self, spark_session):
        """The numbers -112 and 0 are not allowed and set to -1"""
        input_df = spark_session.createDataFrame(
            [
                Row(id=1, version=112),
                Row(id=2, version=None),
                Row(id=3, version=212),
                Row(id=4, version=220),
                Row(id=5, version=-112),
                Row(id=6, version=0),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=1, version=112),
                Row(id=2, version=None),
                Row(id=3, version=212),
                Row(id=4, version=220),
                Row(id=5, version=-1),
                Row(id=6, version=-1),
            ]
        )
        cleaning_definition = dict(version=dict(elements=[-112, 0], mode="disallow", default=-1))
        output_df = EnumCleaner(cleaning_definitions=cleaning_definition).transform(input_df)
        assert_df_equality(expected_output_df, output_df)


class TestDynamicDefaultValues:
    @pytest.fixture(scope="class")
    def input_df(self, spark_session):
        return spark_session.createDataFrame(
            [
                Row(id=1, status="active"),
                Row(id=2, status=""),
                Row(id=3, status="off"),
                Row(id=4, status="inactive"),
                Row(id=5, status=None),
                Row(id=6, status="aktiv"),
            ]
        )

    def test_current_date(self, input_df, spark_session):
        """Substitute the cleansed values with the current date"""
        cleaning_definitions = dict(
            status=dict(elements=["active", "inactive"], mode="allow", default=F.current_date())
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=1, status="active"),
                Row(id=2, status=str(dt.date.today())),
                Row(id=3, status=str(dt.date.today())),
                Row(id=4, status="inactive"),
                Row(id=5, status=None),
                Row(id=6, status=str(dt.date.today())),
            ]
        )
        output_df = EnumCleaner(cleaning_definitions=cleaning_definitions).transform(input_df)
        assert_df_equality(expected_output_df, output_df)

    def test_column_reference(self, input_df, spark_session):
        """Substitute the cleansed values with the calculated string based on another column"""
        default_value_func = (F.col("id") * 10).cast(T.StringType())
        cleaning_definitions = dict(
            status=dict(elements=["active", "inactive"], mode="allow", default=default_value_func)
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(id=1, status="active"),
                Row(id=2, status="20"),
                Row(id=3, status="30"),
                Row(id=4, status="inactive"),
                Row(id=5, status=None),
                Row(id=6, status="60"),
            ]
        )
        output_df = EnumCleaner(cleaning_definitions=cleaning_definitions).transform(input_df)
        assert_df_equality(expected_output_df, output_df)


class TestCleansedValuesAreLoggedAsStruct:

    @pytest.fixture(scope="class")
    def expected_output_df_for_tests_with_multiple_cleansing_rules(self, spark_session):
        return spark_session.createDataFrame(
            [
                Row(a="stay", b="positive", c="or", d="healthy", cleansed_values_enum=Row(b=None, c=None)),
                Row(a="stay", b=None, c="and", d="healthy", cleansed_values_enum=Row(b="negative", c=None)),
                Row(a="stay", b="positive", c="or", d="healthy", cleansed_values_enum=Row(b=None, c="xor")),
            ]
        )

    def test_single_cleansed_value_is_stored_in_separate_column(self, input_df, spark_session):
        expected_output_df = spark_session.createDataFrame(
            [
                Row(b="positive", cleansed_values_enum=Row(b=None)),
                Row(b=None, cleansed_values_enum=Row(b="negative")),
                Row(b="positive", cleansed_values_enum=Row(b=None)),
            ]
        )
        cleansing_definitions = {"b": {"elements": ["positive"], "mode": "allow", "default": None}}
        output_df = EnumCleaner(cleansing_definitions, column_to_log_cleansed_values="cleansed_values_enum").transform(
            input_df
        )
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_single_cleansed_value_is_stored_in_separate_column_with_default_substitute(self, input_df, spark_session):
        expected_output_df = spark_session.createDataFrame(
            [
                Row(b="positive", cleansed_values_enum=Row(b=None)),
                Row(b="cleansed_value", cleansed_values_enum=Row(b="negative")),
                Row(b="positive", cleansed_values_enum=Row(b=None)),
            ]
        )
        cleansing_definitions = {"b": {"elements": ["positive"], "mode": "allow", "default": "cleansed_value"}}
        transformer = EnumCleaner(cleansing_definitions, column_to_log_cleansed_values="cleansed_values_enum")
        output_df = transformer.transform(input_df)
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_only_cleansed_values_are_stored_in_separate_column(self, spark_session):
        input_df = spark_session.createDataFrame(
            [Row(a="stay", b="positive"), Row(a="stay", b="negative"), Row(a="stay", b="positive")]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                Row(a="stay", b="positive", cleansed_values_enum=Row(b=None)),
                Row(a="stay", b=None, cleansed_values_enum=Row(b="negative")),
                Row(a="stay", b="positive", cleansed_values_enum=Row(b=None)),
            ]
        )
        cleansing_definitions = {"b": {"elements": ["positive"], "mode": "allow", "default": None}}
        output_df = EnumCleaner(cleansing_definitions, column_to_log_cleansed_values="cleansed_values_enum").transform(
            input_df
        )
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_multiple_cleansing_rules(self,
                                      input_df_for_tests_with_multiple_cleansing_rules,
                                      cleansing_definitions_for_tests_with_multiple_cleansing_rules,
                                      expected_output_df_for_tests_with_multiple_cleansing_rules):

        output_df = EnumCleaner(cleansing_definitions_for_tests_with_multiple_cleansing_rules, column_to_log_cleansed_values="cleansed_values_enum").transform(
            input_df_for_tests_with_multiple_cleansing_rules
        )
        assert_df_equality(expected_output_df_for_tests_with_multiple_cleansing_rules, output_df, ignore_nullable=True)

    def test_multiple_cleansing_rules_with_parquet_streaming(self, spark_session, tmpdir,
                                                     input_df_for_tests_with_multiple_cleansing_rules,
                                                     cleansing_definitions_for_tests_with_multiple_cleansing_rules,
                                                     expected_output_df_for_tests_with_multiple_cleansing_rules):

        input_table_location = os.path.join(tmpdir, "input_table.parquet")
        output_table_location = os.path.join(tmpdir, "output_table.parquet")
        output_checkpoint_location = os.path.join(tmpdir, "output_table.checkpoint")

        input_df_for_tests_with_multiple_cleansing_rules.write.parquet(input_table_location)

        input_stream = (spark_session.readStream
                        .format("parquet")
                        .option("maxBytesPerTrigger", str(1024 * 1024))
                        .schema(input_df_for_tests_with_multiple_cleansing_rules.schema)
                        .load(input_table_location)
        )

        cleaned_stream = EnumCleaner(cleansing_definitions_for_tests_with_multiple_cleansing_rules, column_to_log_cleansed_values="cleansed_values_enum").transform(
            input_stream
        )

        (cleaned_stream.writeStream
            .queryName("enum_cleaner_stream")
            .format("parquet")
            .outputMode("append")
            .option("checkpointLocation", output_checkpoint_location)
            .trigger(once=True)
            .start(output_table_location)
        ).awaitTermination()

        output_df = spark_session.read.parquet(output_table_location)

        assert_df_equality(expected_output_df_for_tests_with_multiple_cleansing_rules, output_df, ignore_nullable=True, ignore_row_order=True)

    def test_multiple_cleansing_rules_with_delta_streaming(self, spark_session, tmpdir,
                                                           input_df_for_tests_with_multiple_cleansing_rules,
                                                           cleansing_definitions_for_tests_with_multiple_cleansing_rules,
                                                           expected_output_df_for_tests_with_multiple_cleansing_rules):
        input_table_location = os.path.join(tmpdir, "input_table.delta")
        output_table_location = os.path.join(tmpdir, "output_table.delta")
        output_checkpoint_location = os.path.join(tmpdir, "output_table.checkpoint")

        input_df_for_tests_with_multiple_cleansing_rules.write.format("delta").save(input_table_location)

        input_stream = (spark_session.readStream
                        .format("delta")
                        .option("maxBytesPerTrigger", str(1024 * 1024))
                        .load(input_table_location)
                        )

        cleaned_stream = EnumCleaner(cleansing_definitions_for_tests_with_multiple_cleansing_rules, column_to_log_cleansed_values="cleansed_values_enum").transform(
            input_stream
        )

        (cleaned_stream.writeStream
         .queryName("enum_cleaner_stream")
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", output_checkpoint_location)
         .trigger(once=True)
         .start(output_table_location)
         ).awaitTermination()

        output_df = spark_session.read.format("delta").load(output_table_location)
        output_df_sorted = output_df.sort(["a", "b", "c", "d", F.col("cleansed_values_enum").cast(T.StringType())])
        expected_output_df_sorted = expected_output_df_for_tests_with_multiple_cleansing_rules.sort(["a", "b", "c", "d", F.col("cleansed_values_enum").cast(T.StringType())])

        assert_df_equality(expected_output_df_sorted, output_df_sorted, ignore_nullable=True)


class TestCleansedValuesAreLoggedAsMap:

    @pytest.fixture(scope="class")
    def expected_output_schema_for_tests_with_multiple_cleansing_rules(self):
        return T.StructType(
            [
                T.StructField("a", T.StringType(), True),
                T.StructField("b", T.StringType(), True),
                T.StructField("c", T.StringType(), True),
                T.StructField("d", T.StringType(), True),
                T.StructField("cleansed_values_enum", T.MapType(T.StringType(), T.StringType(), True), False),
            ]
        )

    @pytest.fixture(scope="class")
    def expected_output_df_for_tests_with_multiple_cleansing_rules(self, spark_session, expected_output_schema_for_tests_with_multiple_cleansing_rules):
        return spark_session.createDataFrame(
            [
                ("stay", "positive", "or", "healthy", {}),
                ("stay", None, "and", "healthy", {"b": "negative"}),
                ("stay", "positive", "or", "healthy", {"c": "xor"}),
            ],
            schema=expected_output_schema_for_tests_with_multiple_cleansing_rules,
        )

    def test_single_cleansed_value_is_stored_in_separate_column(self, input_df, spark_session):
        expected_output_schema = T.StructType(
            [
                T.StructField("b", T.StringType(), True),
                T.StructField("cleansed_values_enum", T.MapType(T.StringType(), T.StringType(), True), False),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                ("positive", {}),
                (None, {"b": "negative"}),
                ("positive", {}),
            ],
            schema=expected_output_schema,
        )
        cleansing_definitions = {"b": {"elements": ["positive"], "mode": "allow", "default": None}}
        output_df = EnumCleaner(
            cleansing_definitions, column_to_log_cleansed_values="cleansed_values_enum", store_as_map=True
        ).transform(input_df)
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_single_cleansed_value_is_stored_in_separate_column_with_default_substitute(self, input_df, spark_session):
        expected_output_schema = T.StructType(
            [
                T.StructField("b", T.StringType(), True),
                T.StructField("cleansed_values_enum", T.MapType(T.StringType(), T.StringType(), True), False),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                ("positive", {}),
                ("cleansed_value", {"b": "negative"}),
                ("positive", {}),
            ],
            schema=expected_output_schema,
        )
        cleansing_definitions = {"b": {"elements": ["positive"], "mode": "allow", "default": "cleansed_value"}}
        transformer = EnumCleaner(
            cleansing_definitions, column_to_log_cleansed_values="cleansed_values_enum", store_as_map=True
        )
        output_df = transformer.transform(input_df)
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_only_cleansed_values_are_stored_in_separate_column(self, spark_session):
        input_df = spark_session.createDataFrame(
            [Row(a="stay", b="positive"), Row(a="stay", b="negative"), Row(a="stay", b="positive")]
        )

        expected_output_schema = T.StructType(
            [
                T.StructField("a", T.StringType(), True),
                T.StructField("b", T.StringType(), True),
                T.StructField("cleansed_values_enum", T.MapType(T.StringType(), T.StringType(), True), False),
            ]
        )
        expected_output_df = spark_session.createDataFrame(
            [
                ("stay", "positive", {}),
                ("stay", None, {"b": "negative"}),
                ("stay", "positive", {}),
            ],
            schema=expected_output_schema,
        )
        cleansing_definitions = {"b": {"elements": ["positive"], "mode": "allow", "default": None}}
        output_df = EnumCleaner(
            cleansing_definitions, column_to_log_cleansed_values="cleansed_values_enum", store_as_map=True
        ).transform(input_df)
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_multiple_cleansing_rules(self,
                                      input_df_for_tests_with_multiple_cleansing_rules,
                                      cleansing_definitions_for_tests_with_multiple_cleansing_rules,
                                      expected_output_df_for_tests_with_multiple_cleansing_rules):

        output_df = EnumCleaner(
            cleansing_definitions_for_tests_with_multiple_cleansing_rules, column_to_log_cleansed_values="cleansed_values_enum", store_as_map=True
        ).transform(input_df_for_tests_with_multiple_cleansing_rules)
        assert_df_equality(expected_output_df_for_tests_with_multiple_cleansing_rules, output_df, ignore_nullable=True)

    def test_multiple_cleansing_rules_without_any_cleansing(self,
                                                            spark_session,
                                                            expected_output_schema_for_tests_with_multiple_cleansing_rules,
                                                            cleansing_definitions_for_tests_with_multiple_cleansing_rules,
                                                            expected_output_df_for_tests_with_multiple_cleansing_rules):

        input_df = spark_session.createDataFrame(
            [
                Row(a="stay", b="positive", c="or", d="healthy"),
                Row(a="stay", b="positive", c="and", d="healthy"),
                Row(a="stay", b="positive", c="or", d="healthy"),
            ]
        )

        expected_output_df = spark_session.createDataFrame(
            [
                ("stay", "positive", "or", "healthy", {}),
                ("stay", "positive", "and", "healthy", {}),
                ("stay", "positive", "or", "healthy", {}),
            ],
            schema=expected_output_schema_for_tests_with_multiple_cleansing_rules,
        )

        output_df = EnumCleaner(
            cleansing_definitions_for_tests_with_multiple_cleansing_rules, column_to_log_cleansed_values="cleansed_values_enum", store_as_map=True
        ).transform(input_df)
        assert_df_equality(expected_output_df, output_df, ignore_nullable=True)

    def test_multiple_cleansing_rules_with_parquet_streaming(self, spark_session, tmpdir,
                                                             input_df_for_tests_with_multiple_cleansing_rules,
                                                             cleansing_definitions_for_tests_with_multiple_cleansing_rules,
                                                             expected_output_df_for_tests_with_multiple_cleansing_rules):
        input_table_location = os.path.join(tmpdir, "input_table.parquet")
        output_table_location = os.path.join(tmpdir, "output_table.parquet")
        output_checkpoint_location = os.path.join(tmpdir, "output_table.checkpoint")

        input_df_for_tests_with_multiple_cleansing_rules.write.parquet(input_table_location)


        input_stream = (spark_session.readStream
                        .format("parquet")
                        .option("maxBytesPerTrigger", str(1024 * 1024))
                        .schema(input_df_for_tests_with_multiple_cleansing_rules.schema)
                        .load(input_table_location)
                        )

        cleaned_stream = EnumCleaner(cleansing_definitions_for_tests_with_multiple_cleansing_rules, column_to_log_cleansed_values="cleansed_values_enum", store_as_map=True).transform(
            input_stream
        )

        (cleaned_stream.writeStream
         .queryName("enum_cleaner_stream")
         .format("parquet")
         .outputMode("append")
         .option("checkpointLocation", output_checkpoint_location)
         .trigger(once=True)
         .start(output_table_location)
         ).awaitTermination()

        output_df = spark_session.read.parquet(output_table_location)
        output_df_sorted = output_df.sort(["a", "b", "c", "d", F.col("cleansed_values_enum").cast(T.StringType())])
        expected_output_df_sorted = expected_output_df_for_tests_with_multiple_cleansing_rules.sort(["a", "b", "c", "d", F.col("cleansed_values_enum").cast(T.StringType())])

        assert_df_equality(expected_output_df_sorted, output_df_sorted, ignore_nullable=True)

    def test_multiple_cleansing_rules_with_delta_streaming(self, spark_session, tmpdir,
                                                           input_df_for_tests_with_multiple_cleansing_rules,
                                                           cleansing_definitions_for_tests_with_multiple_cleansing_rules,
                                                           expected_output_df_for_tests_with_multiple_cleansing_rules):
        input_table_location = os.path.join(tmpdir, "input_table.delta")
        output_table_location = os.path.join(tmpdir, "output_table.delta")
        output_checkpoint_location = os.path.join(tmpdir, "output_table.checkpoint")

        input_df_for_tests_with_multiple_cleansing_rules.write.format("delta").save(input_table_location)

        input_stream = (spark_session.readStream
                        .format("delta")
                        .option("maxBytesPerTrigger", str(1024 * 1024))
                        .load(input_table_location)
                        )

        cleaned_stream = EnumCleaner(cleansing_definitions_for_tests_with_multiple_cleansing_rules, column_to_log_cleansed_values="cleansed_values_enum", store_as_map=True).transform(
            input_stream
        )

        (cleaned_stream.writeStream
         .queryName("enum_cleaner_stream")
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", output_checkpoint_location)
         .trigger(once=True)
         .start(output_table_location)
         ).awaitTermination()

        output_df = spark_session.read.format("delta").load(output_table_location)
        output_df_sorted = output_df.sort(["a", "b", "c", "d", F.col("cleansed_values_enum").cast(T.StringType())])
        expected_output_df_sorted = expected_output_df_for_tests_with_multiple_cleansing_rules.sort(["a", "b", "c", "d", F.col("cleansed_values_enum").cast(T.StringType())])

        assert_df_equality(expected_output_df_sorted, output_df_sorted, ignore_nullable=True)
