from builtins import str
from builtins import object
from typing import List
from copy import deepcopy
from uuid import uuid4

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from chispa.dataframe_comparer import assert_df_equality

from spooq.transformer.annotator import Annotator, AnnotatorMode
from spooq.transformer.mapper import ColumnMappingNotSupported, DataTypeValidationFailed, MapperMode
from tests import DATA_FOLDER
from spooq.transformer import Mapper
from spooq.transformer.mapper import MapperMode, MissingColumnHandling
from spooq.transformer import mapper_transformations as spq


@pytest.fixture(scope="module")
def transformer(mapping):
    return Mapper(mapping=mapping, missing_column_handling=MissingColumnHandling.nullify)


@pytest.fixture(scope="module")
def input_df(spark_session):
    return spark_session.read.parquet(f"{DATA_FOLDER}/schema_v1/parquetFiles")


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
        # fmt: off
        ("id",              "id",                    spq.to_int),
        ("guid",            "guid",                  spq.to_str),
        ("created_at",      "meta.created_at_sec",   T.LongType()),
        ("created_at_ms",   "meta.created_at_ms",    T.LongType()),
        ("version",         "meta.version",          spq.to_int),
        ("birthday",        "birthday",              spq.to_timestamp),
        ("location_struct", "location",              spq.as_is),
        ("latitude",        "location.latitude",     spq.to_double),
        ("longitude",       "location.longitude",    spq.to_double),
        ("birthday_str",    "attributes.birthday",   spq.to_str),
        ("email",           "attributes.email",      spq.to_str),
        ("myspace",         "attributes.myspace",    spq.to_str),
        ("has_first_name",  "attributes.first_name", spq.has_value),
        ("has_last_name",   "attributes.last_name",  spq.has_value),
        ("gender",          "attributes.gender",     spq.to_str),
        ("ip_address",      "attributes.ip_address", spq.to_str),
        ("university",      "attributes.university", spq.to_str),
        ("friends",         "attributes.friends",    spq.as_is),
        ("friends_json",    "attributes.friends",    spq.to_json_string),
        # fmt: on
    ]


class TestBasicAttributes(object):
    """Basic attributes and parameters"""

    def test_logger(self, transformer):
        assert hasattr(transformer, "logger")

    def test_name(self, transformer):
        assert transformer.name == "Mapper"

    def test_str_representation(self, transformer):
        assert str(transformer) == "Transformer Object of Class Mapper"


class TestLenghtOfMappingTuple:
    @pytest.fixture(scope="class")
    def input_df(self, spark_session: SparkSession):
        return spark_session.range(0)

    def test_column_mapping_without_comments(self, input_df: DataFrame):
        mapping = [("id", "id", T.LongType())]
        output_df = Mapper(mapping).transform(input_df)
        assert_df_equality(input_df, output_df)

    def test_column_mapping_with_comments(self, input_df: DataFrame):
        mapping = [("id", "id", T.LongType(), "This column contains the ID")]
        output_df = Mapper(mapping).transform(input_df)
        assert_df_equality(input_df, output_df, ignore_metadata=True)

    @pytest.mark.parametrize("num_of_elements", (0, 1, 2, 5, 6))
    def test_unsupported_number_of_elements_for_column_mapping(self, input_df: DataFrame, num_of_elements: int):
        mapping = ["string" for x in range(0, num_of_elements)]
        with pytest.raises(ColumnMappingNotSupported):
            Mapper(mapping).transform(input_df)


class TestShapeOfMappedDataFrame(object):
    def test_same_amount_of_records(self, input_df, mapped_df):
        """Amount of Rows is the same after the transformation"""
        assert mapped_df.count() == input_df.count()

    def test_same_amount_of_columns(self, mapping, mapped_df):
        """Amount of Columns of the mapped DF is according to the Mapping"""
        assert len(mapped_df.columns) == len(mapping)

    def test_columns_are_renamed(self, mapped_df, mapping):
        """Mapped DF has renamed the Columns according to the Mapping"""
        assert mapped_df.columns == [name for (name, path, data_type) in mapping]

    def test_base_column_is_missing_in_input(self, input_df, transformer, mapping):
        input_df = input_df.drop("attributes")
        mapped_df = transformer.transform(input_df)
        assert mapped_df.columns == [name for (name, path, data_type) in mapping]

    def test_struct_column_is_empty_in_input(self, input_df, transformer, mapping):
        input_df = input_df.withColumn("attributes", F.lit(None))
        mapped_df = transformer.transform(input_df)
        assert mapped_df.columns == [name for (name, path, data_type) in mapping]

    def test_input_dataframe_is_empty(self, spark_session, transformer, mapping):
        input_df = spark_session.createDataFrame([], schema=T.StructType())
        mapped_df = transformer.transform(input_df)
        assert mapped_df.columns == [name for (name, path, data_type) in mapping]


class TestMultipleMappings(object):
    @pytest.fixture(scope="module")
    def input_columns(self, mapped_df):
        return mapped_df.columns

    @pytest.fixture(scope="module")
    def new_mapping(self):
        return [("created_date", "meta.created_at_sec", "DateType")]

    @pytest.fixture(scope="module")
    def new_columns(self, new_mapping):
        return [name for (name, path, data_type) in new_mapping]

    def test_appending_a_mapping(self, mapped_df, new_mapping, input_columns, new_columns):
        """Output schema is correct for added mapping at the end of the input schema"""
        new_mapped_df = Mapper(
            mapping=new_mapping, mode=MapperMode.append, missing_column_handling=MissingColumnHandling.nullify
        ).transform(mapped_df)
        assert input_columns + new_columns == new_mapped_df.columns

    def test_prepending_a_mapping(self, mapped_df, new_mapping, input_columns, new_columns):
        """Output schema is correct for added mapping at the beginning of the input schema"""
        new_mapped_df = Mapper(
            mapping=new_mapping, mode=MapperMode.prepend, missing_column_handling=MissingColumnHandling.nullify
        ).transform(mapped_df)
        assert new_columns + input_columns == new_mapped_df.columns

    def test_appending_a_mapping_with_duplicated_columns(self, input_columns, mapped_df):
        """Output schema is correct for newly appended mapping with columns
        that are also included in the input schema"""
        new_mapping = [
            ("created_date", "meta.created_at_sec", "DateType"),
            ("birthday", "birthday", "DateType"),
        ]
        new_columns = [name for (name, path, data_type) in new_mapping]
        new_columns_deduplicated = [x for x in new_columns if x not in input_columns]
        new_mapped_df = Mapper(
            mapping=new_mapping, mode=MapperMode.append, missing_column_handling=MissingColumnHandling.nullify
        ).transform(mapped_df)
        assert input_columns + new_columns_deduplicated == new_mapped_df.columns
        assert mapped_df.schema["birthday"].dataType == T.TimestampType()
        assert new_mapped_df.schema["birthday"].dataType == T.DateType()

    def test_prepending_a_mapping_with_duplicated_columns(self, input_columns, mapped_df):
        """Output schema is correct for newly prepended mapping with columns
        that are also included in the input schema"""
        new_mapping = [
            ("created_date", "meta.created_at_sec", "DateType"),
            ("birthday", "birthday", "DateType"),
        ]
        new_columns = [name for (name, path, data_type) in new_mapping]
        new_columns_deduplicated = [x for x in new_columns if x not in input_columns]
        new_mapped_df = Mapper(
            mapping=new_mapping, mode=MapperMode.prepend, missing_column_handling=MissingColumnHandling.nullify
        ).transform(mapped_df)
        assert new_columns_deduplicated + input_columns == new_mapped_df.columns
        assert mapped_df.schema["birthday"].dataType == T.TimestampType()
        assert new_mapped_df.schema["birthday"].dataType == T.DateType()


class TestExceptionForMissingInputColumns(object):
    """
    Raise a ValueError if a referenced input column is missing
    """

    @pytest.fixture(scope="class")
    def transformer(self, mapping):
        return Mapper(mapping=mapping, missing_column_handling=MissingColumnHandling.raise_error)

    def test_missing_column_raises_exception(self, input_df, transformer):
        input_df = input_df.drop("attributes")
        with pytest.raises(AnalysisException):
            transformer.transform(input_df)

    def test_empty_input_dataframe_raises_exception(self, spark_session, transformer):
        input_df = spark_session.createDataFrame([], schema=T.StructType())
        with pytest.raises(AnalysisException):
            transformer.transform(input_df)


class TestNullifyMissingColumns(object):
    """
    Nullify input columns in case it does not exist
    """

    @pytest.fixture(scope="class")
    def transformer(self, mapping):
        return Mapper(mapping=mapping, missing_column_handling=MissingColumnHandling.nullify)

    @pytest.fixture(scope="class")
    def mapped_df(self, input_df, transformer):
        input_df = input_df.drop("attributes")
        return transformer.transform(input_df)

    def test_missing_columns_are_not_skipped(self, mapped_df, mapping):
        assert len(mapping) == len(mapped_df.columns)

    def test_missing_columns_are_nullified(self, mapped_df, mapping):
        attribute_columns = [
            name
            for name, source, transformation in mapping
            if source.startswith("attributes.") and not transformation == spq.has_value
        ]
        filter = " AND ".join([f"{column} is NULL" for column in attribute_columns])
        assert mapped_df.filter(filter).count() == mapped_df.count()


class TestSkipMissingColumns(object):
    """
    Skip mapping transformation in case the input column does not exist
    """

    @pytest.fixture(scope="class")
    def transformer(self, mapping):
        return Mapper(mapping=mapping, missing_column_handling=MissingColumnHandling.skip)

    @pytest.fixture(scope="class")
    def mapped_df(self, input_df, transformer):
        input_df = input_df.drop("attributes")
        return transformer.transform(input_df)

    def test_missing_columns_are_skipped(self, mapped_df, mapping):
        attribute_columns = [name for name, source, _ in mapping if source.startswith("attributes.")]
        assert not any([column in mapped_df.columns for column in attribute_columns])


class TestExceptionWhenInvalidHandling(object):
    """
    Raise an exception in case both parameters skip_missing_columns and nullify_missing_columns are True
    """

    def test_invalid_parameter_setting_raises_exception(self, input_df, transformer):
        with pytest.raises(ValueError):
            Mapper(mapping=mapping, missing_column_handling="invalid")


class TestDataTypesOfMappedDataFrame(object):
    @pytest.mark.parametrize(
        ("column", "expected_data_type"),
        [
            ("id", "integer"),
            ("guid", "string"),
            ("created_at", "long"),
            ("created_at_ms", "long"),
            ("birthday", "timestamp"),
            ("location_struct", "struct"),
            ("latitude", "double"),
            ("longitude", "double"),
            ("birthday_str", "string"),
            ("email", "string"),
            ("myspace", "string"),
            ("has_first_name", "boolean"),
            ("has_last_name", "boolean"),
            ("gender", "string"),
            ("ip_address", "string"),
            ("university", "string"),
            ("friends", "array"),
            ("friends_json", "string"),
        ],
    )
    def test_data_type_of_mapped_column(self, column, expected_data_type, mapped_df):
        assert mapped_df.schema[column].dataType.typeName() == expected_data_type


class TestAmbiguousColumnNames:
    @pytest.fixture(scope="class")
    def input_df(self, spark_session):
        return spark_session.createDataFrame(
            [Row(int_val=123, Key="Hello", key="World"), Row(int_val=124, Key="Nice to", key="meet you")]
        )

    @pytest.fixture(scope="class")
    def expected_output_df(self, spark_session):
        return spark_session.createDataFrame([Row(int_val=123), Row(int_val=124)])

    @pytest.fixture(scope="class")
    def mapping(self):
        return [
            ("int_val", "int_val", "LongType"),
            ("key", "Key", "StringType"),
        ]

    def test_ambiguous_column_names_raise_exception(self, input_df, mapping):
        transformer = Mapper(mapping)
        with pytest.raises(AnalysisException):
            transformer.transform(input_df)

    def test_ambiguous_column_names_exception_is_ignored(self, input_df, mapping, expected_output_df):
        transformer = Mapper(mapping, ignore_ambiguous_columns=True)
        output_df = transformer.transform(input_df)
        assert_df_equality(expected_output_df, output_df)


class TestValidateDataTypes:
    @pytest.fixture(scope="class")
    def input_df(self, spark_session):
        return spark_session.createDataFrame(
            [Row(col_a=123, col_b="Hello", col_c=123456789)], schema="col_a int, col_b string, col_c long"
        )

    @pytest.fixture(scope="class")
    def matching_mapping(self) -> List[tuple]:
        # fmt: off
        return [
            ("col_a", "col_a", T.IntegerType()),
            ("col_b", "col_b", T.StringType()),
            ("col_c", "col_c", T.LongType()),
        ]
        # fmt: on

    def test_matching_mapping_with_casting(self, input_df, matching_mapping):
        transformer = Mapper(matching_mapping)
        assert_df_equality(input_df, transformer.transform(input_df))

    def test_matching_mapping_with_validation(self, input_df, matching_mapping):
        transformer = Mapper(matching_mapping, mode=MapperMode.rename_and_validate)
        assert_df_equality(input_df, transformer.transform(input_df))

    def test_mismatching_mapping_with_casting(self, input_df, matching_mapping):
        mapping_ = deepcopy(matching_mapping)
        mapping_[0] = ("col_a", "col_a", T.StringType())
        transformer = Mapper(mapping_)
        mapped_df = transformer.transform(input_df)
        assert input_df.columns == mapped_df.columns
        assert mapped_df.schema["col_a"].jsonValue()["type"] == "string"

    def test_mismatching_mapping_with_validation(self, input_df, matching_mapping):
        mapping_ = deepcopy(matching_mapping)
        mapping_[0] = ("col_a", "col_a", T.StringType())
        transformer = Mapper(mapping_, mode=MapperMode.rename_and_validate)
        with pytest.raises(DataTypeValidationFailed, match="col_a"):
            transformer.transform(input_df)

    def test_spooq_transformation_raises_exception(self, input_df, matching_mapping):
        mapping_ = deepcopy(matching_mapping)
        mapping_[0] = ("col_a", "col_a", spq.to_int)
        transformer = Mapper(mapping_, mode=MapperMode.rename_and_validate)
        with pytest.raises(
            DataTypeValidationFailed, match="Spooq transformations are not allowed in 'rename_and_validate' mode"
        ):
            transformer.transform(input_df)

    def test_mapper_raises_exception_for_missing_column(self, input_df, matching_mapping):
        missing_column = "col_d"
        mapping_ = deepcopy(matching_mapping)
        mapping_.append((missing_column, missing_column, T.StringType()))
        transformer = Mapper(
            mapping_, mode=MapperMode.rename_and_validate, missing_column_handling=MissingColumnHandling.raise_error
        )
        with pytest.raises(
            AnalysisException,
            match=f"A column or function parameter with name `{missing_column}` cannot be resolved.|"
            f"cannot resolve '{missing_column}' given input columns|"
            f"cannot resolve '`{missing_column}`' given input columns|"
            f"A column, variable, or function parameter with name `{missing_column}` cannot be resolved|"
            f"Column '{missing_column}' does not exist. Did you mean one of the following?",
        ):
            transformer.transform(input_df)

    def test_validation_raises_exception_for_skipped_column(self, input_df, matching_mapping):
        skipped_column = "col_d"
        mapping_ = deepcopy(matching_mapping)
        mapping_.append((skipped_column, skipped_column, T.StringType()))
        transformer = Mapper(
            mapping_, mode=MapperMode.rename_and_validate, missing_column_handling=MissingColumnHandling.skip
        )
        with pytest.raises(DataTypeValidationFailed, match=f"Input column: {skipped_column} not found!"):
            transformer.transform(input_df)

    def test_validation_succeeds_for_nullified_column(self, input_df, matching_mapping):
        column_to_nullify = "col_d"
        mapping_ = deepcopy(matching_mapping)
        mapping_.append((column_to_nullify, column_to_nullify, T.StringType()))
        mapped_df = Mapper(
            mapping_, mode=MapperMode.rename_and_validate, missing_column_handling=MissingColumnHandling.nullify
        ).transform(input_df)
        assert mapped_df.schema[column_to_nullify].jsonValue()["type"] == "string"

    def test_spooq_transformation_as_is(self, input_df, matching_mapping):
        mapping_ = deepcopy(matching_mapping)
        mapping_[0] = ("col_a", "col_a", spq.as_is)
        transformer = Mapper(mapping_, mode=MapperMode.rename_and_validate)
        assert_df_equality(input_df, transformer.transform(input_df))

    def test_spooq_transformation_as_is_string(self, input_df, matching_mapping):
        mapping_ = deepcopy(matching_mapping)
        mapping_[0] = ("col_a", "col_a", "as_is")
        transformer = Mapper(mapping_, mode=MapperMode.rename_and_validate)
        assert_df_equality(input_df, transformer.transform(input_df))


class TestColumnComments:


    @pytest.fixture(scope="class")
    def setup_database(self, spark_session: SparkSession):
        spark_session.sql("CREATE DATABASE IF NOT EXISTS db")
        yield
        spark_session.sql("DROP DATABASE IF EXISTS db CASCADE")

    @pytest.fixture(scope="module")
    def input_schema(self) -> str:
        return """
            col_a int COMMENT 'initial',
            col_b int,
            col_c int
        """

    @pytest.fixture(scope="class")
    def input_df(self, spark_session: SparkSession, input_schema: str) -> DataFrame:
        return spark_session.createDataFrame([Row(col_a=1, col_b=2, col_c=3)], input_schema)

    @pytest.fixture()
    def input_table(self, spark_session: SparkSession, input_df: DataFrame, setup_database) -> str:
        input_df.write.saveAsTable(name="db.table_with_partial_comments", format="delta", mode="overwrite")
        yield "db.table_with_partial_comments"
        spark_session.sql("DROP TABLE db.table_with_partial_comments")

    @pytest.fixture(scope="class")
    def mapping(self) -> List[tuple]:
        # fmt: off
        return [
            ("col_a", "col_a", spq.as_is, "updated"),
            ("col_b", "col_b", spq.as_is, "updated"),
            ("col_c", "col_c", spq.as_is),
        ]
        # fmt: on

    @pytest.fixture()
    def random_string(self) -> str:
        return str(uuid4())[:8]

    @pytest.mark.parametrize(
        argnames=["column_name", "expected_comment"],
        argvalues=[
            ("col_a", "initial"),  # taken from referenced sql_source_table
            ("col_b", None),
            ("col_c", None),
        ],
    )
    def test_fetch_comments_from_input_table(
        self,
        spark_session: SparkSession,
        column_name: str,
        expected_comment: str,
        mapping: List,
        input_table: str,
        random_string: str,
    ):
        mapping = [
            ("col_a", "col_a", spq.as_is),
            ("col_b", "col_b", spq.as_is),
            ("col_c", "col_c", spq.as_is),
        ]
        source_df = spark_session.table(input_table)
        output_df = Mapper(mapping=mapping, annotator_options={"sql_source_table_identifier": input_table}).transform(
            source_df
        )
        output_df.write.saveAsTable(f"db.output_table_{random_string}")
        table_description_df = spark_session.sql(f"DESCRIBE db.output_table_{random_string}")
        assert (
            table_description_df.where(F.col("col_name") == column_name).rdd.map(lambda row: row.comment).collect()[0]
            == expected_comment
        )

    @pytest.mark.parametrize(
        argnames=["column_name", "expected_comment"],
        argvalues=[
            ("col_a", "initial"),  # taken from referenced sql_source_table
            ("col_x", None),
            ("col_y", "updated"),  # taken from mapping
        ],
    )
    def test_fetch_comments_from_related_table(
        self, spark_session: SparkSession, column_name: str, expected_comment: str, input_table: str, random_string: str
    ):
        source_df = spark_session.createDataFrame([], "col_a int, col_x int, col_y int")
        mapping = [
            ("col_a", "col_a", spq.as_is),
            ("col_x", "col_x", spq.as_is),
            ("col_y", "col_y", spq.as_is, "updated"),
        ]
        output_df = Mapper(mapping, annotator_options={"sql_source_table_identifier": input_table}).transform(source_df)
        output_df.write.saveAsTable(f"db.output_table_{random_string}")
        table_description_df = spark_session.sql(f"DESCRIBE db.output_table_{random_string}")
        assert (
            table_description_df.where(F.col("col_name") == column_name).rdd.map(lambda row: row.comment).collect()[0]
            == expected_comment
        )

    @pytest.mark.parametrize(
        argnames=["column_name", "expected_comment"],
        argvalues=[
            ("col_a", "initial"),
            ("col_b", "updated"),
            ("col_c", None),
        ],
    )
    def test_insert_comments_to_input_table(
        self,
        spark_session: SparkSession,
        column_name: str,
        expected_comment: str,
        mapping: List,
        input_table: str,
        random_string: str,
    ):
        source_df = spark_session.table(input_table)
        output_df = Mapper(
            mapping, annotator_options={"mode": AnnotatorMode.insert, "sql_source_table_identifier": input_table}
        ).transform(source_df)
        output_df.write.saveAsTable(f"db.output_table_{random_string}")
        table_description_df = spark_session.sql(f"DESCRIBE db.output_table_{random_string}")
        assert (
            table_description_df.where(F.col("col_name") == column_name).rdd.map(lambda row: row.comment).collect()[0]
            == expected_comment
        )

    @pytest.mark.parametrize(
        argnames=["column_name", "expected_comment"],
        argvalues=[
            ("col_a", "updated"),
            ("col_b", "updated"),
            ("col_c", None),
        ],
    )
    def test_upsert_comments_to_input_table(
        self,
        spark_session: SparkSession,
        column_name: str,
        expected_comment: str,
        mapping: List,
        input_table: str,
        random_string: str,
    ):
        source_df = spark_session.table(input_table)
        output_df = Mapper(mapping, annotator_options={"mode": AnnotatorMode.upsert}).transform(source_df)
        output_df.write.saveAsTable(f"db.output_table_{random_string}")
        table_description_df = spark_session.sql(f"DESCRIBE db.output_table_{random_string}")
        assert (
            table_description_df.where(F.col("col_name") == column_name).rdd.map(lambda row: row.comment).collect()[0]
            == expected_comment
        )

    @pytest.mark.parametrize(
        argnames=["column_name", "expected_comment"],
        argvalues=[
            ("col_a", "updated"),
            ("col_b", "updated"),
            ("col_c", None),
        ],
    )
    def test_upsert_comments_to_input_table_in_validation_mode(
        self,
        spark_session: SparkSession,
        column_name: str,
        expected_comment: str,
        mapping: List,
        input_table: str,
        random_string: str,
    ):
        source_df = spark_session.table(input_table)
        output_df = Mapper(mapping, mode=MapperMode.rename_and_validate, annotator_options={"mode": AnnotatorMode.upsert}).transform(source_df)
        output_df.write.saveAsTable(f"db.output_table_{random_string}")
        table_description_df = spark_session.sql(f"DESCRIBE db.output_table_{random_string}")
        assert (
            table_description_df.where(F.col("col_name") == column_name).rdd.map(lambda row: row.comment).collect()[0]
            == expected_comment
        )
