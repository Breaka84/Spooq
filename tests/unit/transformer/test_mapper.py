from builtins import str
from builtins import object
import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException
from chispa.dataframe_comparer import assert_df_equality

from tests import DATA_FOLDER
from spooq.transformer import Mapper


@pytest.fixture(scope="module")
def transformer(mapping):
    return Mapper(mapping=mapping, missing_column_handling="nullify")


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
        ("id", "id", "IntegerType"),
        ("guid", "guid", "StringType()"),
        ("created_at", "meta.created_at_sec", "timestamp_s_to_s"),
        ("created_at_ms", "meta.created_at_ms", "timestamp_ms_to_ms"),
        ("version", "meta.version", "IntegerType()"),
        ("birthday", "birthday", "TimestampType"),
        ("location_struct", "location", "as_is"),
        ("latitude", "location.latitude", "DoubleType"),
        ("longitude", "location.longitude", "DoubleType"),
        ("birthday_str", "attributes.birthday", "StringType"),
        ("email", "attributes.email", "StringType"),
        ("myspace", "attributes.myspace", "StringType"),
        ("first_name", "attributes.first_name", "StringBoolean"),
        ("last_name", "attributes.last_name", "StringBoolean"),
        ("gender", "attributes.gender", "StringType"),
        ("ip_address", "attributes.ip_address", "StringType"),
        ("university", "attributes.university", "StringType"),
        ("friends", "attributes.friends", "no_change"),
        ("friends_json", "attributes.friends", "json_string"),
    ]


class TestBasicAttributes(object):
    """Basic attributes and parameters"""

    def test_logger(self, transformer):
        assert hasattr(transformer, "logger")

    def test_name(self, transformer):
        assert transformer.name == "Mapper"

    def test_str_representation(self, transformer):
        assert str(transformer) == "Transformer Object of Class Mapper"


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
        new_mapped_df = Mapper(mapping=new_mapping, mode="append", missing_column_handling="nullify").transform(mapped_df)
        assert input_columns + new_columns == new_mapped_df.columns

    def test_prepending_a_mapping(self, mapped_df, new_mapping, input_columns, new_columns):
        """Output schema is correct for added mapping at the beginning of the input schema"""
        new_mapped_df = Mapper(mapping=new_mapping, mode="prepend", missing_column_handling="nullify").transform(mapped_df)
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
        new_mapped_df = Mapper(mapping=new_mapping, mode="append", missing_column_handling="nullify").transform(mapped_df)
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
        new_mapped_df = Mapper(mapping=new_mapping, mode="prepend", missing_column_handling="nullify").transform(mapped_df)
        assert new_columns_deduplicated + input_columns == new_mapped_df.columns
        assert mapped_df.schema["birthday"].dataType == T.TimestampType()
        assert new_mapped_df.schema["birthday"].dataType == T.DateType()


class TestExceptionForMissingInputColumns(object):
    """
    Raise a ValueError if a referenced input column is missing
    """

    @pytest.fixture(scope="class")
    def transformer(self, mapping):
        return Mapper(mapping=mapping, missing_column_handling="raise_error")

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
        return Mapper(mapping=mapping, missing_column_handling="nullify")

    @pytest.fixture(scope="class")
    def mapped_df(self, input_df, transformer):
        input_df = input_df.drop("attributes")
        return transformer.transform(input_df)

    def test_missing_columns_are_not_skipped(self, mapped_df, mapping):
        assert len(mapping) == len(mapped_df.columns)

    def test_missing_columns_are_nullified(self, mapped_df, mapping):
        attribute_columns = [name for name, source, _ in mapping if source.startswith("attributes.")]
        filter = " AND ".join([f"{column} is NULL" for column in attribute_columns])
        assert mapped_df.filter(filter).count() == mapped_df.count()


class TestSkipMissingColumns(object):
    """
    Skip mapping transformation in case the input column does not exist
    """

    @pytest.fixture(scope="class")
    def transformer(self, mapping):
        return Mapper(mapping=mapping, missing_column_handling="skip")

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
            ("first_name", "string"),
            ("last_name", "string"),
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
