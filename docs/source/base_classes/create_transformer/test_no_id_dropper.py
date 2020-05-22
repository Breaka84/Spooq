import pytest
from pyspark.sql.dataframe import DataFrame

from spooq2.transformer import NoIdDropper


@pytest.fixture()
def default_transformer():
    return NoIdDropper(id_columns=["first_name", "last_name"])


@pytest.fixture()
def input_df(spark_session):
    return spark_session.read.parquet("../data/schema_v1/parquetFiles")


@pytest.fixture()
def transformed_df(default_transformer, input_df):
    return default_transformer.transform(input_df)


class TestBasicAttributes(object):

    def test_logger_should_be_accessible(self, default_transformer):
        assert hasattr(default_transformer, "logger")

    def test_name_is_set(self, default_transformer):
        assert default_transformer.name == "NoIdDropper"

    def test_str_representation_is_correct(self, default_transformer):
        assert unicode(default_transformer) == "Transformer Object of Class NoIdDropper"

class TestNoIdDropper(object):

    def test_records_are_dropped(transformed_df, input_df):
        """Transformed DataFrame has no records with missing first_name and last_name"""
        assert input_df.where("first_name is null or last_name is null").count() > 0
        assert transformed_df.where("first_name is null or last_name is null").count() == 0

    def test_schema_is_unchanged(transformed_df, input_df):
        """Converted DataFrame has the expected schema"""
        assert transformed_df.schema == input_df.schema
