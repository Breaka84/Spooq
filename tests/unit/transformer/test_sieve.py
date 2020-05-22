import pytest
from pyspark.sql import functions as F

from spooq2.transformer import Sieve


class TestBasicAttributes(object):
    """Mapper for filtering desired Elements"""
    
    @pytest.fixture(scope="class")
    def transformer(self):
        return Sieve(filter_expression="a == b")

    def test_logger_should_be_accessible(self, transformer):
        assert hasattr(transformer, 'logger')

    def test_name_is_set(self, transformer):
        assert transformer.name == 'Sieve'

    def test_str_representation_is_correct(self, transformer):
        assert unicode(transformer) == 'Transformer Object of Class Sieve'


class TestFiltering(object):

    @pytest.fixture(scope="class")
    def input_df(self, spark_session):
        return spark_session.read.parquet("data/schema_v1/parquetFiles")

    def test_comparison(self, input_df):
        filter_expression = """attributes.gender = "F" """
        transformer = Sieve(filter_expression=filter_expression)
        transformed_df = transformer.transform(input_df)
        assert transformed_df.count() < input_df.count()
        assert transformed_df.count() == input_df.where(input_df.attributes.gender == "F").count()

    def test_regex(self, input_df):
        filter_expression = """attributes.last_name rlike "^.{7}$" """
        transformer = Sieve(filter_expression=filter_expression)
        transformed_df = transformer.transform(input_df)
        assert transformed_df.count() < input_df.count()
        assert transformed_df.count() == input_df.where(F.length(input_df.attributes.last_name) == 7).count()
