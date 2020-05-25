from builtins import str
from builtins import object
import pytest
from pyspark.sql import types as sql_types

from spooq2.transformer import ThresholdCleaner


class TestBasicAttributes(object):
    """Cleaner based on ranges for numerical data"""

    def test_logger(self):
        assert hasattr(ThresholdCleaner(), "logger")

    def test_name(self):
        assert ThresholdCleaner().name == "ThresholdCleaner"

    def test_str_representation(self):
        assert str(ThresholdCleaner()) == "Transformer Object of Class ThresholdCleaner"


class TestCleaning(object):

    # fmt: off
    @pytest.fixture(scope="module")
    def input_df(self, spark_session):
        input_data = [
            #ids  #floats   #integers  #strings
            [0,       12.0,      12,      "12"],
            [1,       65.7,      65,      "65"],
            [2,      300.0,     300,     "300"],
            [4,     5000.0,    5000,    "5000"],
            [5,      -75.0,     -75,     "-75"],
        ]
        schema = sql_types.StructType(
            [
                sql_types.StructField("id",       sql_types.IntegerType(), True),
                sql_types.StructField("floats",   sql_types.DoubleType(),  False),
                sql_types.StructField("integers", sql_types.LongType(),    False),
                sql_types.StructField("strings",  sql_types.StringType(),  False),
            ]
        )
        return spark_session.createDataFrame(input_data, schema=schema)

    @pytest.fixture(scope="module")
    def thresholds(self):
        return {
            "integers": {"min":  1,   "max":  300},
            "floats":   {"min":  1.0, "max":  300.0},
            "strings":  {"min": "1",  "max": "300"},
        }

    @pytest.fixture(scope="module")
    def expected_result(self):
        return {
            "integers": [ 12,    65,    300,   None, None],
            "floats":   [ 12.0,  65.7,  300.0, None, None],
            "strings":  ["12",  "65",  "300",  None, None],
        }
    # fmt: on

    @pytest.mark.parametrize("column_name", ["integers", "floats"])
    def test_numbers(self, column_name, input_df, thresholds, expected_result):

        thresholds_to_test = dict([k_v for k_v in list(thresholds.items()) if k_v[0] == column_name])
        transformer = ThresholdCleaner(thresholds_to_test)
        df_cleaned = transformer.transform(input_df)
        result = [x[column_name] for x in df_cleaned.collect()]
        expected = expected_result[column_name]

        assert result == expected
        assert input_df.columns == df_cleaned.columns

    def test_non_numbers(self, input_df, thresholds):

        column_name = "strings"
        thresholds_to_test = dict([k_v1 for k_v1 in list(thresholds.items()) if k_v1[0] == column_name])
        transformer = ThresholdCleaner(thresholds_to_test)
        
        with pytest.raises(ValueError):
            transformer.transform(input_df).count()

    # Todo: 
    # @pytest.mark.integration
    # def test_transform(self, input_df, transformer, exploded_and_filtered_schema):
    #     expected_output = exploded_and_filtered_schema
    #     actual_output   = u''.join(unicode(transformer.transform(input_df).schema).split())
    #     assert expected_output == actual_output
    #
    # @pytest.mark.slow
    # @pytest.mark.integration
    # def test_array_converted_to_struct(self, input_df, transformer):
    #     expected_output = ['data', 'jsonapi', 'elem']
    #     actual_output   = transformer.transform(input_df).first().asDict().keys()
    #     assert expected_output == actual_output
    #
